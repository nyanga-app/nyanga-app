"""Site scraping, planning and the parallel download pipeline. No GUI code in here."""

from __future__ import annotations

import html
import io
import logging
import copy
import dataclasses
import os
import queue
import re
import shutil
import threading
import time
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass, field
from multiprocessing import Manager
from pathlib import Path
from urllib.parse import quote, urlencode

import requests

from builders import build_file, pages_of
from version import __version__

log = logging.getLogger(__name__)

NO_VOLUME = 0


class SiteError(RuntimeError):
    def __init__(self, message: str, status: int = 0):
        super().__init__(message)
        self.status = status  # the HTTP status that caused it, if any (404: the page is gone)


class Cancelled(Exception):
    pass


@dataclass
class Chapter:
    number: float
    volume: int  # NO_VOLUME when the site does not say
    url: str
    source: str = ""  # the site's name, when the title is on several
    alts: list[Chapter] = field(default_factory=list)  # the same chapter on other sites, tried if this one fails


@dataclass
class Group:
    """The chapters that end up in one output file."""
    label: str
    chapters: list[Chapter]
    # Chapters already in a file we made: number -> (that file, all its chapters in order), so
    # their pages come out of it instead of being downloaded again.
    reuse: dict[float, tuple[str, list[Chapter]]] = field(default_factory=dict)
    about: dict = field(default_factory=dict)  # the series' summary, author, genres, page: written into the file


@dataclass
class MangaInfo:
    title: str
    chapters: list[Chapter]
    cover: bytes | None = None
    status: str = ""
    genres: list[str] = field(default_factory=list)
    description: str = ""
    rating: float = 0.0  # out of 5; 0 when nobody has rated it yet
    author: str = ""
    aliases: list[str] = field(default_factory=list)  # other names it goes by
    sources: list[str] = field(default_factory=list)  # the sites its chapters come from


@dataclass
class SearchFilters:
    """Narrows a search down; names come from the site's `genres` and `kinds`, empty means any."""
    genres: list[str] = field(default_factory=list)
    without: list[str] = field(default_factory=list)  # genres to leave out
    kind: str = ""
    status: str = ""  # "", ONGOING or COMPLETED
    rating: int = 0  # at least this many stars, 0 for any

    def __bool__(self) -> bool:
        return bool(self.genres or self.without or self.kind or self.status or self.rating)


ONGOING, COMPLETED = "Ongoing", "Completed"


@dataclass
class SearchResult:
    """One title in a site's search results."""
    title: str
    url: str
    cover_url: str = ""
    status: str = ""
    latest: str = ""
    author: str = ""
    genres: list[str] | None = None  # None until fetched from the title's page
    description: str = ""
    rating: float = 0.0
    site: str = ""  # name of the site `url` and `cover_url` are on
    aliases: list[str] = field(default_factory=list)
    urls: list[str] = field(default_factory=list)  # its pages on every site that has it


class RateLimiter:
    """Spaces out request starts across all threads, so more workers never means more requests per second."""

    def __init__(self, delay: float):
        self.delay = delay
        self._lock = threading.Lock()
        self._next = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            start = max(now, self._next)
            self._next = start + self.delay
        time.sleep(start - now)


class SharedRateLimiter:
    """The same as RateLimiter, but shared between processes through a multiprocessing manager."""

    def __init__(self, delay: float, manager):
        self.delay = delay
        self._lock = manager.Lock()
        self._next = manager.Value("d", 0.0)

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            start = max(now, self._next.value)
            self._next.value = start + self.delay
        time.sleep(max(0.0, start - now))


# --------------------------------------------------------------------------- sites


class Site:
    """What every site shares: an HTTP session per thread, rate limiting and retries."""
    referer = ""  # sent with page requests
    image_referer = ""  # sent with image requests
    _user_agent = f"Nyanga/{__version__}"

    def __init__(self, limiter: RateLimiter):
        self.limiter = limiter
        self._local = threading.local()  # one HTTP session per thread

    def __getstate__(self) -> dict:
        state = self.__dict__.copy()
        del state["_local"]  # sessions can't be sent to another process; each one makes its own
        return state

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        self._local = threading.local()

    def with_limiter(self, limiter):
        clone = copy.copy(self)
        clone.limiter = limiter
        return clone

    def _session(self) -> requests.Session:
        s = requests.Session()
        s.headers["User-Agent"] = self._user_agent
        return s

    @property
    def _http(self) -> requests.Session:
        if not hasattr(self._local, "session"):
            self._local.session = self._session()
        return self._local.session

    def _get(self, url: str, referer: str | None = None, retries: int = 3,
             method: str = "GET") -> requests.Response:
        last, status = "unknown error", 0
        headers = {"Referer": self.referer if referer is None else referer}
        for attempt in range(retries):
            self.limiter.wait()
            if attempt:
                time.sleep(attempt * 2)  # back off a little more after each failure
            try:
                resp = self._http.request(method, url, headers=headers, timeout=30)
                log.debug("%s %s -> %s (%d bytes)", method, url, resp.status_code, len(resp.content))
                if resp.status_code == 200:
                    return resp
                last, status = f"HTTP {resp.status_code}", resp.status_code
                if resp.status_code in (403, 404):
                    break
                if resp.status_code == 429:  # too many requests: wait as long as the site asks
                    wait = resp.headers.get("Retry-After") or resp.headers.get("X-RateLimit-Retry-After", "")
                    if wait.isdigit():
                        wait = int(wait)
                        time.sleep(min(60.0, max(0.0, wait - time.time() if wait > 10**9 else wait)))
            except requests.RequestException as e:
                last, status = type(e).__name__, 0
                log.debug("%s %s failed: %r", method, url, e)
            log.info("try %d of %d for %s: %s", attempt + 1, retries, url, last)
        log.warning("giving up on %s: %s", url, last)
        raise SiteError(f"Could not fetch {url}: {last}", status)

    def cover(self, url: str) -> bytes | None:
        try:
            return self._get(url, retries=1).content
        except SiteError:
            return None

    def image_size(self, url: str) -> int | None:
        """Size of an image in bytes, asking only for its headers."""
        resp = self._get(url, referer=self.image_referer, retries=1, method="HEAD")
        length = resp.headers.get("content-length", "")
        return int(length) if length.isdigit() else None

    def image(self, url: str) -> bytes:
        resp = self._get(url, referer=self.image_referer)
        if not resp.headers.get("content-type", "").startswith("image/") and \
                not any(resp.content.startswith(magic) for magic, _ in EXT_BY_MAGIC):
            raise SiteError(f"Not an image: {url}")
        return resp.content

    def sources_for(self, chapter: Chapter) -> list[tuple[Site, Chapter]]:
        """Where a chapter can be downloaded from, best first."""
        return [(self, chapter)]

    @classmethod
    def lookup(cls, name: str, limiter: RateLimiter) -> list[SearchResult]:
        """Titles that might go by this name, for finding a title seen on another site."""
        return cls.search(name, limiter)[0]


class FanFox(Site):
    name = "fanfox.net"
    home = "https://fanfox.net/"  # linked from Settings, App tab
    example = "https://fanfox.net/manga/<title>/"
    url_re = re.compile(r"(?:fanfox\.net|mangafox\.\w+)/manga/([a-z0-9_]+)", re.I)
    _title_re = re.compile(r'class="detail-info-right-title-font">([^<]+)<')
    _status_re = re.compile(r'class="detail-info-right-title-tip">([^<]+)<')
    _genres_re = re.compile(r'<p class="detail-info-right-tag-list">(.*?)</p>', re.S)
    _desc_res = [re.compile(rf'<p [^>]*class="{c}">(.*?)</p>', re.S) for c in ("fullcontent", "detail-info-right-content")]
    _score_re = re.compile(r'<span class="item-score">\s*([\d.]+)\s*<')
    _author_re = re.compile(r'<p class="detail-info-right-say">(.*?)</p>', re.S)
    _cover_re = re.compile(r'<img class="detail-info-cover-img" src="([^"]+)"')
    _image_re = re.compile(r'data-original="([^"]+)"')
    _result_re = re.compile(r'<li>\s*<a href="/manga/([a-z0-9_]+)/"[^>]*><img class="manga-list-4-cover" '
                            r'src="([^"]*)".*?</li>', re.S | re.I)
    genres = ["Action", "Adventure", "Comedy", "Drama", "Fantasy", "Martial Arts", "Shounen", "Horror",
              "Supernatural", "Harem", "Psychological", "Romance", "School Life", "Shoujo", "Mystery", "Sci-fi",
              "Seinen", "Tragedy", "Ecchi", "Sports", "Slice of Life", "Mature", "Shoujo Ai", "Webtoons",
              "Doujinshi", "One Shot", "Smut", "Yaoi", "Josei", "Historical", "Shounen Ai", "Gender Bender",
              "Adult", "Yuri", "Mecha", "Lolicon", "Shotacon"]  # in the site's id order, starting at 1
    kinds = ["Japanese Manga", "Korean Manhwa", "Chinese Manhua", "European Manga", "American Manga",
             "HongKong Manga", "Other Manga"]
    sorts = ["Best match", "A to Z", "Newest"]  # the first is the site's own order
    _sort_ids = {"A to Z": 1, "Newest": 2}  # its 3 (rating) only goes lowest first, 4 isn't popularity
    _user_agent = ("Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/129.0 Mobile Safari/537.36")
    referer = "https://fanfox.net/"
    image_referer = "https://m.fanfox.net/"

    def __init__(self, url: str, limiter: RateLimiter):
        super().__init__(limiter)
        match = self.url_re.search(url)
        self.slug = match.group(1).lower() if match else ""  # empty for an instance that only searches
        # Only this title's own links; the page also links to chapters of other manga.
        self._chapter_re = re.compile(
            rf'href="(/manga/{re.escape(self.slug)}/(?:v([^/"]+)/)?c([\d.]+)/1\.html)"', re.I)

    def _session(self) -> requests.Session:
        s = super()._session()
        s.cookies.set("isAdult", "1", domain=".fanfox.net")
        return s

    @property
    def key(self) -> str:
        """Identifies the title across runs, e.g. for download history."""
        return f"{self.name}:{self.slug}"

    @property
    def title_url(self) -> str:
        return f"https://fanfox.net/manga/{self.slug}/"

    def info(self, with_cover: bool = True) -> MangaInfo:
        page = self._get(self.title_url).text
        title = self._title_re.search(page)
        found = {}
        for path, vol, num in self._chapter_re.findall(page):
            found[path] = Chapter(float(num), int(vol) if vol.isdigit() else NO_VOLUME, path)
        status = self._status_re.search(page)
        genres = self._genres_re.search(page)
        desc = next(filter(None, (r.search(page) for r in self._desc_res)), None)  # the full one if it's there
        score = self._score_re.search(page)
        author = self._author_re.search(page)
        info = MangaInfo(
            title=html.unescape(title.group(1)).strip() if title else self.slug.replace("_", " ").title(),
            chapters=sorted(found.values(), key=lambda c: (c.number, c.url)),
            status=status.group(1).strip() if status else "",
            genres=[html.unescape(g) for g in re.findall(r'title="([^"]+)"', genres.group(1))] if genres else [],
            description=_plain(desc.group(1)) if desc else "",
            rating=float(score.group(1)) if score else 0.0,
            author=", ".join(html.unescape(a).strip() for a in re.findall(r'title="([^"]+)"', author.group(1))
                             if a.strip() != "[Add]") if author else "",
        )
        cover = self._cover_re.search(page) if with_cover else None
        if cover:
            try:
                info.cover = self._get(html.unescape(cover.group(1)), retries=1).content
            except SiteError:
                pass  # a missing cover is not worth failing over
        return info

    @classmethod
    def search(cls, query: str, limiter: RateLimiter, page_no: int = 1,
               filters: SearchFilters | None = None, sort: str = "") -> tuple[list[SearchResult], int]:
        """One page of titles matching a name (closest matches first on page 1), and how many pages there are.
        With filters the name may be empty, to browse everything that fits them. `sort` is one of `sorts`."""
        site = cls("", limiter)
        params = {"title": query.strip(), "page": page_no}
        if sort in cls._sort_ids:
            params["sort"] = cls._sort_ids[sort]
        if filters:
            ids = lambda names: ",".join(str(cls.genres.index(g) + 1) for g in names if g in cls.genres)
            params.update(stype=1, genres=ids(filters.genres), nogenres=ids(filters.without),
                          type=cls.kinds.index(filters.kind) + 1 if filters.kind in cls.kinds else 0,
                          st={ONGOING: 1, COMPLETED: 2}.get(filters.status, 0))
            if filters.rating:
                params.update(rating_method="gt", rating=filters.rating - 1)
        page = site._get("https://fanfox.net/search?" + urlencode(params)).text
        pages = max([page_no] + [int(n) for n in re.findall(r'/search\?page=(\d+)&', page)])
        results = []
        for slug, cover, item in ((m.group(1).lower(), m.group(2), m.group(0)) for m in cls._result_re.finditer(page)):
            title = re.search(r'class="manga-list-4-item-title"><a [^>]*title="([^"]+)"', item)
            status = re.search(r'manga-list-4-show-tag-list-2">\s*<a[^>]*>([^<]+)<', item)
            author = re.search(r'Author:<a [^>]*title="([^"]+)"', item)
            latest = re.search(r'Latest Chapter:<a [^>]*>([^<]+)<', item)
            results.append(SearchResult(
                title=html.unescape(title.group(1)).strip() if title else slug.replace("_", " ").title(),
                url=f"https://fanfox.net/manga/{slug}/",
                cover_url=html.unescape(cover),
                status=status.group(1).strip() if status else "",
                latest=html.unescape(latest.group(1)).strip() if latest else "",
                author=html.unescape(author.group(1)).strip() if author else "",
                site=cls.name,
            ))
        # The site's own order puts spin-offs before the title itself; exact and prefix matches go first.
        if page_no == 1 and query.strip() and "sort" not in params:
            wanted = query.strip().lower()
            results.sort(key=lambda r: (r.title.lower() != wanted, not r.title.lower().startswith(wanted)))
        return results, pages if results else page_no

    def about(self, url: str) -> tuple[list[str], str, float]:
        """A title's genres, description and rating, for showing alongside search results."""
        info = type(self)(url, self.limiter).info(with_cover=False)
        return info.genres, info.description, info.rating

    def page_urls(self, chapter: Chapter) -> list[str]:
        # The mobile "roll" reader lists every page of a chapter in one document.
        roll = "https://m.fanfox.net" + chapter.url.replace("/manga/", "/roll_manga/", 1)
        urls = [html.unescape(u) for u in self._image_re.findall(self._get(roll).text)]
        if not urls:
            raise SiteError(f"No pages found for chapter {fmt_num(chapter.number)}")
        return ["https:" + u if u.startswith("//") else u for u in urls]


class MangaDex(Site):
    """mangadex.org through its public API: fan translations, English only for now."""
    name = "mangadex.org"
    home = "https://mangadex.org/"
    example = "https://mangadex.org/title/<id>"
    url_re = re.compile(r"mangadex\.org/title/([0-9a-f]{8}-[0-9a-f-]{27})", re.I)
    api = "https://api.mangadex.org"
    referer = image_referer = "https://mangadex.org/"  # without it, covers come back as a "read this at" picture
    language = "en"
    per_page = 20
    genres = ["Action", "Adventure", "Comedy", "Drama", "Fantasy", "Horror", "Mystery", "Psychological", "Romance",
              "Sci-Fi", "Slice of Life", "Sports", "Thriller", "Tragedy", "Historical", "Isekai", "Crime", "Mecha",
              "Medical", "Philosophical", "Superhero", "Magical Girls", "Wuxia", "Boys' Love", "Girls' Love",
              "School Life", "Martial Arts", "Supernatural", "Magic", "Reincarnation", "Time Travel", "Harem",
              "Reverse Harem", "Villainess", "Monsters", "Vampires", "Zombies", "Survival", "Post-Apocalyptic",
              "Video Games", "Military", "Office Workers", "Cooking", "Music", "Gore", "Long Strip", "Full Color",
              "Oneshot", "Web Comic", "Doujinshi"]
    kinds = ["Japanese Manga", "Korean Manhwa", "Chinese Manhua"]
    _languages = {"Japanese Manga": ["ja"], "Korean Manhwa": ["ko"], "Chinese Manhua": ["zh", "zh-hk"]}
    sorts = ["Best match", "Most followed", "Top rated", "Latest chapter", "Newest", "A to Z"]
    _orders = {"Best match": ("relevance", "desc"), "Most followed": ("followedCount", "desc"),
               "Top rated": ("rating", "desc"), "Latest chapter": ("latestUploadedChapter", "desc"),
               "Newest": ("createdAt", "desc"), "A to Z": ("title", "asc")}
    _statuses = {"ongoing": ONGOING, "completed": COMPLETED, "hiatus": "On hiatus", "cancelled": "Cancelled"}
    _ratings = ["safe", "suggestive", "erotica", "pornographic"]
    _tag_ids: dict[str, str] = {}  # tag name -> id, fetched the first time a filter needs them

    def __init__(self, url: str, limiter: RateLimiter):
        super().__init__(limiter)
        match = self.url_re.search(url)
        self.slug = match.group(1).lower() if match else ""

    @property
    def key(self) -> str:
        return f"{self.name}:{self.slug}"

    @property
    def title_url(self) -> str:
        return f"https://mangadex.org/title/{self.slug}"

    def _json(self, path: str, params: dict | list | None = None) -> dict:
        query = "?" + urlencode(params, doseq=True) if params else ""
        try:
            data = self._get(f"{self.api}{path}{query}").json()
        except ValueError:
            raise SiteError("MangaDex sent back something odd. Try again in a moment ✦")
        if data.get("result") != "ok":
            raise SiteError("MangaDex said no: " + "; ".join(e.get("detail", "") for e in data.get("errors", [])))
        return data

    def _pick(self, names: dict) -> str:
        """The English text of a {language: text} map, else the first one."""
        return (names.get(self.language) or next(iter(names.values()), "")).strip()

    def _title(self, attrs: dict) -> str:
        """The English title when there is one: the main title is often romanised Japanese."""
        if self.language in attrs["title"]:
            return attrs["title"][self.language].strip()
        alt = next((t[self.language] for t in attrs.get("altTitles", []) if self.language in t), "")
        return alt.strip() or self._pick(attrs["title"])

    def _cover_url(self, manga: dict, size: int) -> str:
        art = next((r for r in manga["relationships"] if r["type"] == "cover_art" and "attributes" in r), None)
        return f"https://uploads.mangadex.org/covers/{manga['id']}/{art['attributes']['fileName']}.{size}.jpg" \
            if art else ""

    @staticmethod
    def _authors(manga: dict) -> str:
        return ", ".join(r["attributes"]["name"] for r in manga["relationships"]
                         if r["type"] == "author" and "attributes" in r)

    @staticmethod
    def _aliases(attrs: dict) -> list[str]:
        return [t for names in [attrs["title"], *attrs.get("altTitles", [])] for t in names.values()]

    @staticmethod
    def _plain_desc(text: str) -> str:
        text = text.split("\n---")[0]  # the rest is usually links to other sites
        text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)  # markdown links
        return re.sub(r"[*_]{1,3}([^*_]+)[*_]{1,3}", r"\1", text).strip()

    def _scores(self, ids: list[str]) -> dict[str, float]:
        """Ratings out of 5, many titles in one request."""
        try:
            stats = self._json("/statistics/manga", {"manga[]": ids})["statistics"]
        except SiteError:
            return {}
        return {i: round((s.get("rating", {}).get("bayesian") or 0) / 2, 2) for i, s in stats.items()}

    def info(self, with_cover: bool = True) -> MangaInfo:
        manga = self._json(f"/manga/{self.slug}", {"includes[]": ["cover_art", "author"]})["data"]
        attrs = manga["attributes"]
        info = MangaInfo(
            title=self._title(attrs),
            chapters=self._chapters(),
            status=self._statuses.get(attrs.get("status") or "", ""),
            genres=[self._pick(t["attributes"]["name"]) for t in attrs.get("tags", [])
                    if t["attributes"]["group"] in ("genre", "theme")],
            description=self._plain_desc(self._pick(attrs.get("description") or {})),
            rating=self._scores([self.slug]).get(self.slug, 0.0),
            author=self._authors(manga),
            aliases=self._aliases(attrs),
        )
        if not info.chapters:
            raise SiteError(f"MangaDex has no English chapters of {info.title} to download: big licensed "
                            "titles often only link to the publisher's own site ✦", 404)
        if with_cover and (url := self._cover_url(manga, 512)):
            info.cover = self.cover(url)
        return info

    def _chapters(self) -> list[Chapter]:
        """One upload per chapter number; when several groups translated it, the one with the most chapters."""
        found, offset, total = [], 0, 1
        while offset < total:
            data = self._json(f"/manga/{self.slug}/feed", {
                "translatedLanguage[]": [self.language], "contentRating[]": self._ratings, "limit": 500,
                "offset": offset, "order[chapter]": "asc", "includeExternalUrl": 0, "includeEmptyPages": 0})
            found += data["data"]
            offset, total = offset + len(data["data"]), data["total"]
            if not data["data"]:
                break
        group_of = lambda c: next((r["id"] for r in c["relationships"] if r["type"] == "scanlation_group"), "")
        sizes: dict[str, int] = {}
        for c in found:
            sizes[group_of(c)] = sizes.get(group_of(c), 0) + 1
        best: dict[float, dict] = {}
        for c in found:
            attrs = c["attributes"]
            if attrs.get("externalUrl") or not attrs.get("pages"):
                continue  # only on the publisher's own site
            try:
                number = float(attrs.get("chapter") or 0)  # a oneshot has no number
            except ValueError:
                continue
            if number not in best or sizes[group_of(c)] > sizes[group_of(best[number])]:
                best[number] = c
        return [Chapter(n, int(v) if (v := c["attributes"].get("volume") or "").isdigit() else NO_VOLUME,
                        f"https://mangadex.org/chapter/{c['id']}") for n, c in sorted(best.items())]

    @classmethod
    def search(cls, query: str, limiter: RateLimiter, page_no: int = 1,
               filters: SearchFilters | None = None, sort: str = "") -> tuple[list[SearchResult], int]:
        site = cls("", limiter)
        query = query.strip()
        field, way = cls._orders.get(sort or "Best match")
        if field == "relevance" and not query:
            field, way = "followedCount", "desc"  # nothing to match against when just browsing
        params = {"title": query, "limit": cls.per_page, "offset": (page_no - 1) * cls.per_page,
                  f"order[{field}]": way, "includes[]": ["cover_art", "author"],
                  "contentRating[]": cls._ratings[:3], "availableTranslatedLanguage[]": [cls.language],
                  "hasAvailableChapters": "true"}
        if not query:
            del params["title"]
        if filters:
            if filters.genres or filters.without:
                ids = site._tags()
                params["includedTags[]"] = [ids[g] for g in filters.genres if g in ids]
                params["excludedTags[]"] = [ids[g] for g in filters.without if g in ids]
            if filters.kind in cls._languages:
                params["originalLanguage[]"] = cls._languages[filters.kind]
            if filters.status:
                params["status[]"] = [k for k, v in cls._statuses.items() if v == filters.status]
        data = site._json("/manga", params)
        pages = max(1, -(-min(data["total"], 10_000) // cls.per_page))  # the API stops at 10,000 results
        scores = site._scores([m["id"] for m in data["data"]]) if data["data"] else {}
        results = []
        for m in data["data"]:
            attrs = m["attributes"]
            last = attrs.get("lastChapter") or ""
            results.append(SearchResult(
                title=site._title(attrs),
                url=f"https://mangadex.org/title/{m['id']}",
                cover_url=site._cover_url(m, 256),
                status=cls._statuses.get(attrs.get("status") or "", ""),
                latest=f"Ch. {last}" if last else "",
                author=site._authors(m),
                genres=[site._pick(t["attributes"]["name"]) for t in attrs.get("tags", [])
                        if t["attributes"]["group"] in ("genre", "theme")],
                description=cls._plain_desc(site._pick(attrs.get("description") or {})),
                rating=scores.get(m["id"], 0.0),
                site=cls.name,
                aliases=site._aliases(attrs),
            ))
        if filters and filters.rating:  # the API can't filter by rating, so this page is just thinned out
            results = [r for r in results if r.rating >= filters.rating]
        return results, pages if data["data"] else page_no

    def _tags(self) -> dict[str, str]:
        if not MangaDex._tag_ids:
            MangaDex._tag_ids = {t["attributes"]["name"]["en"]: t["id"] for t in self._json("/manga/tag")["data"]}
        return MangaDex._tag_ids

    def page_urls(self, chapter: Chapter) -> list[str]:
        server = self._json(f"/at-home/server/{chapter.url.rsplit('/', 1)[-1]}")
        ch = server["chapter"]
        if not ch["data"]:
            raise SiteError(f"No pages found for chapter {fmt_num(chapter.number)}")
        return [f"{server['baseUrl']}/data/{ch['hash']}/{f}" for f in ch["data"]]


def _chapter_number(label: str) -> float | None:
    """The number in a chapter's name: "Chapter 12.5: Title" -> 12.5, "Smoke 67" -> 67."""
    m = re.search(r"(?:chapter|ch\.?|episode|ep\.?)\s*(\d+(?:\.\d+)?)", label, re.I) or \
        re.search(r"(\d+(?:\.\d+)?)(?!.*\d)", label)
    return float(m.group(1)) if m else None


class WeebCentral(Site):
    """weebcentral.com: plain HTML, chapters straight from the uploads, no volume numbers."""
    name = "weebcentral.com"
    home = "https://weebcentral.com/"
    example = "https://weebcentral.com/series/<id>/<title>"
    url_re = re.compile(r"weebcentral\.com/series/([0-9A-Z]{26})", re.I)
    referer = image_referer = "https://weebcentral.com/"
    _user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36"
    per_page = 32

    def __init__(self, url: str, limiter: RateLimiter):
        super().__init__(limiter)
        match = self.url_re.search(url)
        self.slug = match.group(1).upper() if match else ""

    @property
    def key(self) -> str:
        return f"{self.name}:{self.slug.lower()}"

    @property
    def title_url(self) -> str:
        return f"https://weebcentral.com/series/{self.slug}"

    def info(self, with_cover: bool = True) -> MangaInfo:
        page = self._get(self.title_url).text
        title = re.search(r'<h1[^>]*>([^<]+)</h1>', page)
        listed = self._get(f"https://weebcentral.com/series/{self.slug}/full-chapter-list").text
        found = []
        for cid, label in re.findall(r'<a href="(?:https://weebcentral\.com)?/chapters/([0-9A-Z]{26})"[^>]*>.*?'
                                     r'<span class="">([^<]+)</span>', listed, re.S):
            number = _chapter_number(html.unescape(label))
            if number is not None:
                found.append(Chapter(number, NO_VOLUME, f"https://weebcentral.com/chapters/{cid}"))
        section = lambda name: re.search(rf"<strong>{name}[^<]*</strong>(.*?)</li>", page, re.S)
        desc, authors, status = (section(n) for n in ("Description", "Author", "Status"))
        names = re.search(r"<strong>Associated Name[^<]*</strong>(.*?)</ul>", page, re.S)
        tags = section("Tag")
        info = MangaInfo(
            title=html.unescape(title.group(1)).strip() if title else "",
            chapters=sorted(found, key=lambda c: c.number),
            status=_plain(status.group(1)) if status else "",
            genres=[html.unescape(t) for t in re.findall(r'included_tag=[^"]*"[^>]*>([^<]+)<', tags.group(1))] if tags else [],
            description=_plain(desc.group(1)) if desc else "",
            author=", ".join(html.unescape(a) for a in re.findall(r'author=[^"]*"[^>]*>([^<]+)<', authors.group(1)))
            if authors else "",
            aliases=[html.unescape(n).strip() for n in re.findall(r"<li>([^<]+)</li>", names.group(1))] if names else [],
        )
        if with_cover:
            info.cover = self.cover(f"https://temp.compsci88.com/cover/fallback/{self.slug}.jpg")
        return info

    @classmethod
    def search(cls, query: str, limiter: RateLimiter, page_no: int = 1,
               filters: SearchFilters | None = None, sort: str = "") -> tuple[list[SearchResult], int]:
        site = cls("", limiter)
        params = {"text": query.strip(), "sort": "Best Match", "order": "Descending", "official": "Any",
                  "anime": "Any", "adult": "Any", "display_mode": "Full Display", "limit": cls.per_page,
                  "offset": (page_no - 1) * cls.per_page}
        page = site._get("https://weebcentral.com/search/data?" + urlencode(params)).text
        results = []
        for item in re.split(r'<article class="bg-base-300', page)[1:]:
            link = re.search(r'href="https://weebcentral\.com/series/([0-9A-Z]{26})/[^"]*"', item)
            if not link:
                continue
            title = re.search(r'class="line-clamp-1 link link-hover">([^<]+)<', item) or \
                re.search(r'alt="([^"]+) cover"', item)
            field_ = lambda name: re.search(rf"<strong>{name}:</strong>\s*<span>([^<]*)</span>", item)
            status = field_("Status")
            results.append(SearchResult(
                title=html.unescape(title.group(1)).strip() if title else "",
                url=f"https://weebcentral.com/series/{link.group(1)}",
                cover_url=f"https://temp.compsci88.com/cover/fallback/{link.group(1)}.jpg",
                status=status.group(1).strip() if status else "",
                author=", ".join(html.unescape(a) for a in re.findall(r'search\?author=[^"]*"[^>]*>([^<]+)<', item)),
                genres=[html.unescape(t).strip(" ,") for t in re.findall(r"<span>([^<]+),?</span>",
                        item.split("Tag(s):", 1)[1])] if "Tag(s):" in item else [],
                site=cls.name,
            ))
        more = "offset=" in page and results
        return results, page_no + 1 if more else page_no

    def page_urls(self, chapter: Chapter) -> list[str]:
        page = self._get(f"{chapter.url}/images?is_prev=False&current_page=1&reading_style=long_strip").text
        urls = [html.unescape(u) for u in re.findall(r'<img[^>]+src="(https?://[^"]+)"', page) if "/static/" not in u]
        if not urls:
            raise SiteError(f"No pages found for chapter {fmt_num(chapter.number)}")
        return urls


class MangaKatana(Site):
    """mangakatana.com: plain HTML; a chapter's pictures are listed in a script on its page."""
    name = "mangakatana.com"
    home = "https://mangakatana.com/"
    example = "https://mangakatana.com/manga/<title>.<id>"
    url_re = re.compile(r"mangakatana\.com/manga/([a-z0-9-]+\.\d+)", re.I)
    referer = image_referer = "https://mangakatana.com/"
    _user_agent = WeebCentral._user_agent

    def __init__(self, url: str, limiter: RateLimiter):
        super().__init__(limiter)
        match = self.url_re.search(url)
        self.slug = match.group(1).lower() if match else ""

    @property
    def key(self) -> str:
        return f"{self.name}:{self.slug}"

    @property
    def title_url(self) -> str:
        return f"https://mangakatana.com/manga/{self.slug}"

    @staticmethod
    def _jpg(url: str) -> str:
        return re.sub(r"\.(avif|webp)$", ".jpg", url)  # Qt can't always show those; the site has a JPEG too

    def _parse(self, page: str, with_cover: bool) -> MangaInfo:
        title = re.search(r'<h1 class="heading">([^<]+)</h1>', page)
        found = {}
        own = re.escape(self.slug)
        for path, label in re.findall(rf'<div class="chapter"><a href="(https://mangakatana\.com/manga/{own}/c[^"]+)">([^<]+)</a>',
                                      page):
            number = _chapter_number(html.unescape(label))
            if number is not None:
                found[path] = Chapter(number, NO_VOLUME, path)
        alt = re.search(r'<div class="alt_name">([^<]*)</div>', page)
        status = re.search(r'<div class="d-cell-small value status [^"]*">([^<]+)<', page)
        genres = re.search(r'<div class="genres">(.*?)</div>', page, re.S)
        desc = re.search(r'<div class="summary">(.*?)</div>', page, re.S)
        cover = re.search(r'<meta property="og:image" content="([^"]+)"', page)
        info = MangaInfo(
            title=html.unescape(title.group(1)).strip() if title else self.slug.rsplit(".", 1)[0].replace("-", " ").title(),
            chapters=sorted(found.values(), key=lambda c: c.number),
            status=status.group(1).strip() if status else "",
            genres=[html.unescape(g) for g in re.findall(r">([^<]+)</a>", genres.group(1))] if genres else [],
            description=_plain(re.sub(r'<div class="label">[^<]*</div>', "", desc.group(1))) if desc else "",
            author=", ".join(html.unescape(a) for a in re.findall(r'<a class="author"[^>]*>([^<]+)<', page)),
            aliases=[html.unescape(n).strip() for n in alt.group(1).split(";") if n.strip()] if alt else [],
        )
        if with_cover and cover:
            info.cover = self.cover(self._jpg(cover.group(1)))
        return info

    def info(self, with_cover: bool = True) -> MangaInfo:
        return self._parse(self._get(self.title_url).text, with_cover)

    @classmethod
    def search(cls, query: str, limiter: RateLimiter, page_no: int = 1,
               filters: SearchFilters | None = None, sort: str = "") -> tuple[list[SearchResult], int]:
        site = cls("", limiter)
        where = f"https://mangakatana.com/page/{page_no}" if page_no > 1 else "https://mangakatana.com/"
        resp = site._get(where + "?" + urlencode({"search": query.strip(), "search_by": "book_name"}))
        if cls.url_re.search(resp.url):  # a single match goes straight to its page
            one = cls(resp.url, limiter)
            info = one._parse(resp.text, with_cover=False)
            cover = re.search(r'<meta property="og:image" content="([^"]+)"', resp.text)
            last = info.chapters[-1] if info.chapters else None
            return [SearchResult(info.title, one.title_url, cls._jpg(cover.group(1)) if cover else "", info.status,
                                 f"Ch. {fmt_num(last.number)}" if last else "", info.author, info.genres,
                                 info.description, site=cls.name, aliases=info.aliases)], 1
        page = resp.text
        results = []
        for item in re.split(r'<div class="item"', page)[1:]:
            link = re.search(r'<h3 class="title">\s*<a href="(https://mangakatana\.com/manga/[^"]+)"[^>]*>([^<]+)</a>', item)
            if not link:
                continue
            cover = re.search(r'<img (?:lazy-)?src="([^"]+)"', item)
            status = re.search(r'<div class="status [^"]*">(?:<i[^>]*></i>)?\s*([^<]+)<', item)
            latest = re.search(r'<div class="chapter"><a [^>]*>([^<:]+)', item)
            genres = re.search(r'<div class="genres[^"]*">(.*?)</div>', item, re.S)
            desc = re.search(r'<div class="summary[^"]*">(.*?)</div>', item, re.S)
            results.append(SearchResult(
                title=html.unescape(link.group(2)).strip(),
                url=link.group(1),
                cover_url=cls._jpg(cover.group(1)) if cover else "",
                status=status.group(1).strip() if status else "",
                latest=html.unescape(latest.group(1)).strip().replace("Chapter ", "Ch. ") if latest else "",
                genres=[html.unescape(g) for g in re.findall(r">([^<]+)</a>", genres.group(1))] if genres else [],
                description=_plain(desc.group(1)) if desc else "",
                site=cls.name,
            ))
        more = f"/page/{page_no + 1}?" in page
        return results, page_no + 1 if more else page_no

    @classmethod
    def lookup(cls, name: str, limiter: RateLimiter) -> list[SearchResult]:
        """Its search only looks at the main title unless asked to look at the other names."""
        found = cls.search(name, limiter)[0]
        site = cls("", limiter)
        resp = site._get("https://mangakatana.com/?" + urlencode({"search": name.strip(), "search_by": "alt_name"}))
        if cls.url_re.search(resp.url):
            one = cls(resp.url, limiter)
            info = one._parse(resp.text, with_cover=False)
            found.append(SearchResult(info.title, one.title_url, author=info.author, site=cls.name,
                                      aliases=info.aliases))
        return found

    def page_urls(self, chapter: Chapter) -> list[str]:
        page = self._get(chapter.url).text
        lists = [re.findall(r"'(https?://[^']+)'", body) for body in re.findall(r"var \w+\s*=\s*\[([^\]]*)\]", page)]
        urls = max(lists, key=len, default=[])  # one array is a decoy with a single picture
        if not urls:
            raise SiteError(f"No pages found for chapter {fmt_num(chapter.number)}")
        return urls


class MangaFreak(Site):
    """mangafreak.me: plain HTML, often the first with a new chapter. Extras are lettered (16a, 16e),
    read as 16.1, 16.5."""
    name = "mangafreak.me"
    home = "https://ww3.mangafreak.me/"
    example = "https://ww3.mangafreak.me/Manga/<Title>"
    url_re = re.compile(r"mangafreak\.\w+/Manga/([A-Za-z0-9_]+)", re.I)
    base = "https://ww3.mangafreak.me"  # it moves between ww1, ww2, ww3...; follows the redirect
    referer = image_referer = base + "/"
    _user_agent = WeebCentral._user_agent

    def __init__(self, url: str, limiter: RateLimiter):
        super().__init__(limiter)
        match = self.url_re.search(url)
        self.slug = match.group(1) if match else ""

    @property
    def key(self) -> str:
        return f"{self.name}:{self.slug.lower()}"

    @property
    def title_url(self) -> str:
        return f"{self.base}/Manga/{self.slug}"

    def _page(self, url: str) -> requests.Response:
        resp = self._get(url)
        moved = re.match(r"https?://[^/]+", resp.url)
        if moved and moved.group(0) != MangaFreak.base and "mangafreak" in moved.group(0):
            MangaFreak.base = moved.group(0)
        return resp

    @staticmethod
    def _number(label: str) -> float | None:
        m = re.search(r"(\d+)([a-z])?\s*$", label.strip(), re.I)
        if not m:
            return _chapter_number(label)
        extra = (ord(m.group(2).lower()) - 96) / 10 if m.group(2) else 0
        return float(int(m.group(1)) + min(extra, 0.9))

    def info(self, with_cover: bool = True) -> MangaInfo:
        page = self._page(self.title_url).text
        title = re.search(r'<div class="manga_series_data">\s*<h1>([^<]+)</h1>', page)
        found = {}
        for path, label in re.findall(r'class="chapter-link" href="(/Read1_[^"]+)">([^<]+)<', page):
            number = self._number(html.unescape(label))
            if number is not None:
                found[number] = Chapter(number, NO_VOLUME, self.base + path)
        alt = re.search(r"<div>Alternative Title:([^<]*)</div>", page)
        status = re.search(r"<div>This is ([^<]+?) series</div>", page)
        genres = re.search(r'<div class="series_sub_genre_list">(.*?)</div>', page, re.S)
        desc = re.search(r'<div class="manga_series_description">\s*<div>[^<]*</div>\s*<p>(.*?)</p>', page, re.S)
        authors = dict.fromkeys(a.strip() for a in re.findall(r"<div>(?:Written|Illustrated) By:([^<]+)</div>", page))
        cover = re.search(r'<div class="manga_series_image">\s*<img src="([^"]+)"', page)
        info = MangaInfo(
            title=html.unescape(title.group(1)).strip() if title else self.slug.replace("_", " "),
            chapters=sorted(found.values(), key=lambda c: c.number),
            status=status.group(1).replace("-", "").strip().title() if status else "",
            genres=[html.unescape(g).strip() for g in re.findall(r">([^<]+)</a>", genres.group(1))] if genres else [],
            description=_plain(desc.group(1)) if desc else "",
            author=", ".join(html.unescape(a) for a in authors if a),
            aliases=[html.unescape(n).strip() for n in alt.group(1).split(",") if n.strip()] if alt else [],
        )
        if with_cover and cover:
            info.cover = self.cover(cover.group(1))
        return info

    @classmethod
    def search(cls, query: str, limiter: RateLimiter, page_no: int = 1,
               filters: SearchFilters | None = None, sort: str = "") -> tuple[list[SearchResult], int]:
        site = cls("", limiter)
        page = site._page(f"{cls.base}/Find/{quote(query.strip())}?page={page_no}").text
        results = []
        for item in re.split(r'<div class="manga_search_item">', page)[1:]:
            link = re.search(r'<h3>\s*<a href="(/Manga/[^"]+)">([^<]+)</a>', item)
            if not link:
                continue
            cover = re.search(r'<img src="([^"]+)"', item)
            count = re.search(r"(\d+) Chapters? Published\.\s*\(([^)]+)\)", item)
            results.append(SearchResult(
                title=html.unescape(link.group(2)).strip(),
                url=cls.base + link.group(1),
                cover_url=cover.group(1) if cover else "",
                status=count.group(2).strip() if count else "",
                latest=f"Ch. {count.group(1)}" if count else "",
                genres=[html.unescape(g).strip() for g in re.findall(r'href="/Genre/[^"]*">([^<]+)</a>', item)],
                site=cls.name,
            ))
        more = f"page={page_no + 1}\"" in page
        return results, page_no + 1 if more else page_no

    @classmethod
    def lookup(cls, name: str, limiter: RateLimiter) -> list[SearchResult]:
        """Its search finds other names but lists only the main one, so read those off the first few pages."""
        found = cls.search(name, limiter)[0]
        if any(_norm(r.title) == _norm(name) for r in found):
            return found
        for r in found[:3]:
            try:
                info = cls(r.url, limiter).info(with_cover=False)
            except SiteError:
                continue
            r.aliases, r.author = info.aliases, info.author
        return found

    def page_urls(self, chapter: Chapter) -> list[str]:
        page = self._page(chapter.url).text
        urls = list(dict.fromkeys(re.findall(r'<img[^>]+src="(https?://images\.mangafreak\.\w+/mangas/[^"]+)"', page)))
        if not urls:
            raise SiteError(f"No pages found for chapter {fmt_num(chapter.number)}")
        return urls


SITES = [FanFox, MangaDex, WeebCentral, MangaKatana, MangaFreak]


def site_for(url: str):
    return next((s for s in SITES if s.url_re.search(url)), None)


def site_named(name: str):
    """The site with this name, or the first one if it is unknown (e.g. removed since it was saved)."""
    return next((s for s in SITES if s.name == name), SITES[0])


def key_for(url: str) -> str:
    """The history key of a title page, e.g. "fanfox.net:solo_leveling", or "" for an unknown link."""
    site_cls = site_for(url)
    return f"{site_cls.name}:{site_cls.url_re.search(url).group(1).lower()}" if site_cls else ""


def _names(names: list[str]) -> str:
    return names[0] if len(names) == 1 else ", ".join(names[:-1]) + " and " + names[-1]


def _norm(title: str) -> str:
    """A title squeezed down for comparing between sites: "The Boy & the Beast!" -> "boyandthebeast"."""
    t = re.sub(r"^the\s+", "", title.lower().replace("&", " and ").replace("’", "'").strip())
    return re.sub(r"[\W_]+", "", t)


def _same_author(a: str, b: str) -> bool:
    """Lenient, since sites spell names differently (Jang Sung-Lak, Jang Sung-Rak) and some only list
    the artist: true when unknown. Only used to choose between titles with the same name."""
    words = lambda s: {w for w in re.split(r"[\W_]+", s.lower()) if len(w) > 1}
    x, y = words(a), words(b)
    return not x or not y or any(p[:4] == q[:4] for p in x for q in y)


def _latest(text: str) -> float:
    nums = re.findall(r"\d+(?:\.\d+)?", text)
    return float(nums[-1]) if nums else -1.0


def _parallel(jobs: list[Callable[[], object]]) -> list[object]:
    """Run each job on its own thread; a job's result, or the exception it raised."""
    def safe(job):
        try:
            return job()
        except Exception as e:  # handed back so one site failing doesn't stop the others
            return e
    if len(jobs) == 1:
        return [safe(jobs[0])]
    with ThreadPoolExecutor(max_workers=len(jobs)) as pool:
        return list(pool.map(safe, jobs))


class AllSites:
    """Searches every site at once and puts the same title from different sites together, so
    nobody has to pick a site. Looks like a site class to the search box and filters."""
    name = _names([s.name for s in SITES])
    genres = list(dict.fromkeys(g for s in SITES for g in getattr(s, "genres", [])))
    kinds = list(dict.fromkeys(k for s in SITES for k in getattr(s, "kinds", [])))
    sorts = list(dict.fromkeys(o for s in SITES for o in getattr(s, "sorts", [])))

    @staticmethod
    def able(site_cls, filters: SearchFilters | None, sort: str) -> bool:
        """Whether a site can answer this search properly: it knows the order and every wanted genre and kind."""
        if sort and sort not in getattr(site_cls, "sorts", []):
            return False
        if not filters:
            return True
        if not getattr(site_cls, "genres", None):
            return False  # it can't filter at all
        return set(filters.genres) <= set(getattr(site_cls, "genres", [])) and \
            (not filters.kind or filters.kind in getattr(site_cls, "kinds", []))

    @classmethod
    def search(cls, query: str, limiter: RateLimiter, page_no: int = 1,
               filters: SearchFilters | None = None, sort: str = "") -> tuple[list[SearchResult], int]:
        sites = [s for s in SITES if cls.able(s, filters, sort)]
        if not sites:
            return [], page_no
        answers = _parallel([lambda s=s: s.search(query, limiter, page_no, filters, sort) if filters
                             else s.search(query, limiter, page_no, sort=sort) for s in sites])
        good = [a for a in answers if not isinstance(a, Exception)]
        for site_cls, a in zip(sites, answers):
            if isinstance(a, Exception):
                log.warning("search on %s failed: %s", site_cls.name, a)
        if not good:
            raise answers[0]
        merged: list[SearchResult] = []
        by_name: dict[str, SearchResult] = {}
        # Take turns between the sites, so the best matches from each come first.
        for rank in range(max(len(results) for results, _ in good)):
            for results, _ in good:
                if rank >= len(results):
                    continue
                r = results[rank]
                names = {_norm(n) for n in [r.title, *r.aliases]} - {""}
                same = next((by_name[n] for n in names if n in by_name), None)
                if same and r.site not in {site_for(u).name for u in same.urls}:
                    cls._join(same, r)
                    continue
                r = copy.copy(r)
                r.urls = [r.url]
                merged.append(r)
                for n in names:
                    by_name.setdefault(n, r)
        if sort == "A to Z":
            merged.sort(key=lambda r: r.title.lower())
        elif page_no == 1 and query.strip() and not sort:
            wanted = _norm(query)
            merged.sort(key=lambda r: (wanted not in {_norm(n) for n in [r.title, *r.aliases]},
                                       not _norm(r.title).startswith(wanted)))
        return merged, max(pages for _, pages in good)

    @staticmethod
    def _join(into: SearchResult, other: SearchResult) -> None:
        """Fold another site's copy of a title into the result already showing."""
        into.urls.append(other.url)
        into.aliases = list(dict.fromkeys(into.aliases + [other.title] + other.aliases))
        if not into.cover_url and other.cover_url:
            into.cover_url, into.site = other.cover_url, other.site
            into.url, into.urls = other.url, [other.url] + into.urls[:-1]  # the cover's site goes first
        if _latest(other.latest) > _latest(into.latest):
            into.latest = other.latest
        if into.genres is None and other.genres is not None:
            into.genres = other.genres
        into.status = into.status or other.status
        into.author = into.author or other.author
        into.description = into.description or other.description
        into.rating = into.rating or other.rating


class Multi:
    """One title on one or more sites, acting as one site: each chapter comes from the best site
    that has it, and the others step in when that one fails."""

    def __init__(self, sites: list[Site], key: str = ""):
        self.sites = sites  # best first
        self.limiter = sites[0].limiter
        by_order = sorted(sites, key=lambda s: SITES.index(type(s)))
        self.key = key or by_order[0].key
        self.title_url = by_order[0].title_url
        self.sources: dict[str, str] = {s.name: s.title_url for s in by_order}  # site name -> title page
        self.missing: list[str] = []  # sites searched that don't have it

    def with_limiter(self, limiter):
        clone = copy.copy(self)
        clone.sites = [s.with_limiter(limiter) for s in self.sites]
        clone.limiter = limiter
        return clone

    @property
    def name(self) -> str:
        return _names([s.name for s in self.sites])

    def _site(self, name: str) -> Site:
        return next((s for s in self.sites if s.name == name), self.sites[0])

    def sources_for(self, chapter: Chapter) -> list[tuple[Site, Chapter]]:
        return [(self._site(c.source), c) for c in [chapter, *chapter.alts]]

    def page_urls(self, chapter: Chapter) -> list[str]:
        return self._site(chapter.source).page_urls(chapter)

    def image(self, url: str) -> bytes:  # only for code that doesn't know about sources
        return self.sites[0].image(url)

    def image_size(self, url: str) -> int | None:
        return self.sites[0].image_size(url)


def _volume_between(at: tuple[float, int], volumes: dict[tuple[float, int], int]) -> int:
    """A volume for a chapter that site didn't have, from its neighbours there: inside a volume, it's
    that one; between two, the later one if it starts with this chapter's number (29 before 29.5)."""
    before = max((k for k in volumes if k < at), default=None)
    after = min((k for k in volumes if k > at), default=None)
    if before is None or after is None:
        return NO_VOLUME  # before the first or past the last: no way to tell
    if volumes[before] == volumes[after] or int(after[0]) != int(at[0]):
        return volumes[before]
    return volumes[after]


def _best_match(site_cls, queries: list[str], author: str, limiter: RateLimiter) -> SearchResult | None:
    """The title going by one of these names on a site, from its first page of search results."""
    names = {_norm(q) for q in queries}
    for query in queries[:3]:
        results = site_cls.lookup(query, limiter)
        same = [r for r in results if names & {_norm(n) for n in [r.title, *r.aliases]}]
        if same:
            return next((r for r in same if _same_author(author, r.author)), same[0])
    return None


def open_title(urls: list[str], limiter: RateLimiter, title: str = "", key: str = "",
               look_on: list[str] | None = None, with_cover: bool = True,
               names: list[str] = ()) -> tuple[Multi, MangaInfo]:
    """Read a title from every site we know it on and put the chapter lists together.

    `urls` are its known pages (at most one per site); `title` its name as it was saved, kept so
    file names don't change; `look_on` the sites to search by name for it (None: all the others),
    by the names the sites give it plus `names` (e.g. from a search result).
    A site whose page has gone (404) is searched again. The returned Multi has `sources`
    (where it is now) and `missing` (sites searched that don't have it)."""
    known = {}
    for url in urls:
        site_cls = site_for(url)
        if site_cls and site_cls.name not in known:
            known[site_cls.name] = site_cls(url, limiter)
    look = [s.name for s in SITES if s.name not in known] if look_on is None else list(look_on)

    def read(site: Site) -> MangaInfo:
        info = site.info(with_cover)
        if not info.chapters:  # fanfox answers a page that's gone with an empty one
            raise SiteError(f"No chapters found on {site.name} ✦", 404)
        return info

    found: dict[str, tuple[Site, MangaInfo]] = {}
    errors: list[Exception] = []
    for site, got in zip(known.values(), _parallel([lambda s=s: read(s) for s in known.values()])):
        if isinstance(got, Exception):
            log.info("%s failed for %s: %s", site.name, site.title_url, got)
            errors.append(got)
            if getattr(got, "status", 0) == 404 and site.name not in look:
                look.append(site.name)  # moved or taken down there: look for it again
        else:
            found[site.name] = (site, got)
    titles = [i.title for _, i in found.values()] + [title, *names] + [a for _, i in found.values() for a in i.aliases]
    queries = list({_norm(t): t.strip() for t in reversed(titles) if t.strip().isascii() and _norm(t)}.values())[::-1]
    author = next((info.author for _, info in found.values() if info.author), "")
    look = [n for n in look if n not in found]
    missing = []
    # A site can know the title by more names than we searched with (MangaDex lists "Blue Box" for
    # "Ao no Hako") even when it has no chapters to give, so sites that found nothing get a second
    # search with the names the others matched.
    more: dict[str, str] = {}

    def search(site_cls, queries) -> tuple[Site, MangaInfo] | None:
        hit = _best_match(site_cls, queries, author, limiter)
        if hit:
            more.update({_norm(n): n.strip() for n in [hit.title, *hit.aliases] if n.strip().isascii() and _norm(n)})
        if not hit or (site_cls.name in known and known[site_cls.name].key == key_for(hit.url)):
            return None  # not there, or only the page that just failed
        site = site_cls(hit.url, limiter)
        return site, read(site)

    for round_no in range(2):
        if not look or not queries:
            break
        clss = [site_named(n) for n in look]
        look = []
        for site_cls, got in zip(clss, _parallel([lambda c=c: search(c, queries) for c in clss])):
            if isinstance(got, Exception) and getattr(got, "status", 0) != 404:
                log.info("looking for %s on %s failed: %s", queries[0], site_cls.name, got)  # try again later
            elif got and not isinstance(got, Exception):
                log.info("found %s on %s: %s", queries[0], site_cls.name, got[0].title_url)
                found[site_cls.name] = got
            elif round_no == 0 and not got:
                look.append(site_cls.name)
            else:
                missing.append(site_cls.name)
        known_names = {_norm(q) for q in queries}
        queries = [q for k, q in more.items() if k not in known_names]
        if not queries:
            missing += look
            break
    if not found:
        raise errors[0] if errors else SiteError("Couldn't find that title anywhere ✦")

    # Best first: the newest chapter, then the most chapters, then the usual site order.
    ranked = sorted(found.values(), key=lambda si: (-max(c.number for c in si[1].chapters) if si[1].chapters else 0,
                                                    -len(si[1].chapters), SITES.index(type(si[0]))))
    # Matched by number, and by how many times it came before: some titles start over each season.
    # The others only add whole chapters the best site lacks, and newer ones: sites number extras
    # differently (25.1 on one is 25.6 on another), and taking them all would get them twice.
    last = max(c.number for c in ranked[0][1].chapters)
    chapters: dict[tuple[float, int], list[Chapter]] = {}
    volumes: dict[str, dict[tuple[float, int], int]] = {}
    for rank, (site, info) in enumerate(ranked):
        seen: dict[float, int] = {}
        for c in info.chapters:
            seen[c.number] = seen.get(c.number, -1) + 1
            at = (c.number, seen[c.number])
            if c.volume != NO_VOLUME:
                volumes.setdefault(site.name, {})[at] = c.volume
            if rank and at not in chapters and c.number != int(c.number) and c.number < last:
                continue
            chapters.setdefault(at, []).append(dataclasses.replace(c, source=site.name, alts=[]))
    # Volume numbers from one site only (the best if it has them, else the one with the most), so a
    # volume isn't stitched together from sites that split them differently.
    by_volume = volumes.get(ranked[0][0].name) or max(volumes.values(), key=len, default={})
    merged = []
    for at in sorted(chapters):
        best, *alts = chapters[at]
        merged.append(dataclasses.replace(best, volume=by_volume.get(at, _volume_between(at, by_volume)), alts=alts))

    in_order = [info for _, info in sorted(found.values(), key=lambda si: SITES.index(type(si[0])))]
    first = lambda attr: next((getattr(i, attr) for i in in_order if getattr(i, attr)), None)
    info = MangaInfo(
        title=title or in_order[0].title,  # the site order, not the ranking, so it doesn't flip between runs
        chapters=merged,
        cover=first("cover"),
        status=first("status") or "",
        genres=first("genres") or [],
        description=first("description") or "",
        rating=first("rating") or 0.0,
        author=author,
        aliases=list(dict.fromkeys(n for i in in_order for n in [i.title, *i.aliases])),
        sources=[s.name for s, _ in ranked],
    )
    multi = Multi([s for s, _ in ranked], key)
    multi.missing = missing
    for name, site in known.items():  # couldn't be reached just now: keep it for next time
        if name not in found and name not in missing:
            multi.sources[name] = site.title_url
    return multi, info


# --------------------------------------------------------------------------- planning


def _plain(fragment: str) -> str:
    """Text from a bit of HTML: line breaks kept, tags dropped, runs of spaces squeezed."""
    text = html.unescape(re.sub(r"<[^>]+>", "", re.sub(r"<br\s*/?>", "\n", fragment, flags=re.I)))
    lines = (" ".join(line.split()) for line in text.splitlines())
    return re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()


def fmt_num(n: float) -> str:
    return f"{n:g}"


def chapter_span(chapters: list[Chapter]) -> str:
    first, last = chapters[0].number, chapters[-1].number
    return fmt_num(first) if first == last else f"{fmt_num(first)}-{fmt_num(last)}"


def make_groups(chapters: list[Chapter], per_file: int, files: dict[str, set[str]] | None = None) -> list[Group]:
    """One group per volume; chapters without a volume are split into files of per_file chapters.

    `files` are the files already made (label -> the chapter numbers in it, as fmt_num gives them):
    they keep their chapters, whatever the sites say now, so a title that moved to a site with other
    volumes still matches what's on disk. A chapter numbered inside a file's range goes in that file,
    and so does a smaller file that sits inside a bigger one's range (a chapter one site was missing,
    downloaded on its own later): remaking the bigger file puts it in its place."""
    files = {label: [c for c in chapters if fmt_num(c.number) in keys] for label, keys in (files or {}).items()}
    files = {label: chs for label, chs in files.items() if chs}
    span = lambda chs: (chs[0].number, chs[-1].number)
    for small in sorted(files, key=lambda f: len(files[f])):
        lo, hi = span(files[small])
        homes = [big for big in files if big != small and len(files[big]) > len(files[small])
                 and span(files[big])[0] < lo and hi < span(files[big])[1]]
        if homes:
            home = min(homes, key=lambda f: span(files[f])[1] - span(files[f])[0])
            files[home] = sorted(files[home] + files.pop(small), key=lambda c: c.number)
    taken = {id(c) for chs in files.values() for c in chs}
    rest = []
    for ch in chapters:
        if id(ch) in taken:
            continue
        homes = [f for f, chs in files.items() if span(chs)[0] < ch.number < span(chs)[1]]
        if homes:
            home = min(homes, key=lambda f: span(files[f])[1] - span(files[f])[0])
            files[home] = sorted(files[home] + [ch], key=lambda c: c.number)
        else:
            rest.append(ch)
    groups = [Group(label, chs) for label, chs in files.items()]
    for g in _plain_groups(rest, per_file):
        same = next((f for f in groups if f.label == g.label), None)  # a volume that has a file already
        if same:
            same.chapters = sorted(same.chapters + g.chapters, key=lambda c: c.number)
        else:
            groups.append(g)
    return sorted(groups, key=lambda g: g.chapters[0].number) if files else groups


def _plain_groups(chapters: list[Chapter], per_file: int) -> list[Group]:
    by_volume: dict[int, list[Chapter]] = {}
    for ch in chapters:
        by_volume.setdefault(ch.volume, []).append(ch)
    loose = by_volume.pop(NO_VOLUME, [])
    groups = [Group(f"Vol. {vol:02d}", chs) for vol, chs in sorted(by_volume.items())]
    for i in range(0, len(loose), per_file):
        part = loose[i:i + per_file]
        groups.append(Group(f"Ch. {chapter_span(part)}", part))
    return groups


def estimate_chapter_bytes(site, chapters: list[Chapter], samples: int = 3, per_chapter: int = 5) -> float | None:
    """Average download size of a chapter, from a few chapters spread across the title.

    For each sampled chapter it reads the page list and the sizes of a few pages spread through
    it, then scales up by the page count. About samples * (1 + per_chapter) small requests.
    Sites that don't say how big a page is (MangaKatana) are skipped for the next site with that
    chapter; if none says, one page is downloaded to measure it.
    """
    if not chapters:
        return None
    picks = sorted({round(i * (len(chapters) - 1) / max(1, samples - 1)) for i in range(samples)})

    def measure(chapter: Chapter):
        """(bytes per page, pages) from the first site that gives sizes, else (None, (site, urls))."""
        fallback = None  # a site's page list, to measure by downloading if no site gives sizes
        for src, ch in site.sources_for(chapter):
            try:
                urls = src.page_urls(ch)
                fallback = fallback or (src, urls)
                spread = sorted({round(k * (len(urls) - 1) / max(1, per_chapter - 1))
                                 for k in range(min(per_chapter, len(urls)))})
                first = src.image_size(urls[spread[0]])
                if not first:
                    continue
                sizes = [first] + [n for n in (src.image_size(urls[k]) for k in spread[1:]) if n]
            except (SiteError, IndexError):
                continue
            return sum(sizes) / len(sizes), len(urls)
        return None, fallback

    done = [r for r in _parallel([lambda c=chapters[i]: measure(c) for i in picks]) if isinstance(r, tuple)]
    per_page = [p for p, _ in done if p]
    if not per_page:  # no site said how big its pages are: download one page to see
        for _, fallback in done:
            if fallback and fallback[1]:
                src, urls = fallback
                try:
                    per_page = [len(src.image(urls[len(urls) // 2]))]
                    break
                except SiteError:
                    continue
    if not per_page:
        return None
    page = sum(per_page) / len(per_page)
    totals = []
    for p, rest in done:
        if p:
            totals.append(p * rest)
        elif rest and rest[1]:  # pages known, sizes not: use the size measured elsewhere
            totals.append(page * len(rest[1]))
    return sum(totals) / len(totals) if totals else None


def safe_name(name: str) -> str:
    return re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name).strip(" .") or "manga"


# --------------------------------------------------------------------------- download

EXT_BY_MAGIC = [(b"\xff\xd8", ".jpg"), (b"\x89PNG", ".png"), (b"GIF8", ".gif"), (b"RIFF", ".webp")]


def image_ext(data: bytes) -> str:
    for magic, ext in EXT_BY_MAGIC:
        if data.startswith(magic):
            return ext
    return ".avif" if data[4:12] in (b"ftypavif", b"ftypavis") else ".jpg"


def download_group(site, group: Group, group_dir: Path, cancel: threading.Event,
                   on_progress: Callable[[float, str, int], None]) -> list[Path]:
    """Download every page of a group into group_dir; already-downloaded chapters are skipped.
    on_progress also gets the bytes just downloaded (0 for a page already on disk), for the speed."""
    n = len(group.chapters)
    split: dict[str, dict[float, list[tuple[str, bytes]]]] = {}  # reused file -> its pages by chapter
    for done, ch in enumerate(group.chapters):
        ch_dir = group_dir / f"ch_{ch.number:08.2f}"
        if (ch_dir / ".done").exists():
            continue
        ch_dir.mkdir(parents=True, exist_ok=True)
        if cancel.is_set():
            raise Cancelled
        if ch.number in group.reuse:
            path, inside = group.reuse[ch.number]
            if path not in split:
                split[path] = _split_file(site, Path(path), inside, cancel)
            pages = split[path].get(ch.number)
            if pages:
                for i, (suffix, data) in enumerate(pages, 1):
                    (ch_dir / f"{i:04d}{suffix}").write_bytes(data)
                (ch_dir / ".done").touch()
                on_progress((done + 1) / n, f"Ch. {fmt_num(ch.number)}  ·  from your file", 0)
                continue
        sources = site.sources_for(ch)
        for tried, (src, at_src) in enumerate(sources):
            mark = ch_dir / ".from"
            if not mark.exists() or mark.read_text() != src.name:  # pages differ between sites
                for page in ch_dir.glob("*"):
                    page.unlink()
                mark.write_text(src.name)
            try:
                _download_chapter(src, at_src, ch_dir, cancel,
                                  lambda i, total, got: on_progress((done + i / total) / n,
                                                                    f"Ch. {fmt_num(ch.number)}  ·  page {i}/{total}", got))
                break
            except Cancelled:
                raise
            except Exception as e:
                if tried + 1 == len(sources):
                    raise
                log.warning("chapter %s failed on %s (%s), trying %s", fmt_num(ch.number), src.name, e,
                            sources[tried + 1][0].name)
        (ch_dir / ".done").touch()
    # Only this group's chapters: the folder can hold others left over from an earlier attempt.
    dirs = [group_dir / f"ch_{ch.number:08.2f}" for ch in group.chapters]
    return sorted(p for d in dirs for p in d.glob("*") if not p.name.startswith(".") and ".conv." not in p.name)


def _download_chapter(site, chapter: Chapter, ch_dir: Path, cancel, on_page: Callable[[int, int, int], None]) -> None:
    urls = site.page_urls(chapter)
    for i, url in enumerate(urls, 1):
        if cancel.is_set():
            raise Cancelled
        got = 0
        if not list(ch_dir.glob(f"{i:04d}.*")):
            data = site.image(url)
            (ch_dir / f"{i:04d}{image_ext(data)}").write_bytes(data)
            got = len(data)
        on_page(i, len(urls), got)


def _split_file(site, path: Path, chapters: list[Chapter], cancel) -> dict[float, list[tuple[str, bytes]]]:
    """The pages of a file we made, by chapter. Files keep no chapter marks, so this asks the site
    how many pages each chapter has; if they don't add up (a strip EPUB cut into pages, a chapter
    the site changed since), nothing is reused and those chapters are downloaded as usual."""
    try:
        pages = pages_of(path)
        lists = []  # (site, its page addresses) per chapter
        for ch in chapters:
            if cancel.is_set():
                raise Cancelled
            src, at_src = site.sources_for(ch)[0]
            lists.append((src, src.page_urls(at_src)))
        counts = [len(urls) for _, urls in lists]
    except Cancelled:
        raise
    except Exception as e:
        log.info("can't reuse %s: %s", path, e)
        return {}
    if sum(counts) == len(pages):
        starts = [sum(counts[:i]) for i in range(len(counts))]
    else:  # made from another site, whose copies have the odd extra page (credits, an ad)
        log.info("%s: %d pages, the site says %d; finding where each chapter starts", path, len(pages), sum(counts))
        starts = _chapter_starts(pages, chapters, lists, cancel)
        if starts is None:
            return {}
    out = {}
    for i, ch in enumerate(chapters):
        out[ch.number] = pages[starts[i]:starts[i + 1] if i + 1 < len(starts) else len(pages)]
    log.info("reusing %d chapters from %s", len(chapters), path)
    return out


def _thumb(data: bytes):
    from PIL import Image, ImageOps
    with Image.open(io.BytesIO(data)) as im:
        return ImageOps.grayscale(im).resize((48, 72))


def _unlike(a, b) -> float:
    """How different two thumbnails look, 0 (the same) to 255."""
    from PIL import ImageChops, ImageStat
    return ImageStat.Stat(ImageChops.difference(a, b)).mean[0]


def _chapter_starts(pages: list[tuple[str, bytes]], chapters: list[Chapter], lists: list[tuple[Site, list[str]]],
                    cancel, slack: int = 6) -> list[int] | None:
    """Where each chapter starts in a file's pages, by finding the picture of its first page (from the
    site) near where the page counts put it. None unless every one is a clear match."""
    counts = [len(urls) for _, urls in lists]
    starts, guess = [], 0
    for i, ch in enumerate(chapters):
        if cancel.is_set():
            raise Cancelled
        if i == 0:
            starts.append(0)  # the file starts with it, whatever comes first
            guess = counts[0]
            continue
        src, urls = lists[i]
        try:
            first = _thumb(src.image(urls[0]))
            near = range(max(starts[-1] + 1, guess - slack), min(len(pages), guess + slack + 1))
            scores = sorted((_unlike(first, _thumb(pages[k][1])), k) for k in near)
        except Cancelled:
            raise
        except Exception as e:
            log.info("can't line up chapter %s: %s", fmt_num(ch.number), e)
            return None
        if not scores or scores[0][0] > 30 or (len(scores) > 1 and scores[1][0] - scores[0][0] < 12):
            log.info("chapter %s has no clear start (%s)", fmt_num(ch.number), scores[:2])
            return None
        starts.append(scores[0][1])
        guess = scores[0][1] + counts[i]
    return starts


# Job states reported to on_status.
QUEUED, DOWNLOADING, BUILDING, DONE, SKIPPED, FAILED, CANCELLED = (
    "Queued", "Downloading", "Building", "Done", "Already exists", "Failed", "Cancelled")

# How download workers run (a setting).
THREADS, PROCESSES = "threads", "processes"


def target_path(title: str, group: Group, fmt: str, book_dir: Path) -> Path:
    """Where a group's file (or folder, for images) goes."""
    name = safe_name(f"{title} - {group.label}")
    return book_dir / (name if fmt == "images" else f"{name}.{fmt}")


def file_meta(title: str, group: Group) -> dict:
    """What a file says about itself (see builders.build_file): its series, and its place in it."""
    volumes = {c.volume for c in group.chapters}
    volume = volumes.pop() if len(volumes) == 1 and NO_VOLUME not in volumes else None
    by_volume = group.label.startswith("Vol") and volume is not None
    first = min((c.number for c in group.chapters), default=None)
    return {"series": title, "volume": volume,
            "number": volume if by_volume else (f"{first:g}" if first is not None else None), **group.about}


def make_file(site, index: int, group: Group, fmt: str, title: str, book_dir: Path, work_dir: Path,
              keep_tmp: bool, cancel, report: Callable[..., None],
              build: Callable[..., Path], rebuild: bool = False) -> bool:
    """Download one group and build its file. An existing file is skipped, unless `rebuild` (it is
    missing chapters added since): then a new one is built beside it and replaces it once done.
    Returns False if it failed."""
    target = target_path(title, group, fmt, book_dir)
    out = target
    try:
        if cancel.is_set():
            raise Cancelled
        if target.exists() and not rebuild:
            report(index, SKIPPED, 1.0, str(target))
            return True
        group_dir = work_dir / safe_name(group.label)
        images = download_group(site, group, group_dir, cancel,
                                lambda frac, msg, got: report(index, DOWNLOADING, frac, msg, got))
        report(index, BUILDING, 1.0, f"{'Remaking' if rebuild else 'Making'} {fmt.upper()}")
        if target.exists():
            out = target.with_name(f"rebuilding - {target.name}")
        build(fmt, images, out, f"{title} - {group.label}", file_meta(title, group))
        if out != target:  # swap the finished file in; the old one stays until then
            if target.is_dir():
                shutil.rmtree(target)
            os.replace(out, target)
        if not keep_tmp:
            shutil.rmtree(group_dir, ignore_errors=True)
        report(index, DONE, 1.0, str(target))
        return True
    except Cancelled:
        report(index, CANCELLED, 0.0, "")
        return True
    except Exception as e:  # report and carry on with the other files
        log.exception("%s failed", group.label)
        report(index, FAILED, 0.0, str(e))
        return False
    finally:
        if out != target and out.exists():  # a rebuild that didn't finish; the old file is untouched
            shutil.rmtree(out) if out.is_dir() else out.unlink()


def _process_job(site, index, group, fmt, title, book_dir, work_dir, keep_tmp, cancel, events,
                 rebuild=False) -> bool:
    """Entry point for process mode: runs in a worker process and reports through a queue."""
    return make_file(site, index, group, fmt, title, book_dir, work_dir, keep_tmp, cancel,
                     lambda *status: events.put(status), build_file, rebuild)


def run_jobs(site, title: str, groups: list[Group], fmt: str, book_dir: Path, work_dir: Path,
             keep_tmp: bool, workers: int, cancel: threading.Event,
             on_status: Callable[..., None], mode: str = THREADS,
             rebuild: frozenset[int] = frozenset()) -> list[int]:
    """Download and build each group, `workers` at a time. Returns the indexes that failed.
    Groups whose index is in `rebuild` are made again even if their file exists.

    THREADS: downloads run on threads (the GIL is released while waiting on the network) and
    building runs in a process pool, so CPU-heavy builds use several cores.
    PROCESSES: each file is downloaded and built in its own worker process.
    Either way all workers share one request rate limit.
    """
    book_dir.mkdir(parents=True, exist_ok=True)
    workers = max(1, workers)
    if mode == PROCESSES:
        ok = _run_in_processes(site, title, groups, fmt, book_dir, work_dir, keep_tmp, workers, cancel, on_status,
                               rebuild)
    else:
        with ProcessPoolExecutor(max_workers=min(workers, os.cpu_count() or 1)) as builders, \
                ThreadPoolExecutor(max_workers=workers) as pool:
            def build(*args) -> Path:
                return builders.submit(build_file, *args).result()

            futures = [pool.submit(make_file, site, i, g, fmt, title, book_dir, work_dir, keep_tmp,
                                   cancel, on_status, build, i in rebuild) for i, g in enumerate(groups)]
            ok = [f.result() for f in futures]

    failed = [i for i, good in enumerate(ok) if not good]
    if not keep_tmp and not failed and not cancel.is_set():
        shutil.rmtree(work_dir, ignore_errors=True)
        parent = work_dir.parent
        if parent.exists() and not any(parent.iterdir()):
            parent.rmdir()
    return failed


def _run_in_processes(site, title, groups, fmt, book_dir, work_dir, keep_tmp, workers, cancel,
                      on_status, rebuild=frozenset()) -> list[bool]:
    with Manager() as manager:
        shared_cancel = manager.Event()
        events = manager.Queue()
        remote_site = site.with_limiter(SharedRateLimiter(site.limiter.delay, manager))
        stop = threading.Event()

        def relay() -> None:
            """Forward progress from the workers to on_status, and the cancel button to the workers."""
            while not stop.is_set() or not events.empty():
                if cancel.is_set() and not shared_cancel.is_set():
                    shared_cancel.set()
                try:
                    on_status(*events.get(timeout=0.1))
                except queue.Empty:
                    pass

        relay_thread = threading.Thread(target=relay, daemon=True)
        relay_thread.start()
        try:
            with ProcessPoolExecutor(max_workers=workers) as pool:
                futures = [pool.submit(_process_job, remote_site, i, g, fmt, title, book_dir, work_dir,
                                       keep_tmp, shared_cancel, events, i in rebuild) for i, g in enumerate(groups)]
                ok = []
                for i, f in enumerate(futures):
                    try:
                        ok.append(f.result())
                    except Exception as e:  # the worker process itself died
                        log.exception("worker process for file %d died", i)
                        on_status(i, FAILED, 0.0, str(e))
                        ok.append(False)
        finally:
            stop.set()
            relay_thread.join()
        return ok
