"""Download history: which chapters of which manga are downloaded, failed or still waiting.

Stored as JSON in library/history.json next to the script (git-ignored), with covers in
library/covers/. History is kept per chapter, so it stays right if the number of chapters
per file changes between downloads.
"""

from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path

import core
import paths

LIBRARY = paths.DATA / "library"
log = logging.getLogger(__name__)
RETRY_SEARCH = 24 * 3600  # how long to wait before looking for a title again on a site that didn't have it


def chapter_key(number: float) -> str:
    return f"{number:g}"


class History:
    def __init__(self, folder: Path = LIBRARY):
        self.folder = folder
        self.path = folder / "history.json"
        try:
            self.data: dict[str, dict] = json.loads(self.path.read_text())
        except (OSError, ValueError) as e:
            log.info("no history read from %s (%s)", self.path, e)
            self.data = {}

    def save(self) -> None:
        self.folder.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, indent=2))
        tmp.replace(self.path)  # never leave a half-written file behind

    # ---- reading

    def entries(self) -> list[tuple[str, dict]]:
        """The library: most recently used first."""
        shown = [kv for kv in self.data.items() if kv[1].get("in_library", True)]
        return sorted(shown, key=lambda kv: kv[1].get("last_used", 0), reverse=True)

    def in_library(self, key: str) -> bool:
        return self.data.get(key, {}).get("in_library", True) if key in self.data else False

    def newer(self, key: str) -> list[str]:
        """Chapters the sites list past the last one downloaded: what downloading new chapters by
        themselves fetches (gaps left on purpose stay gaps)."""
        entry = self.data.get(key, {})
        have = [float(c) for c in entry.get("downloaded", {})]
        if not have:
            return []
        last = max(have)
        return [c for c in entry.get("listed") or entry.get("known", []) if float(c) > last]

    def files(self, key: str) -> list[Path]:
        """Downloaded files of a manga that are still there."""
        found = {Path(f) for f in self.data.get(key, {}).get("downloaded", {}).values()}
        return sorted(f for f in found if f.exists())

    def get(self, key: str) -> dict | None:
        return self.data.get(key)

    def sources(self, key: str | None) -> list[str]:
        """A title's pages on every site it was found on (older entries only know the one they were added from)."""
        entry = self.data.get(key or "", {})
        return list(dict.fromkeys([*entry.get("sources", {}).values(), *([entry["url"]] if entry.get("url") else [])]))

    def find(self, urls: list[str]) -> str | None:
        """The key of the entry for the title behind any of these pages, whichever site it was first added from."""
        wanted = [k for k in map(core.key_for, urls) if k]
        exact = next((k for k in wanted if k in self.data), None)  # its own entry first
        return exact or next((key for key in self.data if set(wanted) & set(map(core.key_for, self.sources(key)))), None)

    def numbering(self, key: str | None) -> dict[str, list[str]]:
        """The chapter numbers already in the files and in the list last seen, for `core.open_title`."""
        entry = self.data.get(key or "", {})
        return {"downloaded": list(entry.get("downloaded", {})), "seen": entry.get("listed") or entry.get("known", [])}

    def look_on(self, key: str) -> list[str]:
        """The sites worth searching for a title: those it isn't known on, unless they lacked it not long ago."""
        entry = self.data.get(key, {})
        have = {core.site_for(u).name for u in self.sources(key) if core.site_for(u)}
        recent = {n for n, t in entry.get("not_on", {}).items() if time.time() - t < RETRY_SEARCH}
        return [s.name for s in core.SITES if s.name not in have | recent]

    def cover_path(self, key: str) -> Path:
        return self.folder / "covers" / (re.sub(r"[^a-z0-9_.-]+", "_", key.lower()) + ".img")

    def by_chapter(self, key: str) -> list[Path]:
        """The files that are still there, in chapter order (file names like "Ch. 101-110" don't sort)."""
        first: dict[Path, float] = {}
        for c, f in self.data.get(key, {}).get("downloaded", {}).items():
            first[Path(f)] = min(first.get(Path(f), float("inf")), float(c))
        return sorted((f for f in first if f.exists()), key=lambda f: (first[f], f.name))

    def reads(self, key: str) -> dict[str, dict]:
        """Per file: when it was last read here ("read", with "page" and "pages") or opened elsewhere ("opened"),
        and "finished" (when, or 0 for marked unfinished; missing means judge by the page)."""
        return self.data.get(key, {}).get("reads", {})

    def last_read(self, key: str) -> tuple[Path, dict] | None:
        """The file read or opened most recently, if it's still there."""
        done = [(Path(f), r) for f, r in self.reads(key).items() if Path(f).exists()]
        return max(done, key=lambda fr: max(fr[1].get("read", 0), fr[1].get("opened", 0)), default=None)

    @staticmethod
    def finished(r: dict) -> bool:
        """Whether a file counts as finished: marked so by hand, or read here up to its last page."""
        if "finished" in r:
            return bool(r["finished"])
        return bool(r.get("read")) and r.get("page", 0) + 1 >= r.get("pages", 0) > 0

    def up_next(self, key: str) -> Path | None:
        """The earliest file, in chapter order, that isn't finished yet."""
        reads = self.reads(key)
        return next((f for f in self.by_chapter(key) if not self.finished(reads.get(str(f), {}))), None)

    def resume(self, key: str) -> tuple[Path, int, float] | None:
        """Where to carry on reading in the app: the file read here last and its page, or the next
        unfinished file after it (page 0) once that one's finished, with when it was read. None
        if nothing was read here yet, or everything after it is finished."""
        done = [(Path(f), r) for f, r in self.reads(key).items() if r.get("read") and Path(f).exists()]
        if not done:
            return None
        path, r = max(done, key=lambda fr: fr[1]["read"])
        if not self.finished(r):
            return path, r.get("page", 0), r["read"]
        files, reads = self.by_chapter(key), self.reads(key)
        after = files[files.index(path) + 1:] if path in files else []
        nxt = next((f for f in after if not self.finished(reads.get(str(f), {}))), None)
        return (nxt, reads.get(str(nxt), {}).get("page", 0) if reads.get(str(nxt), {}).get("read") else 0,
                r["read"]) if nxt else None

    @staticmethod
    def have(entry: dict) -> int:
        """Downloaded chapters the sites still list: extras only an old site had don't count, so it never
        says "97 of 90"."""
        listed = set(entry.get("listed") or entry.get("known", []))
        downloaded = entry.get("downloaded", {})
        return len(listed & set(downloaded)) if listed else len(downloaded)

    @staticmethod
    def percent(entry: dict) -> float:
        total = entry.get("total_chapters") or 0
        return min(1.0, History.have(entry) / total) if total else 0.0

    # ---- writing

    def remember(self, key: str, url: str, title: str, total: int, status: str, cover: bytes | None) -> dict:
        """Create or refresh a manga's entry after it is looked up."""
        entry = self.data.setdefault(key, {"downloaded": {}, "failed": [], "pending": [], "known": []})
        entry.update(url=url, title=title, total_chapters=total, status=status, last_used=time.time(), in_library=True)
        if cover:
            path = self.cover_path(key)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(cover)
        self.save()
        return entry

    def set_sources(self, key: str, found: dict[str, str], missing: list[str]) -> None:
        """Where a title is now (site name -> its page) and the sites just searched that don't have it."""
        entry = self.data[key]
        entry["sources"] = dict(found)
        if found:
            entry["url"] = next(iter(found.values()))  # still read by older versions
        not_on = {n: t for n, t in entry.get("not_on", {}).items() if n not in found}
        not_on.update({n: time.time() for n in missing})
        entry["not_on"] = not_on
        self.save()

    def mark_seen(self, key: str, chapters: list[str]) -> None:
        """The user has now seen these chapters, so they no longer count as new."""
        entry = self.data[key]
        entry["known"] = entry["listed"] = chapters
        entry["new_chapters"] = 0
        self.save()

    def start_run(self, key: str, chapters: list[str], fmt: str, per_file: int, save_to: str) -> None:
        entry = self.data[key]
        downloaded = entry["downloaded"]
        entry["pending"] = sorted(set(entry["pending"]) | {c for c in chapters if c not in downloaded},
                                  key=float)
        entry["last_run"] = {"format": fmt, "per_file": per_file, "save_to": save_to, "started": time.time()}
        entry["last_used"] = time.time()
        self.save()

    def file_done(self, key: str, chapters: list[str], path: str, made: bool = True) -> None:
        """Chapters saved in a file. With `made` False the file was already there and was left alone:
        chapters known to be in another file that still exists keep pointing there."""
        entry = self.data[key]
        for c in chapters:
            other = entry["downloaded"].get(c)
            if made or not other or other == path or not Path(other).exists():
                entry["downloaded"][c] = path
        done = set(chapters)
        entry["pending"] = [c for c in entry["pending"] if c not in done]
        entry["failed"] = [c for c in entry["failed"] if c not in done]
        entry["last_download"] = time.time()
        log.info("%s: chapters %s saved as %s", key, ",".join(chapters), path)
        self.save()

    def file_failed(self, key: str, chapters: list[str]) -> None:
        entry = self.data[key]
        bad = set(chapters)
        entry["pending"] = [c for c in entry["pending"] if c not in bad]
        entry["failed"] = sorted(set(entry["failed"]) | bad, key=float)
        log.warning("%s: chapters %s failed", key, ",".join(chapters))
        self.save()

    def log_read(self, key: str, path: Path, page: int, pages: int) -> None:
        """Remember where the reader is in a file, so it can carry on from there."""
        r = self.data[key].setdefault("reads", {}).setdefault(str(path), {})
        r.update(read=time.time(), page=page, pages=pages)
        if pages and page + 1 >= pages and not r.get("finished"):
            r["finished"] = time.time()
        self.save()

    def mark_finished(self, key: str, paths: list[Path], done: bool) -> None:
        """Mark files finished (or not) by hand, whatever page they were left on."""
        reads = self.data[key].setdefault("reads", {})
        for path in paths:
            reads.setdefault(str(path), {})["finished"] = time.time() if done else 0
        self.save()

    def log_opened(self, key: str, path: Path) -> None:
        self.data[key].setdefault("reads", {}).setdefault(str(path), {})["opened"] = time.time()
        self.save()

    def checked(self, key: str, total: int, status: str, chapters: list[str]) -> int:
        """Record a background check of the site; returns how many chapters are new."""
        entry = self.data[key]
        known = set(entry.get("known", []))
        new = len([c for c in chapters if c not in known]) if known else 0
        entry.update(total_chapters=total, status=status, new_chapters=new, last_check=time.time(), listed=chapters)
        self.save()
        return new

    @staticmethod
    def _deleted(path: str) -> bool:
        """The file is gone but its folder is still there: deleted or moved out on purpose. When the
        whole folder is missing (renamed, moved, a drive unplugged) the file may well come back."""
        p = Path(path)
        return not p.exists() and p.parent.is_dir()

    def prune_missing(self) -> None:
        """Forget downloads whose files were deleted, so they can be downloaded again. Files in a
        folder that has gone missing are kept (see missing_folders)."""
        changed = False
        for entry in self.data.values():
            gone = [c for c, f in entry.get("downloaded", {}).items() if self._deleted(f)]
            for f in sorted({entry["downloaded"][c] for c in gone}):
                log.warning("%s: forgetting %s, the file was deleted or moved out of its folder", entry.get("title"), f)
            for c in gone:
                del entry["downloaded"][c]
            changed |= bool(gone)
            stale = [f for f in entry.get("reads", {}) if self._deleted(f)]
            for f in stale:
                del entry["reads"][f]
            changed |= bool(stale)
        if changed:
            self.save()

    def missing_folders(self, key: str) -> list[Path]:
        """Folders that downloaded files of this manga were in, which aren't there anymore."""
        folders = {Path(f).parent for f in self.data.get(key, {}).get("downloaded", {}).values()}
        return sorted(f for f in folders if not f.is_dir())

    def relocate(self, key: str, old: Path, new: Path) -> int:
        """The files that were in `old` now live in `new`: point the records there, for the files
        found in it. Returns how many files were found."""
        entry, moved = self.data[key], {}
        for c, f in entry.get("downloaded", {}).items():
            if Path(f).parent == old and (new / Path(f).name).exists():
                moved[f] = str(new / Path(f).name)
                entry["downloaded"][c] = moved[f]
        reads = entry.get("reads", {})
        for f in [f for f in reads if f in moved]:
            reads[moved[f]] = reads.pop(f)
        log.info("%s: %d files found in %s (were in %s)", key, len(moved), new, old)
        self.save()
        return len(moved)

    def forget_folder(self, key: str, folder: Path) -> None:
        """Stop counting the files that were in a missing folder, so they can be downloaded again."""
        entry = self.data[key]
        for c in [c for c, f in entry.get("downloaded", {}).items() if Path(f).parent == folder]:
            del entry["downloaded"][c]
        for f in [f for f in entry.get("reads", {}) if Path(f).parent == folder]:
            del entry["reads"][f]
        log.info("%s: forgot the files in missing folder %s", key, folder)
        self.save()

    @staticmethod
    def folder(entry: dict) -> Path | None:
        """The folder holding most of a manga's downloaded files that still exist."""
        counts: dict[Path, int] = {}
        for f in entry.get("downloaded", {}).values():
            if Path(f).exists():
                counts[Path(f).parent] = counts.get(Path(f).parent, 0) + 1
        return max(counts, key=counts.get) if counts else None

    def forget(self, key: str) -> None:
        """Take a manga off the library. Its files stay, and so does the record of them, so opening
        it again still shows what is downloaded."""
        if self.files(key):
            self.data[key]["in_library"] = False
        else:
            self.data.pop(key, None)
            self.cover_path(key).unlink(missing_ok=True)
        self.save()

    def delete_files(self, key: str) -> int:
        """Delete a manga's downloaded files (only those, then its folder if that leaves it empty)
        and forget it completely. Returns how many files were deleted."""
        files = self.files(key)
        for f in files:
            f.unlink(missing_ok=True)
        for folder in {f.parent for f in files}:
            try:
                folder.rmdir()  # only succeeds when empty
            except OSError:
                pass
        self.data.pop(key, None)
        self.cover_path(key).unlink(missing_ok=True)
        self.save()
        return len(files)


def ago(timestamp: float | None) -> str:
    if not timestamp:
        return "never"
    seconds = time.time() - timestamp
    for size, unit in ((86400 * 365, "year"), (86400 * 30, "month"), (86400 * 7, "week"),
                       (86400, "day"), (3600, "hour"), (60, "minute")):
        if seconds >= size:
            n = int(seconds // size)
            return f"{n} {unit}{'s' if n > 1 else ''} ago"
    return "just now"
