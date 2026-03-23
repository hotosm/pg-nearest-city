"""Classes representing a URL."""

from __future__ import annotations

import json
import logging
import os
import shutil
import ssl
import tempfile
import urllib.error
import urllib.request
from contextlib import closing
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime, parsedate_to_datetime
from pathlib import Path
from typing import Iterable, Mapping, cast
from urllib.parse import urlparse, urlunparse

from .types import DownloadOutcome, Header, HTTPCode, headers_to_mapping

DataIn = bytes | str | Mapping[str, object] | None


@dataclass(frozen=True, slots=True)
class URLConfig:
    """Represents a dataset resource identified by a URL.

    data: Optional data object to add to the request.
    target_filetype: Optional filetype to use for the result's name.
    target_filename: Optional filename to use for the result's name.
    headers: Optional headers to add to the request.
    url: The URL to be followed.
    """

    url: str
    headers: dict[str, str] = field(default_factory=dict, repr=False)
    data: DataIn = None
    timeout: float = 15.0
    target_filetype: str = field(default="", repr=False)
    target_filename: str = field(default="", repr=False)
    filename: str = field(init=False)
    method: str = field(init=False)
    scheme: str = field(init=False)
    domain: str = field(init=False)
    path: str = field(init=False)
    slug: str = field(init=False)

    def __post_init__(self):
        object.__setattr__(self, "headers", headers_to_mapping(self.headers))
        if "User-Agent" not in self.headers:
            object.__setattr__(
                self,
                "headers",
                self.headers
                | headers_to_mapping({"User-Agent": "python-urlconfig/1.0"}),
            )
        if self.data is not None:
            v = self.data
            if isinstance(v, bytes):
                data_bytes = v
            elif isinstance(v, str):
                data_bytes = v.encode("utf-8")
            else:
                data_bytes = json.dumps(v).encode("utf-8")
            object.__setattr__(self, "method", "POST")
            object.__setattr__(self, "data", data_bytes)
            if "Content-Type" not in self.headers:
                object.__setattr__(
                    self,
                    "headers",
                    self.headers
                    | headers_to_mapping(
                        {"Content-Type": "application/x-www-form-urlencoded"}
                    ),
                )
        else:
            object.__setattr__(self, "method", "GET")

        parsed = urlparse(self.url)
        scheme = (parsed.scheme or "https").strip().lower()
        if scheme not in ("http", "https"):
            raise ValueError(f"Unsupported URL scheme: {scheme!r}")
        domain = parsed.netloc.strip().rstrip("/")
        if not domain:
            raise ValueError("URL must include a host (netloc)")

        path = parsed.path.lstrip("/")
        slug = (
            parsed.path.rstrip("/").rsplit("/", 1)[-1]
            if parsed.path.rstrip("/")
            else ""
        )
        _filename, _filetype = split_name_parts(slug)

        if self.target_filename:
            _filename, fn_ft = split_name_parts(self.target_filename)
            if not self.target_filetype and fn_ft:
                _filetype = fn_ft

        _filetype = (self.target_filetype or _filetype).lstrip(".")

        if not _filename:
            raise RuntimeError(
                f"failed to extract filename from path: {path} and no filename provided"
            )

        object.__setattr__(
            self, "filename", f"{_filename}{'.' if _filetype else ''}{_filetype}"
        )
        object.__setattr__(self, "scheme", scheme)
        object.__setattr__(self, "domain", domain)
        object.__setattr__(self, "path", path)
        object.__setattr__(self, "slug", slug)

        normalized = urlunparse(
            (scheme, domain, "/" + path, "", parsed.query, parsed.fragment)
        )
        object.__setattr__(self, "url", normalized)

    @property
    def header_user_agent(self) -> dict[str, str]:
        """Return the default User-Agent header."""
        return {"User-Agent": "python-urlconfig/1.0"}

    @property
    def normalized_url(self) -> str:
        """Return the normalized URL string."""
        return self.url

    def with_headers(
        self,
        extra: Mapping[str, str] | Iterable[Header],
        override: bool = True,
    ) -> URLConfig:
        """Return a copy with merged headers."""
        base = dict(self.headers)
        new = headers_to_mapping(extra)
        merged: dict[str, str] = {**base, **new} if override else {**new, **base}
        return replace(self, headers=merged)

    def with_overrides(self, **kwargs) -> URLConfig:
        """Return a copy with arbitrary fields overridden."""
        return replace(self, **kwargs)

    def download(
        self,
        output_dir: Path,
        timeout: float = 15.0,
        logger: logging.Logger | None = None,
    ) -> tuple[DownloadOutcome, dict]:
        """Downloads a file unconditionally.

        Returns:
            DOWNLOADED: the file was downloaded.
            FAILED: the file failed to download."
        """
        target = output_dir / self.filename

        return self._http_get_conditional(
            dest_path=target, verify_tls=True, timeout=timeout, logger=logger
        )

    def ensure_cached(
        self,
        cache_dir: Path,
        local_freshness: timedelta = timedelta(days=7),
        timeout_get: float = 15.0,
        verify_tls: bool = True,
        logger: logging.Logger | None = None,
    ) -> DownloadOutcome:
        """Ensure the cached file exists and is up to date.

        Goes directly to conditional GET with stored ETag/Last-Modified.

        Returns:
            DOWNLOADED: the file was downloaded.
            LOCAL_FRESH: the local copy is fresh enough.
            NOT_MODIFIED: the remote copy hasn't been modified vs the local copy.
        """
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
        except OSError:
            pass

        target = cache_dir / self.filename

        def _log(level: str, msg: str):
            if logger is not None:
                getattr(logger, level)(msg)
            else:
                print(f"[{level.upper()}] {msg}")

        if not target.exists():
            _log("info", f"Cache miss: downloading {self.filename}")
            outcome, meta = self._http_get_conditional(
                target, verify_tls, timeout_get, logger=logger
            )
            if outcome == DownloadOutcome.DOWNLOADED:
                self._write_meta(p=target, headers=meta)
            else:
                _log("error", meta.get("err", "unknown error"))
            return outcome

        mtime = datetime.fromtimestamp(target.stat().st_mtime, tz=timezone.utc)
        if datetime.now(tz=timezone.utc) - mtime <= local_freshness:
            return DownloadOutcome.LOCAL_FRESH

        _log("info", f"{self.slug} older than {local_freshness}: conditional GET")
        prior = self._read_meta(p=target)
        outcome, meta = self._http_get_conditional(
            target,
            verify_tls,
            timeout_get,
            etag=prior.get("etag") if prior else None,
            last_modified=(
                prior.get("last_modified") if prior else self._fmt_http(mtime)
            ),
            logger=logger,
        )
        if outcome == DownloadOutcome.DOWNLOADED:
            self._write_meta(p=target, headers=meta)
        return outcome

    def _ctx(self, verify_tls: bool):
        if self.scheme == "https":
            return (
                ssl.create_default_context()
                if verify_tls
                else ssl._create_unverified_context()
            )  # nosec
        return None

    @staticmethod
    def _parse_http(v: str | None) -> datetime | None:
        if not v:
            return None
        try:
            dt = parsedate_to_datetime(v)
            return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).astimezone(
                timezone.utc
            )
        except Exception:
            return None

    @staticmethod
    def _fmt_http(dt: datetime) -> str:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return format_datetime(dt, usegmt=True)

    def _http_get_conditional(
        self,
        dest_path: Path,
        verify_tls: bool,
        timeout: float,
        etag: str | None = None,
        last_modified: str | None = None,
        logger: logging.Logger | None = None,
    ) -> tuple[DownloadOutcome, dict]:

        def _log(level: str, msg: str):
            if logger is not None:
                getattr(logger, level)(msg)
            else:
                print(f"[{level.upper()}] {msg}")

        req = urllib.request.Request(
            self.url,
            data=cast(bytes | None, self.data),
            method=self.method,
            headers=dict(self.headers),
        )
        if etag:
            req.add_header(key="If-None-Match", val=etag)
        if last_modified and not req.has_header("If-None-Match"):
            req.add_header(key="If-Modified-Since", val=last_modified)
        try:
            with closing(
                urllib.request.urlopen(
                    req, timeout=timeout, context=self._ctx(verify_tls)
                )
            ) as resp:
                _log("debug", f"opening request: {req}")
                tmp_fd, tmp_name = tempfile.mkstemp(
                    prefix=".dl_", dir=str(dest_path.parent)
                )
                try:
                    with os.fdopen(tmp_fd, "wb") as tmpf:
                        shutil.copyfileobj(resp, tmpf, length=1048576)
                    os.replace(tmp_name, dest_path)
                except Exception as e:
                    _log("error", str(e))
                    try:
                        os.remove(tmp_name)
                    except Exception:
                        _log("error", str(e))
                    raise
                return DownloadOutcome.DOWNLOADED, dict(resp.headers.items())
        except urllib.error.HTTPError as e:
            if e.code == HTTPCode.NOT_MODIFIED:
                return DownloadOutcome.NOT_MODIFIED, dict((e.headers or {}).items())
            return DownloadOutcome.FAILED, {"err": e}

    @staticmethod
    def _meta_path(p: Path) -> Path:
        return p.with_suffix(p.suffix + ".meta.json")

    @staticmethod
    def _read_meta(p: Path) -> dict | None:
        mp = URLConfig._meta_path(p)
        if not mp.exists():
            return None
        try:
            return json.loads(mp.read_text())
        except Exception:
            return None

    @staticmethod
    def _write_meta(p: Path, headers: dict) -> None:

        mp = URLConfig._meta_path(p)
        payload = {
            "etag": headers.get("ETag"),
            "last_modified": headers.get("Last-Modified"),
            "content_length": headers.get("Content-Length"),
        }
        tmp = mp.with_suffix(mp.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, separators=(",", ":"), sort_keys=True))
        os.replace(tmp, mp)


def split_name_parts(name: str) -> tuple[str, str]:
    """Split a given path into its filename and suffixes."""
    if not name:
        return "", ""
    if name.startswith(".") and name.count(".") == 1:
        return name, ""

    head, sep, tail = name.partition(".")
    return head, tail if sep else ""
