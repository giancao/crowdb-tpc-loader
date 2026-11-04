"""Refresh table-scoped grants before issuing a new object request."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from urllib.parse import urljoin, urlsplit

from pyiceberg.io.pyarrow import PyArrowFileIO

from .errors import CompatibilityError
from .http_fileio import origin
from .rest_catalog import create_catalog

EXPIRY = "s3.session-token-expires-at-ms"
REFRESH = "client.refresh-credentials-endpoint"
KEYS = ("s3.access-key-id", "s3.secret-access-key", "s3.session-token", EXPIRY)


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


@dataclass(frozen=True)
class _Grant:
    filesystem: object
    expires: int
    refresh_after: int
    prefix: str | None


class FileCredentials:
    def __init__(self, properties: dict, bucket: str):
        self.properties = dict(properties)
        self.bucket = bucket
        base = str(properties.get("uri", ""))
        self.url = urljoin(base.rstrip("/") + "/", str(properties[REFRESH]))
        if origin(base) != origin(self.url) or urlsplit(self.url).fragment:
            raise CompatibilityError("File credential refresh must use the catalog origin")
        self.timeout = float(properties.get("http.timeout", 60))
        if not 0 < self.timeout < float("inf"):
            raise CompatibilityError("File credential refresh timeout must be positive and finite")
        self._grant = self._build(properties, None)

    def _build(self, properties: dict, prefix: str | None) -> _Grant:
        try:
            if any(not isinstance(properties[key], str) or not properties[key] for key in KEYS):
                raise ValueError()
            expires = int(properties[EXPIRY])
            if expires <= 0:
                raise ValueError()
        except (KeyError, TypeError, ValueError):
            raise CompatibilityError("File credential response has invalid credentials or expiry") from None
        remaining = max(0, expires - _now_ms())
        margin = min(60_000, remaining // 10)
        filesystem = PyArrowFileIO(dict(properties)).fs_by_scheme("s3", self.bucket)
        return _Grant(filesystem, expires, expires - margin, prefix)

    def resolve(self, path: str) -> _Grant:
        grant = self._grant
        if grant.prefix is not None and not path.startswith(grant.prefix):
            raise PermissionError("Object is outside the refreshed table credential scope")
        if _now_ms() < grant.refresh_after:
            return grant
        renewed = self._refresh(path)
        # Publish one immutable snapshot. Concurrent readers never combine a
        # secret from one response with a token from another response.
        if self._grant.expires < renewed.expires or _now_ms() >= self._grant.refresh_after:
            self._grant = renewed
        grant = self._grant
        if grant.prefix is not None and not path.startswith(grant.prefix):
            raise PermissionError("Object is outside the refreshed table credential scope")
        return grant

    def _refresh(self, path: str) -> _Grant:
        catalog = create_catalog("crowdb_file_credentials", self.timeout, self.properties, configured=True)
        try:
            with catalog._session.get(self.url, stream=True) as response:
                if response.status_code != 200:
                    error = PermissionError if response.status_code in {401, 403} else OSError
                    raise error(f"File credential refresh returned HTTP {response.status_code}")
                data = response.raw.read(65_537)
                if len(data) > 65_536:
                    raise CompatibilityError("File credential response exceeds its size bound")
                try:
                    entries = json.loads(data)["storage-credentials"]
                    matches = []
                    for entry in entries:
                        prefix = urlsplit(entry["prefix"])
                        if (
                            prefix.scheme != "s3"
                            or prefix.netloc != self.bucket
                            or prefix.query
                            or prefix.fragment
                            or not prefix.path.endswith("/")
                            or any(part in {".", ".."} for part in prefix.path.split("/"))
                        ):
                            continue
                        scoped = prefix.netloc + prefix.path
                        if path.startswith(scoped):
                            matches.append((scoped, entry["config"]))
                    if len(matches) != 1:
                        raise ValueError()
                    prefix, config = matches[0]
                    properties = {**self.properties, **{key: config[key] for key in KEYS}}
                except (KeyError, TypeError, ValueError):
                    raise CompatibilityError(
                        "File credential response does not uniquely authorize this object"
                    ) from None
                renewed = self._build(properties, prefix)
                if renewed.expires <= _now_ms():
                    raise CompatibilityError("File credential response is already expired")
                if self._grant.prefix is not None and self._grant.prefix != renewed.prefix:
                    raise CompatibilityError("File credential refresh changed its table scope")
                return renewed
        finally:
            catalog._session.close()
