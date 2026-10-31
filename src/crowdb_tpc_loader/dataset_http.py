"""Small public Dataset v1 HTTP boundary with no automatic mutation retries."""

from __future__ import annotations

import json
from urllib.parse import quote, urlsplit

import requests

from .errors import ArgumentError, LoadError

MAX_JSON = 16 * 1024**2


class DatasetHTTP:
    def __init__(self, origin, namespace, name, token, timeout=60):
        try:
            parsed = urlsplit(origin)
            _ = parsed.port
            valid = (
                parsed.scheme in {"http", "https"}
                and parsed.netloc
                and not any((parsed.username, parsed.password, parsed.query, parsed.fragment))
            )
        except ValueError:
            valid = False
        if not valid:
            raise ArgumentError("Dataset endpoint must be HTTP(S), without userinfo, query or fragment")
        if not namespace or not name or any(len(v.encode()) > 255 for v in (namespace, name)):
            raise ArgumentError("Dataset namespace/name must contain 1–255 UTF-8 bytes")
        if not token or "\n" in token or "\r" in token:
            raise ArgumentError("Dataset requires a nonempty token without newline characters")
        self.origin, self.namespace, self.name = origin.rstrip("/"), namespace, name
        self.token, self.timeout = token, timeout
        self.root = (
            self.origin
            + "/api/dataset/v1/namespaces/"
            + quote(namespace, safe="")
            + "/datasets/"
            + quote(name, safe="")
        )
        self.session = requests.Session()

    def fork(self):
        return DatasetHTTP(self.origin, self.namespace, self.name, self.token, self.timeout)

    def close(self):
        self.session.close()

    def request(self, method, suffix="", *, value=None, data=None):
        headers = {"Authorization": "Bearer " + self.token}
        if data is None:
            headers["Content-Type"] = "application/json"
            body = (
                json.dumps(value, allow_nan=False, separators=(",", ":")).encode()
                if method != "GET"
                else None
            )
            if body is not None and len(body) > 8 * 1024**2:
                raise ArgumentError("Dataset control request exceeds the bounded 8 MiB client limit")
        else:
            headers["Content-Type"] = "application/octet-stream"
            body = data
        response = self.session.request(
            method,
            self.root + suffix,
            headers=headers,
            data=body,
            timeout=self.timeout,
            allow_redirects=False,
            stream=True,
        )
        response.raw.decode_content = True
        if not 200 <= response.status_code < 300:
            try:
                error = response.raw.read(2048).decode(errors="replace")
            finally:
                response.close()
            raise LoadError(f"Dataset {method} {suffix or '/'}: HTTP {response.status_code}: {error}")
        return response

    def call(self, method, suffix="", *, value=None, data=None):
        with self.request(method, suffix, value=value, data=data) as response:
            body = response.raw.read(MAX_JSON + 1)
            if len(body) > MAX_JSON:
                raise LoadError("Dataset JSON response exceeds the bounded client limit")
            return json.loads(body)
