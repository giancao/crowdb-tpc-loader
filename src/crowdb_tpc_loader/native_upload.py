"""Native injection selects its final S3-readable URL before manifest creation."""

from __future__ import annotations

import hashlib
import secrets
import time
import uuid
from urllib.parse import quote, urlsplit

from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
import requests

from .errors import LoadError
from .util import remote_uri


def enabled(table) -> bool:
    return (
        str(
            getattr(getattr(table, "io", None), "properties", {}).get("crowdb.native-injection", "false")
        ).lower()
        == "true"
    )


def new_upload_id() -> str:
    timestamp = int(time.time() * 1000)
    random = secrets.randbits(74)
    return str(
        uuid.UUID(
            int=(timestamp << 80)
            | (7 << 76)
            | ((random >> 62) << 64)
            | (2 << 62)
            | (random & ((1 << 62) - 1))
        )
    )


def upload(properties, uri, path, size, buffer_size, progress):
    parsed = urlsplit(remote_uri(uri))
    segments = parsed.path.lstrip("/").split("/")
    if parsed.netloc != "iceberg" or len(segments) < 4:
        raise LoadError("Native injection requires the catalog's immutable Iceberg table path")
    filename = segments[-1]
    try:
        identity = uuid.UUID(filename.removesuffix(".parquet")[-36:])
        if identity.version != 7:
            raise ValueError("upload identity must be UUIDv7")
    except ValueError as exc:
        raise LoadError("Native upload identity was not journaled in the upload URI") from exc
    limit = int(properties.get("crowdb.native-injection-max-bytes", 1024**3))
    if size > limit:
        raise LoadError(
            "Native injection part exceeds the catalog's advertised request limit; generate smaller Parquet parts"
        )
    sha256, md5, count = hashlib.sha256(), hashlib.md5(), 0
    hash_started = time.monotonic()
    with path.open("rb") as source:
        while block := source.read(buffer_size):
            sha256.update(block)
            md5.update(block)
            count += len(block)
    if count != size:
        raise LoadError("Local Parquet file changed size before native injection")
    hash_seconds = round(time.monotonic() - hash_started, 6)
    endpoint = urlsplit(properties["s3.endpoint"])
    if endpoint.scheme not in {"http", "https"} or not endpoint.netloc or endpoint.query or endpoint.fragment:
        raise LoadError("Catalog provided an invalid native upload endpoint")
    url = f"{properties['s3.endpoint'].rstrip('/')}/{parsed.netloc}/{quote(parsed.path.lstrip('/'), safe='/')}?crowdb-native-upload={identity}"
    credentials = Credentials(
        properties["s3.access-key-id"], properties["s3.secret-access-key"], properties.get("s3.session-token")
    )
    headers = {
        "Content-Length": str(size),
        "x-amz-content-sha256": sha256.hexdigest(),
        "Content-Type": "application/octet-stream",
    }
    transfer_started = time.monotonic()
    with path.open("rb") as source, requests.Session() as session:
        request = AWSRequest(method="PUT", url=url, data=source, headers=headers)
        S3SigV4Auth(
            credentials, "s3", properties.get("client.region", properties.get("s3.region", "us-east-1"))
        ).add_auth(request)
        with session.put(
            url,
            data=source,
            headers=dict(request.headers),
            timeout=float(properties.get("http.timeout", 60)),
            allow_redirects=False,
            stream=True,
        ) as response:
            response.raise_for_status()
            payload = response.raw.read(8193)
            if len(payload) > 8192:
                raise LoadError("Native injection returned an oversized response")
            import json

            location = remote_uri(json.loads(payload)["location"])
    final = urlsplit(location)
    prefix = "/" + "/".join(segments[:3]) + "/f/v1/"
    if (
        final.scheme != "s3"
        or final.netloc != parsed.netloc
        or not final.path.startswith(prefix)
        or not final.path.endswith(".parquet")
    ):
        raise LoadError("Native injection returned a location outside the owning table")
    progress(size)
    return location, md5.hexdigest(), hash_seconds, round(time.monotonic() - transfer_started, 6)
