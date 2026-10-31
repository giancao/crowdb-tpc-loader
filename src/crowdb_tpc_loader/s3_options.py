"""Standalone S3 target configuration without catalog or embedded credentials."""

from __future__ import annotations

import os
import re
from urllib.parse import urlsplit

from .errors import ArgumentError
from .models import Options


def standalone_options(args, common, redactor):
    if any((args.catalog_uri, args.token, args.namespace, args.catalog_property, args.py_io_impl)):
        raise ArgumentError("Standalone S3 does not accept Iceberg catalog options")
    if args.on_exists != "error":
        raise ArgumentError("Standalone S3 requires a fresh prefix; --on-exists skip is unsupported")
    endpoint = args.s3_endpoint or os.getenv("S3_ENDPOINT")
    try:
        parsed = urlsplit(endpoint or "")
        _ = parsed.port
        valid = (
            parsed.scheme in {"http", "https"}
            and parsed.netloc
            and not any((parsed.username, parsed.password, parsed.query, parsed.fragment))
        )
    except ValueError:
        valid = False
    if not valid:
        raise ArgumentError("S3 requires an HTTP(S) endpoint without userinfo, query or fragment")
    bucket, prefix = args.s3_bucket or "", args.s3_prefix or ""
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
        raise ArgumentError("--s3-bucket must be a valid 3–63 character bucket name")
    parts = prefix.split("/")
    if not prefix or any(
        part in {"", ".", ".."} or not re.fullmatch(r"[A-Za-z0-9_.-]+", part) for part in parts
    ):
        raise ArgumentError("--s3-prefix must be a new nonempty object prefix without empty/dot components")
    properties = {"s3.endpoint": endpoint.rstrip("/"), "http.timeout": str(args.timeout)}
    for key, variable in (
        ("s3.access-key-id", "AWS_ACCESS_KEY_ID"),
        ("s3.secret-access-key", "AWS_SECRET_ACCESS_KEY"),
        ("s3.session-token", "AWS_SESSION_TOKEN"),
    ):
        value = os.getenv(variable)
        if value:
            redactor.add(value)
            if "\n" in value or "\r" in value:
                raise ArgumentError("S3 credentials must not contain newline characters")
            properties[key] = value
        elif variable != "AWS_SESSION_TOKEN":
            raise ArgumentError(f"Standalone S3 requires {variable}")
    properties["s3.region"] = os.getenv("AWS_REGION", "us-east-1")
    return Options(
        **common,
        target="s3",
        s3_endpoint=endpoint.rstrip("/"),
        s3_bucket=bucket,
        s3_prefix=prefix,
        s3_properties=properties,
        verify=args.verify,
        work_dir=args.work_dir,
        keep_files=args.keep_files,
        upload_buffer_mib=args.upload_buffer_mib,
        upload_workers=args.upload_workers,
    )
