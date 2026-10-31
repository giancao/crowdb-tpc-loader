"""Incremental verification of the documented CDBDS001 read framing."""

from __future__ import annotations

import base64
import hashlib
import json
import struct

from .errors import LoadError


def _exact(stream, length):
    parts, remaining = [], length
    while remaining:
        chunk = stream.read(remaining)
        if not chunk:
            raise LoadError("Truncated Dataset read frame")
        parts.append(chunk)
        remaining -= len(chunk)
    return b"".join(parts)


def _expected(samples):
    fields = {}
    for sample in samples:
        for name, value in sample["fields"].items():
            body = base64.b64decode(value["data"], validate=True)
            fields[sample["id"], name] = {
                "length": len(body),
                "md5": hashlib.md5(body).hexdigest(),
                "sha256": hashlib.sha256(body).hexdigest(),
            }
        fields[sample["id"], "payload"] = sample["payload"]
    return fields


def _header(body, snapshot, samples):
    header, expected = json.loads(body), _expected(samples)
    ids = header.get("sample_ids")
    if (
        header.get("snapshot") != snapshot
        or not isinstance(ids, list)
        or len(ids) != len(samples)
        or len(set(ids)) != len(ids)
        or set(ids) != {s["id"] for s in samples}
        or header.get("end") is not True
        or header.get("next") is not None
    ):
        raise LoadError("Dataset read snapshot/sample inventory mismatch")
    fields = header.get("fields")
    if not isinstance(fields, list) or len(fields) != len(expected):
        raise LoadError("Dataset read field inventory mismatch")
    seen = set()
    for field in fields:
        key = field.get("sample"), field.get("field")
        proof = expected.get(key)
        if (
            key in seen
            or proof is None
            or field.get("state") != "value"
            or type(field.get("length")) is not int
            or field["length"] != proof["length"]
            or field.get("md5") != proof["md5"]
        ):
            raise LoadError("Dataset read field metadata mismatch")
        seen.add(key)
    return fields, expected


def verify_stream(stream, snapshot, samples):
    if _exact(stream, 8) != b"CDBDS001":
        raise LoadError("Unknown Dataset read framing version")
    fields, expected = None, None
    index, length, total = 0, 0, 0
    md5, sha256 = hashlib.md5(), hashlib.sha256()
    while True:
        prefix = _exact(stream, 5)
        kind, size = prefix[0], struct.unpack(">I", prefix[1:])[0]
        if kind == 1:
            if fields is not None or not 0 < size <= 8 * 1024**2:
                raise LoadError("Invalid Dataset read header frame")
            fields, expected = _header(_exact(stream, size), snapshot, samples)
        elif kind == 2:
            if fields is None or index >= len(fields) or not 16 < size <= 4 * 1024**2 + 16:
                raise LoadError("Invalid Dataset read data frame")
            body = _exact(stream, size)
            if hashlib.md5(body[16:]).digest() != body[:16]:
                raise LoadError("Dataset data frame checksum mismatch")
            body = body[16:]
            length += len(body)
            if length > fields[index]["length"]:
                raise LoadError("Dataset field exceeds its declared length")
            md5.update(body)
            sha256.update(body)
        elif kind == 3:
            if size or fields is None or index >= len(fields):
                raise LoadError("Invalid Dataset field completion frame")
            field = fields[index]
            proof = expected[field["sample"], field["field"]]
            if (
                length != proof["length"]
                or md5.hexdigest() != proof["md5"]
                or sha256.hexdigest() != proof["sha256"]
            ):
                raise LoadError("Dataset complete-field checksum/length mismatch")
            total += length
            index, length = index + 1, 0
            md5, sha256 = hashlib.md5(), hashlib.sha256()
        elif kind == 0:
            if size or fields is None or index != len(fields) or stream.read(1):
                raise LoadError("Dataset read ended early or contains trailing data")
            return {"samples": len(samples), "fields": len(fields), "bytes": total}
        elif kind == 4:
            if not 0 < size <= 64 * 1024:
                raise LoadError("Invalid Dataset error frame")
            raise LoadError(
                "Dataset server ended the stream with an error: "
                + _exact(stream, size).decode(errors="replace")
            )
        else:
            raise LoadError("Unknown Dataset read frame type")
