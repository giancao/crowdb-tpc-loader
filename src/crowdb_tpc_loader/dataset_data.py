"""Deterministic synthetic Dataset samples and bounded local input validation."""

from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
import stat
import struct
from contextlib import contextmanager

from .errors import ArgumentError, ValidationError
from .report import atomic_json
from .util import directory_lock, ensure_space, prepare_generate_directory

SCHEMA = [{"name": name, "required": True} for name in ("label", "text", "payload")]
MAX_SAMPLES = 10_000
MAX_INVENTORY = 16 * 1024**2
CHUNK_BYTES = 64 * 1024


def sample_id(seed, index):
    return base64.b64encode(struct.pack(">QQ", seed, index)).decode("ascii")


def inline_fields(seed, index):
    values = {"label": str(index % 16).encode(), "text": f"Synthetic sample {index}, seed {seed}".encode()}
    return {
        name: {"kind": "inline", "data": base64.b64encode(value).decode()} for name, value in values.items()
    }


def payload_chunks(seed, index, length):
    identity = struct.pack(">QQ", seed, index)
    for offset in range(0, length, CHUNK_BYTES):
        yield hashlib.shake_256(identity + struct.pack(">Q", offset)).digest(
            min(CHUNK_BYTES, length - offset)
        )


def validate_counts(samples, payload_bytes, seed):
    if type(samples) is not int or not 1 <= samples <= MAX_SAMPLES:
        raise ArgumentError(f"--samples must be between 1 and {MAX_SAMPLES}")
    if type(payload_bytes) is not int or not 1 <= payload_bytes <= 4 * 1024**2 * 10_000:
        raise ArgumentError("--payload-bytes must fit the Dataset 10000-part upload limit")
    if type(seed) is not int or not 0 <= seed < 2**64:
        raise ArgumentError("--seed must be an unsigned 64-bit integer")


def generate_dataset(output, samples=128, payload_bytes=64 * 1024, seed=0):
    validate_counts(samples, payload_bytes, seed)
    output = prepare_generate_directory(output)
    with directory_lock(output):
        if any(path.name != ".crowdb-tpc.lock" for path in output.iterdir()):
            raise ArgumentError("Dataset output became nonempty; refusing to overwrite it")
        ensure_space(output, samples * payload_bytes + MAX_INVENTORY)
        (output / "payload").mkdir()
        records = []
        for index in range(samples):
            relative = f"payload/{index:08d}.bin"
            md5, sha256 = hashlib.md5(), hashlib.sha256()
            with (output / relative).open("xb") as stream:
                for chunk in payload_chunks(seed, index, payload_bytes):
                    stream.write(chunk)
                    md5.update(chunk)
                    sha256.update(chunk)
            records.append(
                {
                    "id": sample_id(seed, index),
                    "fields": inline_fields(seed, index),
                    "payload": {
                        "path": relative,
                        "length": payload_bytes,
                        "md5": md5.hexdigest(),
                        "sha256": sha256.hexdigest(),
                    },
                }
            )
        inventory = {
            "format_version": 1,
            "kind": "crowdb-synthetic-dataset",
            "seed": seed,
            "sample_count": samples,
            "payload_bytes": payload_bytes,
            "schema": SCHEMA,
            "samples": records,
        }
        atomic_json(output / "inventory.json", inventory)
    return output


def load_inventory(root):
    root = Path(root).expanduser().absolute()
    if root.is_symlink() or not root.is_dir():
        raise ValidationError("Dataset input must be a regular directory")
    root = root.resolve()
    path = root / "inventory.json"
    if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_INVENTORY:
        raise ValidationError("Dataset inventory must be a bounded regular JSON file")
    try:
        value = json.loads(path.read_text())
        validate_inventory(value)
    except (ValueError, KeyError, TypeError, ArgumentError) as exc:
        raise ValidationError(f"Invalid Dataset inventory: {exc}") from exc
    return root, value


def validate_inventory(value):
    if (
        not isinstance(value, dict)
        or value.get("format_version") != 1
        or value.get("kind") != "crowdb-synthetic-dataset"
    ):
        raise ValidationError("Unsupported Dataset inventory format")
    count, size, seed = value["sample_count"], value["payload_bytes"], value["seed"]
    validate_counts(count, size, seed)
    if value["schema"] != SCHEMA or not isinstance(value["samples"], list) or len(value["samples"]) != count:
        raise ValidationError("Dataset schema/sample count mismatch")
    for index, sample in enumerate(value["samples"]):
        if sample["id"] != sample_id(seed, index) or sample["fields"] != inline_fields(seed, index):
            raise ValidationError("Dataset sample identity/inline metadata mismatch")
        payload = sample["payload"]
        if payload["path"] != f"payload/{index:08d}.bin" or payload["length"] != size:
            raise ValidationError("Dataset payload path/length mismatch")
        for key, length in (("md5", 32), ("sha256", 64)):
            digest = payload[key]
            if (
                not isinstance(digest, str)
                or len(digest) != length
                or any(c not in "0123456789abcdef" for c in digest)
            ):
                raise ValidationError(f"Invalid Dataset {key}")


def validate_dataset_input(root):
    root, inventory = load_inventory(root)
    for sample in inventory["samples"]:
        payload = sample["payload"]
        path = root / payload["path"]
        if path.is_symlink() or path.parent.is_symlink() or not path.is_file() or path.resolve() != path:
            raise ValidationError("Dataset payload must be a regular in-directory file")
        if path.stat().st_size != payload["length"]:
            raise ValidationError("Dataset payload length changed")
        md5, sha256 = hashlib.md5(), hashlib.sha256()
        with open_payload(root, sample) as stream:
            while chunk := stream.read(CHUNK_BYTES):
                md5.update(chunk)
                sha256.update(chunk)
        if md5.hexdigest() != payload["md5"] or sha256.hexdigest() != payload["sha256"]:
            raise ValidationError("Dataset payload checksum changed")
    return root, inventory


@contextmanager
def open_payload(root, sample):
    """Open only regular files beneath the validated directory, without symlinks."""
    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    payload_fd = None
    try:
        payload_fd = os.open("payload", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root_fd)
        fd = os.open(Path(sample["payload"]["path"]).name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=payload_fd)
        with os.fdopen(fd, "rb") as stream:
            metadata = os.fstat(stream.fileno())
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_size != sample["payload"]["length"]:
                raise ValidationError("Dataset payload type/length changed")
            yield stream
    finally:
        if payload_fd is not None:
            os.close(payload_fd)
        os.close(root_fd)
