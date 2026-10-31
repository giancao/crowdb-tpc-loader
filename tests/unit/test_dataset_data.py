import json

import pytest

from crowdb_tpc_loader.dataset_data import generate_dataset, validate_dataset_input
from crowdb_tpc_loader.errors import ArgumentError, ValidationError


def test_generator_is_deterministic_bounded_and_validated(tmp_path):
    a = generate_dataset(tmp_path / "a", samples=3, payload_bytes=65_537, seed=42)
    b = generate_dataset(tmp_path / "b", samples=3, payload_bytes=65_537, seed=42)
    _, first = validate_dataset_input(a)
    _, second = validate_dataset_input(b)
    assert first == second
    assert len({s["id"] for s in first["samples"]}) == 3
    assert all(
        (a / s["payload"]["path"]).read_bytes() == (b / s["payload"]["path"]).read_bytes()
        for s in first["samples"]
    )
    c = generate_dataset(tmp_path / "c", samples=3, payload_bytes=65_537, seed=43)
    assert validate_dataset_input(c)[1]["samples"][0] != first["samples"][0]


@pytest.mark.parametrize("kwargs", [{"samples": 0}, {"samples": 10001}, {"payload_bytes": 0}, {"seed": -1}])
def test_invalid_generation_has_no_output(tmp_path, kwargs):
    with pytest.raises(ArgumentError):
        generate_dataset(tmp_path / "out", **kwargs)
    assert not (tmp_path / "out").exists()


def test_existing_output_is_preserved(tmp_path):
    keep = tmp_path / "keep"
    keep.write_text("owned by someone else")
    with pytest.raises(ArgumentError):
        generate_dataset(tmp_path)
    assert keep.read_text() == "owned by someone else"


def test_corrupted_payload_and_path_escape_are_rejected(tmp_path):
    root = generate_dataset(tmp_path / "data", samples=1, payload_bytes=16)
    path = root / "payload/00000000.bin"
    original = path.read_bytes()
    path.write_bytes(b"x" * 16)
    with pytest.raises(ValidationError, match="checksum"):
        validate_dataset_input(root)
    path.write_bytes(original)
    inventory = json.loads((root / "inventory.json").read_text())
    inventory["samples"][0]["payload"]["path"] = "../outside"
    (root / "inventory.json").write_text(json.dumps(inventory))
    with pytest.raises(ValidationError, match="path"):
        validate_dataset_input(root)


def test_symlinked_payload_is_rejected(tmp_path):
    root = generate_dataset(tmp_path / "data", samples=1, payload_bytes=16)
    path = root / "payload/00000000.bin"
    target = tmp_path / "outside"
    path.rename(target)
    path.symlink_to(target)
    with pytest.raises(ValidationError, match="regular"):
        validate_dataset_input(root)
