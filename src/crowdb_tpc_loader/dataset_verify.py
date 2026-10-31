"""Read-only verification of the exact Dataset snapshot recorded by a load."""

from .dataset_wire import verify_stream
from .errors import LoadError


def verify_manifest(actual, inventory, uploads):
    expected = {
        sample["id"]: {
            **sample["fields"],
            "payload": {"kind": "payload", "upload": uploads[sample["id"]]["id"]},
        }
        for sample in inventory["samples"]
    }
    samples = actual.get("samples")
    schema = actual.get("schema")
    if not isinstance(schema, list) or any(
        not isinstance(field, dict) or type(field.get("required")) is not bool for field in schema
    ):
        raise LoadError("Published Dataset schema is malformed")
    schema_fields = {field.get("name"): field for field in schema}
    expected_schema = {field["name"]: field for field in inventory["schema"]}
    if (
        len(schema_fields) != len(schema)
        or schema_fields != expected_schema
        or not isinstance(samples, list)
        or len(samples) != len(expected)
    ):
        raise LoadError("Published Dataset schema/sample count differs from generated data")
    seen = set()
    for sample in samples:
        if (
            sample["id"] in seen
            or sample["id"] not in expected
            or sample.get("fields") != expected[sample["id"]]
        ):
            raise LoadError("Published Dataset sample/field inventory differs from generated data")
        seen.add(sample["id"])


def verify_dataset(client, report, batch_size=16):
    info = client.call("GET")
    if info.get("id") != report["dataset_id"]:
        raise LoadError("Dataset name refers to a different immutable Dataset identity")
    snapshot = report["snapshot_id"]
    found = client.call("GET", "/snapshots/" + snapshot)
    if (
        found.get("id") != snapshot
        or found.get("state") != "published"
        or found.get("operation") != report["operation_id"]
    ):
        raise LoadError("Dataset snapshot publication identity/state mismatch")
    verify_manifest(
        client.call("GET", "/snapshots/" + snapshot + "/manifest"), report["inventory"], report["uploads"]
    )
    totals = {"samples": 0, "fields": 0, "bytes": 0}
    samples = report["inventory"]["samples"]
    for offset in range(0, len(samples), batch_size):
        batch = samples[offset : offset + batch_size]
        request = {
            "snapshot": snapshot,
            "sample_ids": [sample["id"] for sample in batch],
            "fields": [field["name"] for field in report["inventory"]["schema"]],
        }
        with client.request("POST", "/reads", value=request) as response:
            if (
                response.headers.get("Content-Type", "").split(";", 1)[0].strip()
                != "application/vnd.crowdb.dataset-batch.v1"
            ):
                raise LoadError("Dataset read response has an unexpected content type")
            counts = verify_stream(response.raw, snapshot, batch)
        for key, value in counts.items():
            totals[key] += value
    totals["is_latest"] = info.get("latest") == snapshot
    return totals
