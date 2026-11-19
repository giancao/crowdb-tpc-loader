# TPC Loader Data Injection Plan

Goal: supply reproducible Iceberg, standalone S3 and Dataset data for real
storage testing while preserving existing TPC-H/TPC-DS behavior. This execution
plan is removed after implementation and verification.

Upstream: [usage](../README.md), [recovery](RECOVERY.md), [testing](TESTING.md).
Dataset behavior follows the CROWDB Dataset specification and public HTTP v1
OpenAPI in the sibling CROWDB checkout. Synthetic samples are not a TPC benchmark.

## Scope and compatibility

- Existing `generate/load --benchmark tpch|tpcds` commands keep their behavior.
  Iceberg remains the default load target: S3/FileIO upload, followed by
  snapshot registration.
- Add `load --target s3` for fully validated TPC Parquet in a fresh bucket/prefix,
  without requiring an Iceberg catalog or creating tables. Reuse bounded
  streaming and checksums. Use a UUID-owned child directory below the requested
  fresh prefix; CROWDB rejects conditional publication headers, so standalone
  uploads must not send them. Keep conditional creation as the existing upload
  helper's default for other callers. Never fall back after a failed mutation.
- Add `dataset generate`, `dataset load` and `dataset verify`. Synthetic samples
  have deterministic opaque IDs, inline label/text and generated binary payloads.
  Expose sample count, payload bytes and seed. Support reusing validated local
  input rather than regenerating it for every load.
- Credentials come from the environment and pass through existing redaction.
  Keep endpoint/name configuration separate. No credentials in docs/reports.
- Preserve current uncommitted package/version and file-naming changes. No
  release, version bump, PyPI publication or unrelated cleanup is included.

## Implementation order

- [x] **T01 Baseline**: preserve current diff; inspect CLI, Options, Runner,
  validation, upload helpers and reports. Run existing focused tests before
  changing shared boundaries. Files: `cli.py`, `models.py`, `runner.py`,
  `loader.py`, `report.py`, `s3_upload.py`. Baseline: 191 unit tests pass.
- [x] **T02 Standalone S3**: add `--target iceberg|s3`, default Iceberg. S3 requires
  endpoint, bucket and fresh prefix. Validate all TPC data before uploads; reject
  an occupied target instead of overwriting. Put every run under its UUID child
  directory and omit unsupported conditional publication headers, including
  multipart completion. Upload with bounded workers and
  Content-MD5, journaling remote keys before I/O. Report rows, lengths, digests,
  per-file progress and partial/unknown outcomes. Files: CLI/options, S3-owned
  orchestration and existing upload/report helpers. 197 unit tests pass; real
  target acceptance remains V04.
- [x] **T03 Dataset generation**: use a new/empty output or isolated owned work
  directory. Store versioned inventory, seed/schema, IDs, relative payload paths,
  lengths and MD5/SHA256. Generate in bounded chunks and validate every file
  before creating a remote Dataset. Reject invalid sizes/counts, duplicate IDs,
  corrupt inputs, path escapes and symlinks; check disk space. Files: Dataset
  generator/validation and options modules.
- [x] **T04 HTTP boundary**: use public HTTP with existing requests dependency,
  preserving Python 3.10–3.12 support without a platform-specific SDK requirement.
  Follow canonical base64, upload part/manifest limits, finite request deadlines,
  checksum and authentication contracts. No direct metadata/storage writes or
  hidden mutation retries. Files: Dataset-owned HTTP client and shared errors.
- [x] **T05 Dataset load**: require a fresh name. Persist run, creation, upload and
  publication identities before mutation. Upload each payload under a stable ID
  using bounded parts and checksum proof, then prepare/publish one complete
  manifest under one operation ID. Journal uncertain publication before I/O.
  After a lost reply, read operation status for reconciliation without creating
  a replacement operation or replaying an ambiguous publication. A prepared
  complete resource can already report `published` with `is_latest=false`;
  that state does not prove the head advanced. Verify the actual latest head.
  Retain files
  and reports on ambiguity. Verify snapshot identity and manifest inventory;
  report every created upload and its state/ownership.
- [x] **T06 Read-only Dataset verification**: `dataset verify <report>` pins the
  reported immutable Dataset/snapshot and validates every sample ID, inline
  field, payload length and hash. Decode documented binary framing with bounded
  reads; reject truncation, bad lengths, missing/duplicate fields and integrity
  failures. Include multipart payloads. Normal large load does not silently
  download every payload; full readback is explicit acceptance.
- [x] **T07 CLI/docs**: preserve lightweight help/version, existing commands and
  recovery guarantees. Document generate-only, fresh load, local-input reuse and
  verification. Separate generation, validation, transfer, publication and
  verification metrics. Retain failed artifacts; no automatic remote rollback,
  purge or delete. Files: CLI, target-owned orchestration/reports, README,
  `docs/RECOVERY.md`, `docs/TESTING.md`, and the website guide where applicable.

## Verification

- [x] **V01 Offline tests**: deterministic seeds/IDs/payloads, boundaries, complete
  validation before mutation, input corruption/path rejection, unchanged CLI
  behavior and credential redaction.
- [x] **V02 Protocol/failure tests**: local HTTP server validates paths, auth,
  request bodies, multipart checksums, publication identities, lost-reply status
  reconciliation and binary framing. Check bounded/chunked/truncated reads and
  corruption. Assert no ambiguous publication replay and accurate partial reports.
- [x] **V03 Quality**: run `ruff check src tests scripts`,
  `ruff format --check src tests scripts`, and `python -m pytest` through CROWDB's
  Pixi environment. Keep package metadata and existing safety tests consistent.
- [x] **V04 Real server**: use a fresh owned container namespace. Actually generate
  and load TPC data into Iceberg and run SQL; separately load/read S3 objects and
  generate/load/fully verify Dataset samples, including multipart. Record exact
  source/image, commands, reports, counts and hashes. A skip is not acceptance.
- [ ] **V05 Physical integration**: use this loader as S3/Iceberg/Dataset input
  for CROWDB's `doc/dev/physical-node-test-plan.md`. The native 512-record ChunkKV
  timeout remains a separate case. After fixing it, expand the working set and
  run **30 active minutes** of mixed I/O, retaining injected data as witnesses
  across host loss and retained-volume recreation.

## Completion

All three remote paths need real acceptance, examples matching actual commands,
and reports distinguishing confirmed/unknown results. Implementing a generator
does not close the independent physical storage timeout. Physical execution and
remaining CROWDB defects keep their own evidence and results.

Current evidence: 252 tests passed, 6 opt-in tests skipped in the supported
Python 3.12 / PyIceberg 0.10 / PyArrow 23 environment; ruff check and format
passed. Real TPC-H SF 0.01 generation validated all eight tables (86,805 rows).
Standalone S3 injection and full checksum readback passed. Iceberg injection,
full remote Parquet reads and independent bidirectional SQL comparison passed
for all eight tables. Dataset multipart generation, publication and exact
readback passed (8 samples, 24 fields, 1,048,808 verified field bytes). Website checks
passed. Evidence is retained under CROWDB's `.crowdb-runtime/artifacts/physical-
nodes/physical-20261031T024347Z/`, including `loader-real-s3-final.log`,
`loader-real-iceberg-compatible.log` and `loader-real-dataset-soak3.log`.

Physical integration remains open. Fresh 512- and 4,096-record range-aware
writes passed on an earlier repaired image. Standalone S3 SF1 injection and
full checksums passed for all eight tables (8,661,245 rows). A later Iceberg SF1
run committed seven tables before RootKV OOM prevented lineitem completion.
The latest retained-volume recovery also hit RootKV OOM before loader dispatch;
its original volumes and complete service logs are preserved. No complete
30-minute soak or fault matrix has passed. Complete V05 only after the backend
repair, a fresh expanded seed, the full mixed workload, and recovery audits pass.
