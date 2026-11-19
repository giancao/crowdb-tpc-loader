# CrowDB TPC Loader

Generate TPC-H or TPC-DS Parquet for Iceberg or standalone S3, and generate synthetic samples for CROWDB Dataset. Iceberg loading creates 8 TPC-H or 24 TPC-DS tables. Synthetic Dataset samples are separate test data. The tool does not run benchmark SQL queries or claim TPC certification.

- Repository: [buzzcrow/crowdb-tpc-loader](https://github.com/buzzcrow/crowdb-tpc-loader)
- End-to-end guide: [CROWDB TPC loader documentation](https://crowdb.dev/docs/tpc-loader/)

## Install

Python 3.10–3.12 is required. Install the published package from PyPI:

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install crowdb-tpc-loader
```

For local development, install from a checkout with `python -m pip install --only-binary=:all: -e .`. The first TPC-H run may download `tpchgen-cli` 3.0.0; TPC-DS may download DuckDB's `tpcds` extension. Use `--no-download` and provide these components ahead of time for an offline run.

## Load

Start `crowdb/crowdb-iceberg:latest` and export the `ICEBERG_URI` and `ICEBERG_TOKEN` values printed by `docker exec <container> crowdb-monitor credentials show --format env`. Keep the token private. Use a new namespace for each run:

```sh
crowdb-tpc-loader load --benchmark tpch --sf 1 \
  --namespace tpch_demo --report-file ./tpch-demo.json
crowdb-tpc-loader load --benchmark tpcds --sf 1 \
  --namespace tpcds_demo --upload-workers 4 --report-file ./tpcds-demo.json
```

The loader validates the entire dataset before creating a table. It then rewrites each validated Parquet file in place with the field IDs a new table assigns (`1..N` in column order), keeping row groups, codecs and physical types; this needs free space for one extra file. Uploads, checksums and `--keep-files` use these final files, and a file whose field IDs differ from the created table is refused before upload. `generate` and `--target s3` leave generator output unchanged. TPC-H and TPC-DS use the same load flow. Different tables calculate MD5 and upload concurrently, with 8 workers by default. `--upload-workers N` selects 1–24 scheduled table writes; the S3 path caps active workers and connections at 8. One S3 client shares its connection pool, with each request signed using that table's catalog credentials. Files stream from disk without a whole-file buffer. S3 PUT and multipart parts send `Content-MD5`; file payload SHA256 is disabled while SigV4 authentication remains enabled. PUT uses `If-None-Match: *` without an existence HEAD.

Each table's files are registered in one snapshot. Required recovery records are durable before table creation, each upload and commit; concurrently ready records share one report save. Catalog configuration and namespace setup happen once per run. A successful commit reuses its returned table metadata, then verifies the snapshot's file inventory. Ambiguous commits are checked without repeating `add_files`. One table's failure does not roll back successful tables. See [recovery](docs/RECOVERY.md) before retrying. An existing table stops the default load; `--on-exists skip` leaves it unchanged without verifying it.

After all tables are committed and verified, local staging is removed unless `--keep-files` is set. Failed and uncertain runs retain local files. Reports contain `md5`, `md5_duration_seconds`, `transfer_duration_seconds`, and the combined `upload_duration_seconds`. For non-S3 FileIO the checksum is calculated inline while copying, and separate checksum time is unavailable. Performance checks use SF=1; record generation, transfer and commit time separately.

TPC-H writes one file per large table for every 10 scale-factor units; `--parts N` chooses another count.

To test readers of merge-on-read deletes, add `--delete-fraction 0.1` to a load. Each table then gets a second snapshot with one delete file per data file. `--delete-mode position` (default) deletes every Nth row by file position; `--delete-mode equality` deletes every Nth row by its primary key (TPC-H only). `--delete-tables orders,lineitem` limits it to some tables. Tables are format-version 2; deletion vectors are not written because PyIceberg cannot write format-version 3. The delete commit is never retried.

For local Parquet only, use `crowdb-tpc-loader generate --benchmark tpch --sf 1 --output-dir ./tpch-sf1`.

Normal load does not download uploaded files to recheck MD5, decode every row, or execute SQL. The server validates `Content-MD5` for each S3 upload. PyIceberg reads Parquet footers for registration and small manifest metadata for commit verification.

The native CrowDB FileIO refreshes expiring table credentials through the catalog's authenticated refresh endpoint. Open readers and Arrow scans can cross a credential window without reloading the table. Refresh preserves the object position and table scope; a rejected refresh stops the read. This does not retry uploads or uncertain commits.

Uploaded Parquet names are scoped below the catalog-provided table location as `<table>/part-<index>.parquet` (the catalog may add its standard `data/` component). The run ID remains in the report, table properties, and snapshot properties for recovery and audit; it is not part of the object name. Iceberg metadata such as `metadata/*.avro` remains under the catalog's table location; those names and locations are assigned by the catalog. Data files do not include a snapshot ID because one file may be referenced by multiple snapshots and the ID is only known after commit.

## Check the result

### Standalone S3 injection

The new S3 and Dataset commands require a checkout containing these commands; they have not been released to PyPI. Install that checkout as described above and check `--help`.

Use an existing bucket, a new empty prefix, `S3_ENDPOINT`, and private `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment values. `AWS_REGION` and `AWS_SESSION_TOKEN` are optional. No Iceberg catalog is required:

```sh
crowdb-tpc-loader load --target s3 --benchmark tpch --sf 0.01 \
  --s3-bucket test-data --s3-prefix run-001/tpch --verify \
  --report-file ./s3-run-001.json
```

All TPC data is generated and validated before uploading files. Each run writes beneath `<prefix>/<run-id>/`, recorded as `s3_prefix` in its report, so independent runs use distinct keys. This path uses ordinary PUT and multipart publication because CROWDB does not support conditional publication headers. Files and a final inventory remain in S3; no Iceberg tables are created. `--verify` explicitly streams every object back and checks its length and MD5. Omit it for ordinary uploads that rely on server checksum validation. Failed or uncertain runs retain staging and their object states; they are never automatically retried or overwritten by a later run.

### Synthetic Dataset injection

Generate reproducible binary payloads with inline label/text fields. The same seed, count and size produce the same sample IDs and bytes. Supply private `DATASET_TOKEN` and the service's `DATASET_URI`; the default namespace is `dataset-ns`.

```sh
crowdb-tpc-loader dataset generate --samples 128 --payload-bytes 131073 \
  --seed 42 --output-dir ./synthetic-data
crowdb-tpc-loader dataset load --input-dir ./synthetic-data \
  --name dataset_run_001 --part-size 65536 --verify \
  --report-file ./dataset-run-001.json
crowdb-tpc-loader dataset verify ./dataset-run-001.json
```

For generate-and-load in one command, replace `--input-dir` with `--samples`, `--payload-bytes` and `--seed`. Use a fresh Dataset name. The current generator accepts 1–10000 samples; each payload must fit at most 10000 upload parts, with each part at most 4 MiB. Generation validates every local payload before remote creation. `--upload-workers` selects 1–8 concurrent payload uploads; the default part size is 1 MiB.

Loading journals upload/publication identities, prepares a complete immutable snapshot, then advances latest. Full `--verify` checks every inline field and payload through bounded binary reads. The read-only `dataset verify` command can use `--dataset-uri` to check another cluster endpoint. It reports the snapshot bytes and current `is_latest` observation; successful candidate readback alone does not establish that an uncertain publication advanced latest. User-provided input directories are never removed.

### Iceberg verification

The loader journals each data file's S3 URI before uploading it and registers
the uploaded files with PyIceberg; standard manifests contain those URIs.
Commit reconciliation never
retries an ambiguous `add_files` operation. Location providers cannot escape
the native table prefix.

Run the read-only verifier from a checkout after loading:

```sh
python scripts/verify_crowdb.py ./tpch-demo.json --require-complete --iceberg-scan
```

With DuckDB's `iceberg` and `httpfs` extensions, attach the REST Catalog using its token and query `tpch_demo.region` or run TPC-H Q1 against `tpch_demo.lineitem`. The [website guide](https://crowdb.dev/docs/tpc-loader/) has the SQL. At SF 0.01, DuckDB 1.5.6 ran all 22 TPC-H and 99 TPC-DS queries against the published `latest` container; results matched DuckDB reading the same local Parquet. This is a development check, not a benchmark result. See [compatibility](docs/COMPATIBILITY.md) and the [test record](docs/TEST_REPORT.md).

## Develop and publish

```sh
python -m pip install --only-binary=:all: -e '.[dev,sql-test]'
ruff check src tests scripts
ruff format --check src tests scripts
python -m pytest
python -m build
python -m twine check dist/*
```

CI runs lint, format, tests, and package checks. Publishing is manual through [the PyPI workflow](.github/workflows/publish.yml) from a `release/v<version>` branch after configuring PyPI Trusted Publishing. See [testing](docs/TESTING.md) for optional real generator and CROWDB runs. The package is Apache-2.0; third-party generators and libraries keep their own licenses.

To publish a later version:

1. Keep the existing PyPI Trusted Publisher for project `crowdb-tpc-loader`: GitHub owner `buzzcrow`, repository `crowdb-tpc-loader`, workflow `publish.yml`, environment `pypi`. Keep the matching `pypi` environment in GitHub.
2. Bump the version in `pyproject.toml` and `src/crowdb_tpc_loader/__init__.py`, then run CI. Create and push `release/v<version>` from the commit to publish.
3. In GitHub Actions, open **Publish to PyPI**, click **Run workflow**, select that release branch, then run it. The workflow verifies the branch name against the package version, reruns checks, builds distributions, and publishes through OIDC. No PyPI API token is stored in GitHub.
4. Confirm the files on [PyPI](https://pypi.org/project/crowdb-tpc-loader/), then test `python -m pip install --no-cache-dir crowdb-tpc-loader==<version>` in a clean environment and run `crowdb-tpc-loader --version`.
