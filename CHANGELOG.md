# Changelog

## 0.1.3

- Write the target table's field IDs into Parquet files before an Iceberg load; refuse to upload a file whose IDs differ from the created table.
- Require PyIceberg 0.11.1 or later in the 0.11 series, whose `add_files` accepts files with field IDs.

## 0.1.2

- Place new table data below a readable table path and use the catalog warehouse/namespace/table location when advertised.
- Keep Parquet object names stable without embedding the loader run ID; retain the run ID in recovery and snapshot metadata.

## 0.1.1

- Share one S3 upload client and up to eight connections across tables, retaining request-local credentials.
- Calculate file MD5 concurrently, send Content-MD5, and stream uploads without payload SHA256 prereads.
- Compute multipart file and part checksums in one read pass; keep conditional PUT without an existence HEAD.
- Fetch Catalog configuration and initialize the namespace once per run; reuse successful commit metadata.
- Batch concurrent recovery records and remove intermediate report saves and duplicate-file scans.
- Record checksum and transfer timings separately, retain SHA256 verification for older reports, and use SF=1 for performance tests.

## 0.1.0

- Load TPC-H and TPC-DS datasets into Iceberg tables, with validation and recoverable reports.
- Write separate tables concurrently, with 8 workers by default and a configurable 1–24 range.
- Retry transient Catalog 503 responses during table creation without retrying ambiguous table commits.
- Verify the published CROWDB Iceberg image with a small TPC-H load and DuckDB Q1 read.
- Add CI checks and a manually triggered PyPI release workflow.
