from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from crowdb_tpc_loader import cli
from crowdb_tpc_loader.deletes import key_rounds
from crowdb_tpc_loader.errors import ArgumentError

EQUALITY = ["--delete-mode", "equality", "--delete-fraction", "0.1"]


def parse(*extra, benchmark="tpch"):
    argv = ["load", "--benchmark", benchmark, "--catalog-uri", "https://x", *extra]
    redactor = cli.initial_redactor(argv)
    return cli.to_options(cli.build_parser(redactor).parse_args(argv), redactor)


def test_key_set_options_parse():
    options = parse(
        *EQUALITY,
        "--delete-key-columns",
        "lineitem=l_shipdate,partsupp=ps_partkey+ps_suppkey",
        "--delete-commits",
        "4",
        "--delete-file-rows",
        "1000",
    )
    assert options.delete_key_columns == {
        "lineitem": ("l_shipdate",),
        "partsupp": ("ps_partkey", "ps_suppkey"),
    }
    assert (options.delete_commits, options.delete_file_rows) == (4, 1000)
    plain = parse(*EQUALITY)
    assert (plain.delete_key_columns, plain.delete_keys_files) == ({}, {})
    assert (plain.delete_commits, plain.delete_file_rows) == (1, 16_000_000)


@pytest.mark.parametrize(
    "extra,message",
    [
        (["--delete-fraction", "0.1", "--delete-commits", "2"], "require --delete-mode equality"),
        (["--delete-mode", "equality", "--delete-commits", "2"], "require --delete-fraction"),
        ([*EQUALITY, "--delete-file-rows", "10"], "--delete-file-rows applies"),
        ([*EQUALITY, "--delete-key-columns", "orders=o_nope"], "unknown or repeated"),
        ([*EQUALITY, "--delete-key-columns", "orders=o_custkey+o_custkey"], "unknown or repeated"),
        ([*EQUALITY, "--delete-key-columns", "store=s_store_sk"], "TABLE=VALUE"),
        ([*EQUALITY, "--delete-key-columns", "orders=o_custkey,orders=o_clerk"], "TABLE=VALUE"),
        ([*EQUALITY, "--delete-key-columns", "orders=o_custkey", "--delete-tables", "lineitem"], "missing"),
        ([*EQUALITY, "--delete-key-columns", "orders=o_custkey", "--delete-keys-file", "orders=k"], "either"),
        ([*EQUALITY, "--delete-commits", "2", "--delete-keys-file", "orders=k"], "cannot be combined"),
        (["--delete-mode", "equality", "--delete-keys-file", "orders=missing.parquet"], "not a readable"),
    ],
)
def test_invalid_key_set_options(extra, message):
    with pytest.raises(ArgumentError, match=message):
        parse(*extra)


def test_equality_keys_file_is_tpch_only():
    with pytest.raises(ArgumentError, match="tpch only"):
        parse("--delete-mode", "equality", "--delete-keys-file", "store=k", benchmark="tpcds")


def table(*rows):
    return SimpleNamespace(name="orders", parts=[SimpleNamespace(rows=count) for count in rows])


def test_rounds_only_for_tables_deleted_by_a_key_set(options):
    equality = replace(options, delete_mode="equality", delete_fraction=0.1)
    # The per-data-file path keeps every table that names no key set.
    assert key_rounds(None, table(5)) is None
    assert key_rounds(replace(options, delete_fraction=0.1), table(5)) is None
    assert key_rounds(equality, table(5)) is None
    assert key_rounds(replace(equality, delete_commits=2, delete_tables=("lineitem",)), table(5, 5)) is None
    assert key_rounds(replace(equality, delete_key_columns={"orders": ("o_custkey",)}), table(5, 5)) == [
        [0, 1]
    ]
    keys_file = replace(options, delete_mode="equality", delete_keys_files={"orders": Path("k")})
    assert key_rounds(keys_file, table(5, 5)) == [[0, 1]]
    # Fewer non-empty files than commits: one round per file; an empty file joins the first round.
    assert key_rounds(replace(equality, delete_commits=4), table(5, 0, 7)) == [[0, 1], [2]]
    assert key_rounds(replace(equality, delete_commits=2), table(1, 1, 1, 1, 1)) == [[0, 1], [2, 3, 4]]
