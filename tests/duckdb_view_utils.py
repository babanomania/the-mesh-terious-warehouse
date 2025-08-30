from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import List, Tuple, Optional


def _strip_scheme(endpoint: str) -> str:
    return re.sub(r"^https?://", "", endpoint or "")


def _shorten(msg: str, max_len: int = 160) -> str:
    """Shorten and normalize an error message for concise test output."""
    compact = re.sub(r"\s+", " ", str(msg)).strip()
    return compact if len(compact) <= max_len else compact[: max_len - 1] + "\u2026"


@dataclass
class ViewSpec:
    schema: str
    view: str
    path: str  # iceberg table path under warehouse (e.g. dimensions/dim_product)


def connect_duckdb():
    import duckdb  # type: ignore

    db_path = os.getenv("DUCKDB_PATH", "tests/warehouse.duckdb")
    con = duckdb.connect(database=db_path)

    minio_endpoint = _strip_scheme(os.getenv("MINIO_ENDPOINT", "http://localhost:9000"))
    access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("SET s3_url_style='path';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET unsafe_enable_version_guessing=true;")
    con.execute(f"SET s3_endpoint='{minio_endpoint}';")
    con.execute(f"SET s3_access_key_id='{access_key}';")
    con.execute(f"SET s3_secret_access_key='{secret_key}';")
    return con


def warehouse_root() -> str:
    return os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse").rstrip("/")


def iceberg_path(*parts: str) -> str:
    base = warehouse_root()
    suffix = "/".join(p.strip("/") for p in parts if p)
    return f"{base}/{suffix}"


def view_specs() -> List[ViewSpec]:
    wh = warehouse_root()
    specs: List[ViewSpec] = []
    # dimensions
    specs.extend(
        [
            ViewSpec("dimensions", "dim_product", f"{wh}/dimensions/dim_product"),
            ViewSpec("dimensions", "dim_vehicle", f"{wh}/dimensions/dim_vehicle"),
            ViewSpec("dimensions", "dim_warehouse", f"{wh}/dimensions/dim_warehouse"),
            ViewSpec("dimensions", "dim_error_code", f"{wh}/dimensions/dim_error_code"),
            ViewSpec("dimensions", "dim_date", f"{wh}/dimensions/dim_date"),
        ]
    )
    # facts
    specs.extend(
        [
            ViewSpec("orders", "fact_orders", f"{wh}/orders/fact_orders"),
            ViewSpec("returns", "fact_returns", f"{wh}/returns/fact_returns"),
            ViewSpec(
                "dispatch_logs",
                "fact_dispatch_logs",
                f"{wh}/dispatch_logs/fact_dispatch_logs",
            ),
            ViewSpec(
                "inventory",
                "fact_inventory_movements",
                f"{wh}/inventory/fact_inventory_movements",
            ),
            ViewSpec(
                "order_errors",
                "fact_order_errors",
                f"{wh}/order_errors/fact_order_errors",
            ),
        ]
    )
    return specs


def probe_and_create_views(con, specs: List[ViewSpec]) -> Tuple[List[str], List[str]]:
    created: List[str] = []
    skipped: List[str] = []
    for s in specs:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {s.schema};")
        probe_sql = (
            "select 1 from iceberg_scan('" + s.path + "', allow_moved_paths = true) limit 0"
        )
        try:
            con.execute(probe_sql)
        except Exception as exc:  # underlying table not present yet
            skipped.append(f"{s.schema}.{s.view} (missing table at {s.path}: {exc})")
            continue
        q = (
            "CREATE OR REPLACE VIEW "
            + f"{s.schema}.{s.view}"
            + " AS select * from iceberg_scan('"
            + s.path
            + "', allow_moved_paths = true)"
        )
        con.execute(q)
        created.append(f"{s.schema}.{s.view}")
    return created, skipped


def validate_views(con, specs: List[ViewSpec]) -> Tuple[List[str], List[str]]:
    ok: List[str] = []
    bad: List[str] = []
    for s in specs:
        exists = con.execute(
            "select 1 from information_schema.views where table_schema = ? and table_name = ?",
            [s.schema, s.view],
        ).fetchone()
        if not exists:
            continue
        try:
            cnt = con.execute(f"select count(*) from {s.schema}.{s.view}").fetchone()[0]
            ok.append(f"{s.schema}.{s.view} (count={cnt})")
        except Exception as exc:
            bad.append(f"{s.schema}.{s.view} (query failed: {exc})")
    return ok, bad


def ensure_view(
    con, schema: str, view: str, path: str
) -> Tuple[bool, Optional[str]]:
    """Attempt to create or replace a single view for an Iceberg table path.

    Returns (created, skipped_reason). If the underlying table is missing, returns
    (False, reason). On success, returns (True, None).
    """
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    # DuckDB cannot prepare parameters for DDL; avoid prepared params here.
    safe_path = path.replace("'", "''")
    probe_sql = (
        "select 1 from iceberg_scan('" + safe_path + "', allow_moved_paths = true) limit 0"
    )
    try:
        con.execute(probe_sql)
    except Exception as exc:
        return False, f"missing table {path}: {_shorten(exc)}"
    q = (
        "CREATE OR REPLACE VIEW "
        + f"{schema}.{view}"
        + " AS select * from iceberg_scan('"
        + safe_path
        + "', allow_moved_paths = true)"
    )
    con.execute(q)
    return True, None


def validate_view(con, schema: str, view: str) -> Tuple[bool, str]:
    """Validate a single view by running count(*). Returns (ok, message).

    ok is True only if the view exists and has at least one row.
    """
    exists = con.execute(
        "select 1 from information_schema.views where table_schema = ? and table_name = ?",
        [schema, view],
    ).fetchone()
    if not exists:
        return False, "view not found"
    try:
        cnt = con.execute(f"select count(*) from {schema}.{view}").fetchone()[0]
        if cnt and int(cnt) > 0:
            return True, f"count={cnt}"
        return False, f"count={cnt}"
    except Exception as exc:
        return False, f"query failed: {_shorten(exc)}"
