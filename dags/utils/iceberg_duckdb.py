from __future__ import annotations

import os
import re
from typing import Dict, List, Optional


def duckdb_attach_iceberg(con) -> None:
    """Configure DuckDB for S3 + Iceberg (no catalog attach required)."""

    def strip_scheme(endpoint: str) -> str:
        return re.sub(r"^https?://", "", endpoint or "")

    minio_endpoint = strip_scheme(os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
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
    # Use iceberg_scan to read tables by S3 path; no REST attach needed
    return


def table_expr(module: str, table: str) -> str:
    """Return a DuckDB iceberg_scan expression for an Iceberg table path.

    Example: table_expr('orders','raw_orders') ->
      "iceberg_scan('s3://warehouse/orders/raw_orders', allow_moved_paths = true)"
    """
    warehouse = os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse").rstrip("/")
    return (
        f"iceberg_scan('{warehouse}/{module}/{table}', allow_moved_paths = true)"
    )


def max_ts(con, module: str, table: str, ts_col: str) -> Optional[str]:
    """Return max timestamp ISO string from Iceberg table path, or None."""
    try:
        res = con.execute(
            f"select max({ts_col}) as m from {table_expr(module, table)}"
        ).fetchone()
        if not res:
            return None
        m = res[0]
        if m is None:
            return None
        return str(m)
    except Exception:
        return None


def with_ts_filter(base_sql: str, ts_col: str, max_ts_iso: Optional[str]) -> str:
    if not max_ts_iso:
        return base_sql
    try:
        lookback_h = int(os.getenv("INCREMENTAL_LOOKBACK_HOURS", "0"))
    except Exception:
        lookback_h = 0
    lb_clause = (
        f" and {ts_col} > (timestamp '{max_ts_iso}' - INTERVAL {lookback_h} HOUR)"
        if lookback_h > 0
        else f" and {ts_col} > timestamp '{max_ts_iso}'"
    )
    if " where " in base_sql.lower():
        return base_sql + lb_clause
    return base_sql + " where true " + lb_clause


def append_arrow_to_iceberg(table_fqn: str, columns: List[Dict[str, str]], arrow_table) -> None:
    """Ensure table exists in Iceberg and append Arrow data."""
    import pyarrow as pa
    from base_ingest import _load_iceberg_catalog, _ensure_table_exists

    if arrow_table is None:
        return
    if isinstance(arrow_table, pa.Table) and arrow_table.num_rows == 0:
        return
    catalog = _load_iceberg_catalog()
    t = _ensure_table_exists(catalog, table_fqn, columns)
    t.append(arrow_table)
