import json
import os
import re

from superset.app import create_app  # type: ignore


def _strip_scheme(endpoint: str) -> str:
    if not endpoint:
        return endpoint
    return re.sub(r"^https?://", "", endpoint)


def main() -> None:
    db_name = os.getenv("DUCKDB_SUPERSET_DB_NAME", "duckdb_iceberg")
    uri = os.getenv("DUCKDB_SUPERSET_URI", "duckdb:////data/warehouse.duckdb")

    # Read MinIO/S3 settings from env (provided via .env)
    minio_endpoint = _strip_scheme(os.getenv("MINIO_ENDPOINT", "minio:9000"))
    s3_access_key_id = os.getenv("MINIO_ROOT_USER", "minioadmin")
    s3_secret_access_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

    extras = {
        # Keep prequeries minimal to avoid duplicate extension init across pooled connections
        "prequeries": [
            f"SET s3_endpoint='{minio_endpoint}'",
            "SET s3_url_style='path'",
            "SET s3_use_ssl=false",
            "SET unsafe_enable_version_guessing=true",
            f"SET s3_access_key_id='{s3_access_key_id}'",
            f"SET s3_secret_access_key='{s3_secret_access_key}'",
        ],
        # Pass DuckDB config via connect_args (applies before prequeries)
        "engine_params": {
            "connect_args": {
                "config": {
                    "s3_endpoint": minio_endpoint,
                    "s3_url_style": "path",
                    "s3_use_ssl": False,
                    "s3_access_key_id": s3_access_key_id,
                    "s3_secret_access_key": s3_secret_access_key,
                    "unsafe_enable_version_guessing": True,
                }
            }
        },
        "metadata_params": {},
        "schemas_allowed_for_file_upload": [],
    }

    # Optionally attach Iceberg REST catalog if requested
    if os.getenv("DUCKDB_ATTACH_ICEBERG_CATALOG", "false").lower() in {"1", "true", "yes"}:
        rest_uri = os.getenv("ICEBERG_REST_URI")
        if rest_uri:
            # DuckDB's iceberg_attach may vary by version; this works on >=0.10
            extras["prequeries"].append(
                "CALL iceberg_attach('iceberg_rest', 'rest', {"
                + f"'uri': '{rest_uri}', "
                + f"'s3.endpoint': '{minio_endpoint}', "
                + f"'s3.access_key_id': '{s3_access_key_id}', "
                + f"'s3.secret_access_key': '{s3_secret_access_key}', "
                + "'s3.url_style': 'path', "
                + "'unsafe_enable_version_guessing': true, "
                + "'s3.use_ssl': false})"
            )

    # Build Superset Flask app and enter application context before importing models
    app = create_app()
    with app.app_context():
        from superset import db  # type: ignore
        from superset.models.core import Database  # type: ignore
        from superset.connectors.sqla.models import SqlaTable  # type: ignore
        from superset.models.slice import Slice  # type: ignore
        from superset.models.dashboard import Dashboard  # type: ignore
        try:
            import duckdb  # type: ignore
        except Exception:
            duckdb = None

        def _duckdb_db_path_from_uri(uri: str) -> str | None:
            # expected formats: duckdb:////absolute/path.duckdb or duckdb:///relative.duckdb
            m = re.match(r"^duckdb:\/\/(\/.*)$", uri)
            return m.group(1) if m else None

        def ensure_duckdb_view(schema: str, name: str, select_sql: str) -> None:
            if duckdb is None:
                return
            db_path = _duckdb_db_path_from_uri(uri)
            if not db_path:
                return
            try:
                con = duckdb.connect(db_path)
            except Exception:
                return
            try:
                con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
                # Add a lightweight time column for charting if missing
                ddl = (
                    f"CREATE OR REPLACE VIEW {schema}.{name} AS "
                    + select_sql
                )
                con.execute(ddl)
            except Exception:
                # Underlying tables/views may not exist yet; skip silently
                pass
            finally:
                try:
                    con.close()
                except Exception:
                    pass

        # 1) Ensure DuckDB database connection exists/updated
        existing_db = (
            db.session.query(Database)
            .filter(Database.database_name == db_name)
            .one_or_none()
        )
        if existing_db is None:
            database = Database(database_name=db_name)
            database.sqlalchemy_uri = uri
            database.extra = json.dumps(extras)
            db.session.add(database)
            db.session.commit()
        else:
            existing_db.sqlalchemy_uri = uri
            existing_db.extra = json.dumps(extras)
            db.session.commit()

        database = (
            db.session.query(Database)
            .filter(Database.database_name == db_name)
            .one()
        )

        # Helpers for idempotent creation of datasets, charts, and dashboard
        def _set_main_dttm(ds: SqlaTable) -> None:
            try:
                names = {c.column_name for c in ds.columns}
            except Exception:
                names = set()
            preferred = ["event_date", "event_ts", "timestamp", "created_at"]
            for cand in preferred:
                if cand in names:
                    ds.main_dttm_col = cand
                    break

        def ensure_dataset(table_name: str, schema: str | None = None) -> SqlaTable:
            ds = (
                db.session.query(SqlaTable)
                .filter(
                    SqlaTable.database_id == database.id,
                    SqlaTable.table_name == table_name,
                    SqlaTable.schema == schema,
                )
                .one_or_none()
            )
            if ds is None:
                ds = SqlaTable(
                    database=database,
                    database_id=database.id,
                    table_name=table_name,
                    schema=schema,
                    sql=None,
                    is_sqllab_view=False,
                )
                db.session.add(ds)
                db.session.flush()
            # Refresh column metadata (safe if table exists)
            try:
                ds.fetch_metadata()
                _set_main_dttm(ds)
            except Exception:
                # If table/view doesn't exist yet, skip; charts using COUNT(*) will still work later
                pass
            db.session.commit()
            return ds

        def ensure_virtual_dataset(
            name: str, sql: str, schema: str | None = None
        ) -> SqlaTable:
            ds = (
                db.session.query(SqlaTable)
                .filter(
                    SqlaTable.database_id == database.id,
                    SqlaTable.table_name == name,
                    SqlaTable.schema == schema,
                )
                .one_or_none()
            )
            if ds is None:
                ds = SqlaTable(
                    database=database,
                    database_id=database.id,
                    table_name=name,
                    schema=schema,
                    sql=sql,
                    is_sqllab_view=True,
                )
                db.session.add(ds)
            else:
                ds.sql = sql
                ds.is_sqllab_view = True
            db.session.flush()
            try:
                ds.fetch_metadata()
                _set_main_dttm(ds)
            except Exception:
                pass
            db.session.commit()
            return ds

        def ensure_chart(
            name: str,
            viz_type: str,
            dataset: SqlaTable,
            params: dict,
        ) -> Slice:
            slc = db.session.query(Slice).filter(Slice.slice_name == name).one_or_none()
            if slc is None:
                slc = Slice(
                    slice_name=name,
                    viz_type=viz_type,
                    datasource_type="table",
                    datasource_id=dataset.id,
                    params=json.dumps(params),
                )
                db.session.add(slc)
            else:
                slc.viz_type = viz_type
                slc.datasource_type = "table"
                slc.datasource_id = dataset.id
                slc.params = json.dumps(params)
            db.session.commit()
            return slc

        def ensure_dashboard(title: str, slices: list[Slice]) -> Dashboard:
            dash = (
                db.session.query(Dashboard)
                .filter(Dashboard.dashboard_title == title)
                .one_or_none()
            )
            if dash is None:
                dash = Dashboard(dashboard_title=title)
                db.session.add(dash)
                db.session.flush()
            # Attach slices (no custom layout, keep simple)
            dash.slices = slices
            db.session.commit()
            return dash

        # 2) Ensure base datasets (physical DuckDB views) exist
        orders_ds = ensure_dataset("fact_orders", schema="orders")
        returns_ds = ensure_dataset("fact_returns", schema="returns")
        dispatch_ds = ensure_dataset("fact_dispatch_logs", schema="dispatch_logs")
        inventory_ds = ensure_dataset("fact_inventory_movements", schema="inventory")
        stockout_ds = ensure_dataset("raw_stockout_risk", schema="ml_insights")

        # 3) Create charts using adhoc SQL metrics to avoid requiring pre-defined metrics
        count_metric = {
            "expressionType": "SQL",
            "sqlExpression": "COUNT(*)",
            "label": "count",
        }

        orders_chart = ensure_chart(
            name="Orders by Warehouse",
            viz_type="bar",
            dataset=orders_ds,
            params={
                "viz_type": "bar",
                "granularity_sqla": "event_date",
                "time_grain_sqla": None,
                "time_range": "No filter",
                "metrics": [count_metric],
                "groupby": ["warehouse_id"],
                "adhoc_filters": [],
                "order_desc": True,
                "row_limit": 1000,
                "color_scheme": "supersetColors",
            },
        )

        # Create a physical view and dataset for returns grouped by warehouse via orders
        returns_by_wh_sql = (
            "SELECT o.warehouse_id AS warehouse_id, COUNT(*) AS total_returns, MIN(r.event_date) AS event_date\n"
            "FROM returns.fact_returns r\n"
            "JOIN orders.fact_orders o ON r.order_id = o.order_id\n"
            "GROUP BY o.warehouse_id"
        )
        ensure_duckdb_view("analytics", "returns_by_warehouse", returns_by_wh_sql)
        returns_by_wh_ds = ensure_dataset("returns_by_warehouse", schema="analytics")

        returns_chart = ensure_chart(
            name="Returns by Warehouse",
            viz_type="bar",
            dataset=returns_by_wh_ds,
            params={
                "viz_type": "bar",
                "granularity_sqla": "event_date",
                "time_grain_sqla": None,
                "time_range": "No filter",
                "metrics": [count_metric],
                "groupby": ["warehouse_id"],
                "adhoc_filters": [],
                "order_desc": True,
                "row_limit": 1000,
                "color_scheme": "supersetColors",
            },
        )

        orders_by_product = ensure_chart(
            name="Orders by Product",
            viz_type="bar",
            dataset=orders_ds,
            params={
                "viz_type": "bar",
                "granularity_sqla": "event_date",
                "time_grain_sqla": None,
                "time_range": "No filter",
                "metrics": [count_metric],
                "groupby": ["product_id"],
                "adhoc_filters": [],
                "order_desc": True,
                "row_limit": 1000,
                "color_scheme": "supersetColors",
            },
        )

        orders_daily = ensure_chart(
            name="Daily Orders",
            viz_type="line",
            dataset=orders_ds,
            params={
                "viz_type": "line",
                "granularity_sqla": "event_date",
                "time_grain_sqla": None,
                "time_range": "No filter",
                "metrics": [count_metric],
                "groupby": [],
                "adhoc_filters": [],
                "row_limit": 1000,
            },
        )

        returns_by_reason = ensure_chart(
            name="Returns by Reason",
            viz_type="bar",
            dataset=returns_ds,
            params={
                "viz_type": "bar",
                "granularity_sqla": "event_date",
                "time_grain_sqla": None,
                "time_range": "No filter",
                "metrics": [count_metric],
                "groupby": ["reason_code"],
                "adhoc_filters": [],
                "order_desc": True,
                "row_limit": 1000,
                "color_scheme": "supersetColors",
            },
        )

        returns_daily = ensure_chart(
            name="Daily Returns",
            viz_type="line",
            dataset=returns_ds,
            params={
                "viz_type": "line",
                "granularity_sqla": "event_date",
                "time_grain_sqla": None,
                "time_range": "No filter",
                "metrics": [count_metric],
                "groupby": [],
                "adhoc_filters": [],
                "row_limit": 1000,
            },
        )

        inv_by_product = ensure_chart(
            name="Inventory Movement by Product",
            viz_type="bar",
            dataset=inventory_ds,
            params={
                "viz_type": "bar",
                "granularity_sqla": "event_date",
                "time_grain_sqla": None,
                "time_range": "No filter",
                "metrics": [
                    {"expressionType": "SQL", "sqlExpression": "SUM(delta_qty)", "label": "sum_delta_qty"}
                ],
                "groupby": ["product_id"],
                "adhoc_filters": [],
                "order_desc": True,
                "row_limit": 1000,
                "color_scheme": "supersetColors",
            },
        )

        inv_daily = ensure_chart(
            name="Daily Inventory Movement",
            viz_type="line",
            dataset=inventory_ds,
            params={
                "viz_type": "line",
                "granularity_sqla": "event_date",
                "time_grain_sqla": None,
                "time_range": "No filter",
                "metrics": [
                    {"expressionType": "SQL", "sqlExpression": "SUM(delta_qty)", "label": "sum_delta_qty"}
                ],
                "groupby": [],
                "adhoc_filters": [],
                "row_limit": 1000,
            },
        )

        dispatch_by_status = ensure_chart(
            name="Dispatch Events by Status",
            viz_type="bar",
            dataset=dispatch_ds,
            params={
                "viz_type": "bar",
                "granularity_sqla": "event_date",
                "time_grain_sqla": None,
                "time_range": "No filter",
                "metrics": [count_metric],
                "groupby": ["status"],
                "adhoc_filters": [],
                "order_desc": True,
                "row_limit": 1000,
                "color_scheme": "supersetColors",
            },
        )

        dispatch_daily = ensure_chart(
            name="Daily Dispatch Events",
            viz_type="line",
            dataset=dispatch_ds,
            params={
                "viz_type": "line",
                "granularity_sqla": "event_date",
                "time_grain_sqla": None,
                "time_range": "No filter",
                "metrics": [count_metric],
                "groupby": [],
                "adhoc_filters": [],
                "row_limit": 1000,
            },
        )

        heatmap_chart = ensure_chart(
            name="Dispatch Events Heatmap",
            viz_type="heatmap",
            dataset=dispatch_ds,
            params={
                "viz_type": "heatmap",
                "all_columns_x": "vehicle_id",
                "all_columns_y": "status",
                "metric": count_metric,
                "granularity_sqla": "event_date",
                "time_range": "No filter",
                "row_limit": 1000,
            },
        )

        stockout_chart = ensure_chart(
            name="Avg Stockout Risk by Product",
            viz_type="bar",
            dataset=stockout_ds,
            params={
                "viz_type": "bar",
                "granularity_sqla": None,
                "time_grain_sqla": None,
                "time_range": "No filter",
                "metrics": [
                    {
                        "expressionType": "SQL",
                        "sqlExpression": "AVG(risk_score)",
                        "label": "avg_risk_score",
                    }
                ],
                "groupby": ["product_id"],
                "adhoc_filters": [],
                "order_desc": True,
                "row_limit": 1000,
                "color_scheme": "supersetColors",
            },
        )

        # 4) Assemble a simple dashboard with the four charts
        ensure_dashboard(
            title="Warehouse Ops Overview",
            slices=[
                orders_chart,
                returns_chart,
                orders_by_product,
                orders_daily,
                returns_by_reason,
                returns_daily,
                inv_by_product,
                inv_daily,
                dispatch_by_status,
                dispatch_daily,
                heatmap_chart,
                stockout_chart,
            ],
        )


if __name__ == "__main__":
    main()
