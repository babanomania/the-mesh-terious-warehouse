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
        # Execute once per connection to enable S3 + Iceberg access
        "prequeries": [
            "INSTALL 'httpfs'",
            "LOAD 'httpfs'",
            "INSTALL 'iceberg'",
            "LOAD 'iceberg'",
            f"SET s3_endpoint='{minio_endpoint}'",
            "SET s3_url_style='path'",
            "SET s3_use_ssl=false",
            f"SET s3_access_key_id='{s3_access_key_id}'",
            f"SET s3_secret_access_key='{s3_secret_access_key}'",
        ],
        # Pass DuckDB config via connect_args
        "engine_params": {
            "connect_args": {
                "config": {
                    "s3_endpoint": minio_endpoint,
                    "s3_url_style": "path",
                    "s3_use_ssl": False,
                    "s3_access_key_id": s3_access_key_id,
                    "s3_secret_access_key": s3_secret_access_key,
                }
            }
        },
        # Keep other extras minimal; adjust if needed later
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
                + "'s3.use_ssl': false})"
            )

    # Build Superset Flask app and enter application context before importing models
    app = create_app()
    with app.app_context():
        from superset import db  # type: ignore
        from superset.models.core import Database  # type: ignore
        existing = (
            db.session.query(Database)
            .filter(Database.database_name == db_name)
            .one_or_none()
        )
        if existing is None:
            database = Database(database_name=db_name)
            database.sqlalchemy_uri = uri
            database.extra = json.dumps(extras)
            db.session.add(database)
        else:
            existing.sqlalchemy_uri = uri
            existing.extra = json.dumps(extras)
        db.session.commit()


if __name__ == "__main__":
    main()
