# AGENTS.md

This document defines all key agent roles, responsibilities, and associated data contracts within **The Mesh-terious Warehouse** project. It is designed for use by automated code generators like Codex to understand domain boundaries, application architecture, and the complete data lifecycle across ingestion, transformation, and analytics workflows.

---

## Core Architectural Principles

* **Lakehouse Foundation:**

  * Storage: Apache Iceberg on MinIO (columnar table format)
  * Query Engine: DuckDB for in-place analytics
  * DataOps: Apache Airflow for orchestration
  * Metadata: OpenMetadata for lineage, glossary, ownership
  * Messaging: RabbitMQ for ingestion events (simulated real-time)

* **Data Mesh Principles:**

  * Domain-aligned pipelines (each domain is independently owned)
  * Independent dbt models and Airflow DAGs per domain
  * Governance via OpenMetadata policies, ownership, tagging

* **Data Fabric Overlay:**

  * OpenMetadata acts as a discovery + governance hub
  * Provides lineage, glossary terms, access policies
  * Datasets and pipelines are auto-registered and discoverable

* **Application Layers:**

  * Ingestion layer (RabbitMQ event producers, per domain)
  * Orchestration layer (Airflow DAGs)
  * Transformation layer (dbt)
  * Lakehouse storage (Iceberg)
  * Query interface (DuckDB + Superset)
  * ML workflows (Jupyter or Airflow batch inference)

* **Runtime Environment:**

  * Entire stack is containerized via `docker-compose`
  * Each service (MinIO, RabbitMQ, Airflow, OpenMetadata, Superset, DuckDB) is defined as a separate service
  * `.env` file is used for environment configuration and secrets

---

## Suggested Project Structure

```bash
mesh-terious-warehouse/
│
├── docker-compose.yml              # Container orchestration file
├── .env                            # Environment variables
├── ingestion/
│   └── producers/
│       └── <domain>/               # Mock data generators per domain
│           ├── produce_orders_<region>.py
│           ├── produce_returns_<region>.py
│           └── ...
│
├── dags/
│   └── <domain>_dags/              # Airflow DAGs per domain
│       ├── ingest_orders_<region>.py
│       └── ...
│
├── models/
│   └── dbt/
│       └── <domain>/               # dbt models per domain
│
├── metadata/
│   └── openmetadata_bootstrap/     # OpenMetadata workflow and ingestion config
│
├── notebooks/
│   └── forecasting/                # ML notebooks for training/inference
│
└── docs/
    ├── AGENTS.md                   # Codex-oriented agent specification
    └── README.md                   # Project documentation
```

---

## Notes on Data Generators

Each mock data generator simulates a specific operational domain by publishing JSON events to RabbitMQ. Generators follow a naming convention (`produce_<event>_<region>.py`) and include the following behaviors:

* Use `argparse` to support the following modes:

  * `--live`: emits 1 event every N seconds (default 10s)
  * `--burst`: emits M events immediately (default 100)
  * `--replay path/to/file.csv`: replays historical data from static source

* Event fields must align with downstream schema (fact table definitions):

  * Required fields for every event:

    * `event_id`: UUID
    * `event_ts`: ISO-8601 timestamp
    * `event_type`: string

* Values should be randomly generated with reproducibility and data realism:

  * Use seeded randomness: `random.seed(domain + event_type)`
  * Geolocation fields should generate coordinates within logical bounds
  * Categorical values (e.g., product categories, error codes) should follow defined vocabularies
  * Refer to the schema in this document for field requirements

---

## Recommended DAG Types (Airflow)

For each domain, create the following DAG types:

1. **Ingestion DAGs** (`ingest_<event>_<region>.py`):

   * Source: RabbitMQ queue listener
   * Action: Validate schema, write to raw Iceberg table
   * Partitioned by `event_date`
   * Register table in OpenMetadata with owner and domain tags

2. **Staging DAGs** (`stg_<entity>.py`):

   * Source: raw Iceberg tables
   * Action: Apply dbt models to clean/flatten/denormalize data
   * Register lineage in OpenMetadata via dbt ingestion

3. **Fact DAGs** (`fact_<entity>.py`):

   * Source: staging tables
   * Action: Populate or upsert fact tables (incremental by `event_ts`)
   * Tag output dataset as product in OpenMetadata

4. **ML DAGs** (`forecast_<target>.py`):

   * Source: fact tables
   * Action: Trigger ML model inference and write to `fact_` table
   * Output is discoverable in metadata catalog

---

## Data Model Specification (for Generator & DAG Alignment)

### Fact Tables (Target Iceberg Tables)

| Table                      | Key Fields                                                  |
| -------------------------- | ----------------------------------------------------------- |
| `fact_orders`              | `order_id`, `product_id`, `warehouse_id`, `order_ts`, `qty` |
| `fact_returns`             | `return_id`, `order_id`, `return_ts`, `reason_code`         |
| `fact_inventory_movements` | `movement_id`, `product_id`, `delta_qty`, `source_type`     |
| `fact_vehicle_routes`      | `vehicle_id`, `route_id`, `assigned_ts`, `location`         |
| `fact_dispatch_logs`       | `dispatch_id`, `order_id`, `vehicle_id`, `status`, `eta`    |
| `fact_order_errors`        | `error_id`, `order_id`, `error_code`, `detected_ts`         |
| `fact_forecast_demand`     | `product_id`, `region`, `forecast_ts`, `forecast_qty`       |
| `fact_stockout_risks`      | `product_id`, `risk_score`, `predicted_date`, `confidence`  |

### Dimension Tables

| Table            | Key Fields                                                 |
| ---------------- | ---------------------------------------------------------- |
| `dim_warehouse`  | `warehouse_id`, `region`, `manager`, `capacity`            |
| `dim_product`    | `product_id`, `name`, `category`, `unit_cost`              |
| `dim_vehicle`    | `vehicle_id`, `type`, `capacity`, `current_location`       |
| `dim_employee`   | `employee_id`, `role`, `assigned_warehouse`, `shift_hours` |
| `dim_route`      | `route_id`, `region_covered`, `avg_duration`               |
| `dim_date`       | `date_id`, `day`, `week`, `month`, `quarter`, `year`       |
| `dim_error_code` | `error_code`, `description`, `severity_level`              |

---

## Metadata and Governance Agents

### OpenMetadata

* All Iceberg tables, DAGs, and dbt models are registered via ingestion connectors
* Glossary terms are applied to shared business concepts (e.g., `SLA`, `Restock`, `Forecast`)
* Ownership and domain tags are applied at dataset level
* Lineage is visualized from ingestion (RabbitMQ) → transformation (dbt) → consumption (Superset)
* Sensitive columns (e.g., PII placeholders) are tagged with access levels
* Metadata updates can be triggered via metadata ingestion workflows

---

## Codex Notes

* Each DAG and generator script follows a consistent naming pattern
* All Iceberg tables are partitioned by `event_date` and append-only unless overwritten
* Data lineage and metadata registration are automated where possible
* DuckDB is used as the SQL engine for querying Iceberg during analytics
* ML-generated predictions are treated like any other `fact_` table

---

## Future Considerations

* Expand OpenMetadata glossary to include product lifecycle terms
* Add validation layers to data generators to simulate errors intentionally
* Monitor freshness via metadata ingestion DAGs
* Add SLAs to DAG definitions using Airflow’s SLA config
