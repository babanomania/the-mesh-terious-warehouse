# TODO.md

This document tracks development milestones for the **Mesh-terious Warehouse** project, organized by functional area.

---

## 🔧 Infrastructure Setup

* [ ] Create `docker-compose.yml` for full stack:

  * [ ] MinIO (Iceberg storage backend)
  * [ ] Airflow (DAG orchestration)
  * [ ] OpenMetadata (metadata layer)
  * [ ] Superset (BI dashboarding)
  * [ ] RabbitMQ (ingestion messaging)
  * [ ] DuckDB (analytics engine)
* [ ] Create `.env` file for all services
* [ ] Provision shared network and volumes for persistence

---

## 📦 Ingestion Layer (Mock Producers)

* [x] Implement base generator class with reproducible seeded randomness
* [ ] For each warehouse (`north`, `south`, `east`, `west`):

  * Orders generators:
    * [x] `produce_orders_north.py`
    * [x] `produce_orders_south.py`
    * [x] `produce_orders_east.py`
    * [x] `produce_orders_west.py`
  * [x] `produce_returns_<region>.py`
  * [ ] `produce_inventory_<region>.py`
  * [ ] `produce_restocks_<region>.py`
  * [ ] `produce_dispatch_logs_<region>.py`
  * [ ] `produce_errors_<region>.py`
* [ ] For `logistics_core` domain:

  * [ ] `produce_vehicle_tracking.py`
  * [ ] `produce_route_updates.py`
  * [ ] `produce_delivery_sla.py`
* [ ] For `ml_insights` domain:

  * [ ] `produce_demand_forecast.py`
  * [ ] `produce_sla_risk.py`
  * [ ] `produce_stockout_risk.py`

---

## 🗂️ Data Modeling

* [ ] Create Iceberg-compatible schemas for all `fact_` and `dim_` tables (match `AGENTS.md`)
* [ ] Partitioning strategy: use `event_date` for all `fact_` tables
* [ ] Create Iceberg DDLs for DuckDB queries
* [ ] Create YAML specs for dbt models

---

## 🔄 DAG Development (Airflow)

* [ ] Create DAG bootstrap template (base class)
* [ ] DAGs per domain:

  * Ingestion DAGs:
    * [x] `ingest_orders_north.py` → from RabbitMQ to Iceberg
    * [x] `ingest_orders_south.py` → from RabbitMQ to Iceberg
    * [ ] `ingest_<event>_<region>.py` → from RabbitMQ to Iceberg
  * [ ] `stg_<entity>.py` → transform raw to staging (via dbt)
  * [ ] `fact_<entity>.py` → load final fact tables
* [ ] ML DAGs:

  * [ ] `forecast_demand.py`
  * [ ] `predict_stockout_risk.py`
* [ ] Register DAGs with SLAs + OpenMetadata integration

---

## 🧠 ML Workflow Integration

* [ ] Create Jupyter notebook for demand forecasting model (e.g., XGBoost, Prophet)
* [ ] Create Airflow task to run inference notebook or Python script
* [ ] Write outputs to `fact_stockout_risks`
* [ ] Validate predictions using Iceberg → DuckDB → Superset

---

## 📊 Superset Integration

* [ ] Add Superset service to Docker Compose
* [ ] Connect Superset to DuckDB SQL Engine
* [ ] Create starter dashboard for:

  * [ ] `fact_orders`, `fact_returns`
  * [ ] SLA heatmap from `fact_dispatch_logs`
  * [ ] Forecast risk summary from `fact_stockout_risks`

---

## 📚 Metadata and Governance (OpenMetadata)

* [ ] Bootstrap OpenMetadata with default glossary
* [ ] Ingest metadata from:

  * [ ] Iceberg tables
  * [ ] dbt models
  * [ ] Airflow DAGs
* [ ] Apply:

  * [ ] Ownership metadata
  * [ ] Domain tags
  * [ ] Glossary terms
  * [ ] Sensitivity levels (e.g., PII flags)
* [ ] Enable lineage from ingestion → DAG → dbt → Iceberg → Superset

---

## 🧪 Testing & Validation

* [ ] Add CI test for:

  * [ ] Data generator schema match
  * [ ] DAG syntax and dry-run
  * [ ] Iceberg schema compliance
* [ ] Add logging to all generator and DAG processes
* [ ] Implement unit test suite for DAG logic and data contracts

---

## 🧩 Optional Extensions

* [ ] Add Kafka-to-RabbitMQ bridge for Kafka simulation
* [ ] Add real-time anomaly detector for dispatch failures
* [ ] Add API gateway for mock orders or live interaction
* [ ] Add Airflow SLA monitoring DAG
* [ ] Support push to external Lakehouse formats (e.g., Delta, Hudi)
