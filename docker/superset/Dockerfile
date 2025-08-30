FROM apache/superset:latest

# Install DuckDB SQLAlchemy dialect and client at build time
USER root
RUN pip install --no-cache-dir duckdb duckdb-engine

# Create superset_home and set sensible permissions; actual volume will
# be chowned by an init container at runtime to UID 50000
RUN mkdir -p /app/superset_home && chmod 775 /app/superset_home

# Default back to non-root; runtime UID set via compose
USER 1000

