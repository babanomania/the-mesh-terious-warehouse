{{ config(materialized='table') }}

-- Skeleton route dimension managed by dbt; no upstream events yet.
select
  cast(null as varchar) as route_id,
  cast(null as varchar) as region_covered,
  cast(null as integer) as avg_duration,
  cast(null as date) as event_date
where false
