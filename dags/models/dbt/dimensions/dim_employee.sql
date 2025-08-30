{{ config(materialized='table') }}

-- Skeleton employee dimension managed by dbt; no upstream events yet.
select
  cast(null as varchar) as employee_id,
  cast(null as varchar) as role,
  cast(null as varchar) as assigned_warehouse,
  cast(null as integer) as shift_hours,
  cast(null as date) as event_date
where false
