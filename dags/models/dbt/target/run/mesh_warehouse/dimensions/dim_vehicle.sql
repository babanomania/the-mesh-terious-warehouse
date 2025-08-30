
  
    
    

    create  table
      "warehouse"."main"."dim_vehicle"
  
    as (
      

with src as (
  select
    vehicle_id,
    cast(event_ts as date) as event_date
  from iceberg_scan('s3://warehouse/dispatch_logs/raw_dispatch_logs', allow_moved_paths = true)
  where vehicle_id is not null and vehicle_id <> ''
)

select
  vehicle_id,
  cast(null as varchar) as type,
  cast(null as integer) as capacity,
  cast(null as varchar) as current_location,
  event_date
from src
    );
  
  
  