{{ config(materialized='view') }}

select * from {{ source('dimensions', 'dim_product') }}
