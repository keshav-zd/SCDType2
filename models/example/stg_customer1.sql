{{ config(materialized='view') }}
with source_data as (

    -- select 1 as id
    -- union all
    -- select null as id
    select * from customer
)

select *
from source_data