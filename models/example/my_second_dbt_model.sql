-- Use the `ref` function to select from other models

{{ config(materialized='view') }}

with source_data as (

    -- select 1 as id
    -- union all
    -- select null as id
    select * from{{ref('my_first_dbt_model')}}
)

select *
from source_data
