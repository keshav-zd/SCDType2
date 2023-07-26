{{ config(materialized="table", unlogged=True) }}

select *
from zecdata
limit 5
;
