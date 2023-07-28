select * from {{ source('postgres_data','customer') }}
