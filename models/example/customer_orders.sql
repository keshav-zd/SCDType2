{{ config(materialized="table") }}

-- Use WITH before CREATE TABLE to define CTEs
with 
    customer_data as (select id, name, address from customer),
    order_data as (select orderid, orderitem, orderdate from orders)
-- Use CREATE TABLE after the CTEs are defined
select cd.id, cd.name, cd.address, od.orderid, od.orderitem, od.orderdate
from customer_data cd
left join order_data od on cd.id = od.orderid
