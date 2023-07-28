{{ config(materialized="table") }}

-- Use WITH before CREATE TABLE to define CTEs
WITH customer_data AS (
  SELECT
    id,
    name,
    address
  FROM customer
),
order_data AS (
  SELECT
    orderid,
    orderItem,
    orderDate
  FROM orders
)
-- Use CREATE TABLE AS without a semicolon
CREATE TABLE customer_orders AS
SELECT
  cd.id,
  cd.name,
  cd.address,
  od.orderid,
  od.orderItem,
  od.orderDate
FROM customer_data cd
LEFT JOIN order_data od
ON cd.id = od.orderid
