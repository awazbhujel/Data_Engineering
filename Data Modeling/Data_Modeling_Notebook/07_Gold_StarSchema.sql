-- Databricks notebook source
create schema data_modeling_demo.gold_star;

-- COMMAND ----------

CREATE OR REPLACE TABLE data_modeling_demo.gold_star.fact_sales AS
SELECT 
  o.order_id,
  o.customer_id,
  o.order_date,
  o.order_status,
  o.payment_id,
  o.order_total,
  oi.order_item_id,
  oi.product_id,
  oi.quantity,
  oi.unit_price,
  oi.discount_amount,
  oi.line_total
FROM data_modeling_demo.silver.orders o
JOIN data_modeling_demo.silver.order_items oi
ON o.order_id = oi.order_id

-- COMMAND ----------

select * from data_modeling_demo.gold_star.fact_sales

-- COMMAND ----------

CREATE OR REPLACE TABLE data_modeling_demo.gold_star.dim_customer AS
SELECT
  cu.customer_id,
  cu.customer_name,
  cu.email,
  cu.phone,
  cu.registration_date,
  c.city_name,
  s.state_name,
  co.country_name
FROM data_modeling_demo.silver.customers cu
JOIN data_modeling_demo.silver.cities c
  ON cu.city_id = c.city_id
JOIN data_modeling_demo.silver.states s
  ON c.state_id = s.state_id
JOIN data_modeling_demo.silver.countries co
  ON s.country_id = co.country_id

-- COMMAND ----------

select * from data_modeling_demo.gold_star.dim_customer;

-- COMMAND ----------

-- DBTITLE 1,Create dim_payment
CREATE OR REPLACE VIEW data_modeling_demo.gold_star.dim_payment AS
SELECT * FROM data_modeling_demo.silver.payments

-- COMMAND ----------

-- DBTITLE 1,Create dim_date
CREATE OR REPLACE TABLE data_modeling_demo.gold_star.dim_date AS
SELECT 
  date_value AS date,
  YEAR(date_value) AS year,
  MONTH(date_value) AS month,
  DATE_FORMAT(date_value, 'MMMM') AS month_name,
  DATE_FORMAT(date_value, 'MMM') AS month_short,
  QUARTER(date_value) AS quarter,
  DAYOFWEEK(date_value) AS day_of_week,
  DATE_FORMAT(date_value, 'EEEE') AS day_of_week_name,
  DATE_FORMAT(date_value, 'EEE') AS day_of_week_short,
  DAYOFMONTH(date_value) AS day_of_month,
  DAYOFYEAR(date_value) AS day_of_year,
  WEEKOFYEAR(date_value) AS week_of_year,
  -- Fiscal year (assuming fiscal year starts in January; adjust offset if different)
  YEAR(date_value) AS fiscal_year,
  QUARTER(date_value) AS fiscal_quarter,
  -- Additional useful flags
  CASE WHEN DAYOFWEEK(date_value) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
  CASE WHEN DAYOFWEEK(date_value) NOT IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekday,
  -- First and last day flags
  CASE WHEN DAYOFMONTH(date_value) = 1 THEN TRUE ELSE FALSE END AS is_first_day_of_month,
  CASE WHEN DAYOFMONTH(date_value) = DAYOFMONTH(LAST_DAY(date_value)) THEN TRUE ELSE FALSE END AS is_last_day_of_month,
  -- Year-Month for easy grouping
  DATE_FORMAT(date_value, 'yyyy-MM') AS year_month
FROM (
  SELECT EXPLODE(SEQUENCE(TO_DATE('2024-01-01'), TO_DATE('2026-12-31'), INTERVAL 1 DAY)) AS date_value
);

-- COMMAND ----------

SELECT * FROM data_modeling_demo.gold_star.dim_date
ORDER BY date
LIMIT 10

-- COMMAND ----------

-- DBTITLE 1,Create dim_product
CREATE OR REPLACE TABLE data_modeling_demo.gold_star.dim_product AS
SELECT
  p.product_id,
  p.product_name,
  p.unit_price,
  p.is_active,
  b.brand_name,
  b.brand_description,
  sc.subcategory_name,
  c.category_name,
  c.category_description
FROM data_modeling_demo.silver.products p
JOIN data_modeling_demo.silver.brands b
  ON p.brand_id = b.brand_id
JOIN data_modeling_demo.silver.subcategories sc
  ON p.subcategory_id = sc.subcategory_id
JOIN data_modeling_demo.silver.categories c
  ON sc.category_id = c.category_id

-- COMMAND ----------

-- DBTITLE 1,Preview dim_product
SELECT * FROM data_modeling_demo.gold_star.dim_product
LIMIT 10

-- COMMAND ----------

-- DBTITLE 1,Sales by Year and Category
SELECT 
  d.year,
  p.category_name,
  COUNT(DISTINCT fs.order_id) AS total_orders,
  COUNT(fs.order_item_id) AS total_items_sold,
  SUM(fs.quantity) AS total_quantity,
  SUM(fs.line_total) AS total_sales,
  SUM(fs.discount_amount) AS total_discounts,
  AVG(fs.line_total) AS avg_line_total,
  MIN(fs.order_date) AS first_sale_date,
  MAX(fs.order_date) AS last_sale_date
FROM data_modeling_demo.gold_star.fact_sales fs
JOIN data_modeling_demo.gold_star.dim_product p
  ON fs.product_id = p.product_id
JOIN data_modeling_demo.gold_star.dim_date d
  ON fs.order_date = d.date
GROUP BY d.year, p.category_name
ORDER BY d.year, total_sales DESC