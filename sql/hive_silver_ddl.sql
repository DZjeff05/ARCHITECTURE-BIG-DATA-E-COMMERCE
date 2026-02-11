-- =========================================================
-- SILVER (EXTERNAL TABLES) - Hive
-- Stockage: HDFS Parquet /data/silver/<table>
-- Partitions: ingest_year / ingest_month / ingest_day
-- =========================================================

CREATE DATABASE IF NOT EXISTS ecommerce_silver;
USE ecommerce_silver;

-- -------------------------
-- DIM_CUSTOMERS
-- -------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS dim_customers (
  customer_id INT,
  gender STRING,
  country STRING,
  signup_date DATE
)
PARTITIONED BY (
  ingest_year INT,
  ingest_month INT,
  ingest_day INT
)
STORED AS PARQUET
LOCATION '/data/silver/dim_customers';

-- -------------------------
-- DIM_PRODUCTS
-- -------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS dim_products (
  product_id INT,
  product_name STRING,
  category STRING,
  brand STRING,
  price DOUBLE,
  stock_quantity INT
)
PARTITIONED BY (
  ingest_year INT,
  ingest_month INT,
  ingest_day INT
)
STORED AS PARQUET
LOCATION '/data/silver/dim_products';

-- -------------------------
-- DIM_DATE  (IMPORTANT: date_key)
-- -------------------------
DROP TABLE IF EXISTS dim_date;

CREATE EXTERNAL TABLE dim_date (
  date_key DATE,
  year INT,
  month INT,
  day INT,
  week INT,
  quarter INT
)
PARTITIONED BY (
  ingest_year INT,
  ingest_month INT,
  ingest_day INT
)
STORED AS PARQUET
LOCATION '/data/silver/dim_date';

-- -------------------------
-- FACT_ORDERS
-- -------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS fact_orders (
  order_id INT,
  customer_id INT,
  order_date DATE,
  total_amount DOUBLE,
  payment_method STRING,
  shipping_country STRING
)
PARTITIONED BY (
  ingest_year INT,
  ingest_month INT,
  ingest_day INT
)
STORED AS PARQUET
LOCATION '/data/silver/fact_orders';

-- -------------------------
-- FACT_ORDER_ITEMS
-- -------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS fact_order_items (
  order_item_id INT,
  order_id INT,
  product_id INT,
  quantity INT,
  unit_price DOUBLE,
  order_date DATE,
  line_amount DOUBLE
)
PARTITIONED BY (
  ingest_year INT,
  ingest_month INT,
  ingest_day INT
)
STORED AS PARQUET
LOCATION '/data/silver/fact_order_items';

-- -------------------------
-- FACT_REVIEWS
-- -------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS fact_reviews (
  review_id INT,
  product_id INT,
  customer_id INT,
  rating INT,
  review_date DATE
)
PARTITIONED BY (
  ingest_year INT,
  ingest_month INT,
  ingest_day INT
)
STORED AS PARQUET
LOCATION '/data/silver/fact_reviews';

-- =========================================================
-- Charger les partitions automatiquement
-- =========================================================
MSCK REPAIR TABLE dim_customers;
MSCK REPAIR TABLE dim_products;
MSCK REPAIR TABLE dim_date;
MSCK REPAIR TABLE fact_orders;
MSCK REPAIR TABLE fact_order_items;
MSCK REPAIR TABLE fact_reviews;
