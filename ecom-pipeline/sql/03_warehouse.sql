-- ============================================================
-- LAYER 3: DATA WAREHOUSE (Star Schema)
-- ============================================================

-- Dimension: Date
CREATE TABLE IF NOT EXISTS dim_date (
    date_id      SERIAL PRIMARY KEY,
    full_date    DATE UNIQUE,
    day_of_week  INT,
    day_name     VARCHAR(10),
    week_number  INT,
    month        INT,
    month_name   VARCHAR(10),
    quarter      INT,
    year         INT,
    is_weekend   BOOLEAN
);

-- Pre-populate dim_date for 3 years
INSERT INTO dim_date (full_date, day_of_week, day_name, week_number, month, month_name, quarter, year, is_weekend)
SELECT
    d::date,
    EXTRACT(DOW FROM d)::INT,
    TO_CHAR(d, 'Day'),
    EXTRACT(WEEK FROM d)::INT,
    EXTRACT(MONTH FROM d)::INT,
    TO_CHAR(d, 'Month'),
    EXTRACT(QUARTER FROM d)::INT,
    EXTRACT(YEAR FROM d)::INT,
    EXTRACT(DOW FROM d) IN (0, 6)
FROM generate_series('2024-01-01'::date, '2026-12-31'::date, '1 day'::interval) d
ON CONFLICT (full_date) DO NOTHING;

-- Dimension: Customers (SCD Type 2)
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_key  SERIAL PRIMARY KEY,
    customer_id   UUID,
    full_name     VARCHAR(100),
    email         VARCHAR(100),
    city          VARCHAR(50),
    age_group     VARCHAR(20),
    valid_from    DATE,
    valid_to      DATE,
    is_current    BOOLEAN DEFAULT TRUE
);

-- Dimension: Products
CREATE TABLE IF NOT EXISTS dim_products (
    product_key   SERIAL PRIMARY KEY,
    product_id    UUID UNIQUE,
    name          VARCHAR(100),
    category      VARCHAR(50),
    cost_price    NUMERIC(10,2),
    sell_price    NUMERIC(10,2),
    margin_pct    NUMERIC(5,2)
);

-- Fact: Orders
CREATE TABLE IF NOT EXISTS fct_orders (
    order_key      SERIAL PRIMARY KEY,
    order_id       UUID UNIQUE,
    customer_key   INT REFERENCES dim_customers(customer_key),
    product_key    INT REFERENCES dim_products(product_key),
    date_key       INT REFERENCES dim_date(date_id),
    quantity       INT,
    unit_price     NUMERIC(10,2),
    total_amount   NUMERIC(10,2),
    cost_amount    NUMERIC(10,2),
    profit         NUMERIC(10,2),
    status         VARCHAR(20)
);
