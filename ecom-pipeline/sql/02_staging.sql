-- ============================================================
-- LAYER 2: STAGING (Validated + cleaned)
-- ============================================================

CREATE TABLE IF NOT EXISTS stg_orders (
    order_id       UUID PRIMARY KEY,
    customer_id    UUID,
    product_id     UUID,
    quantity       INT,
    unit_price     NUMERIC(10,2),
    total_amount   NUMERIC(10,2),
    status         VARCHAR(20),
    order_date     DATE,
    is_valid       BOOLEAN,
    invalid_reason VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS stg_customers (
    customer_id  UUID PRIMARY KEY,
    full_name    VARCHAR(100),
    email        VARCHAR(100),
    city         VARCHAR(50),
    age          INT,
    age_group    VARCHAR(20),
    is_valid     BOOLEAN,
    invalid_reason VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS stg_products (
    product_id   UUID PRIMARY KEY,
    name         VARCHAR(100),
    category     VARCHAR(50),
    cost_price   NUMERIC(10,2),
    sell_price   NUMERIC(10,2),
    margin_pct   NUMERIC(5,2),
    is_valid     BOOLEAN,
    invalid_reason VARCHAR(200)
);
