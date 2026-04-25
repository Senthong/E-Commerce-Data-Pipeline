-- ============================================================
-- LAYER 1: SOURCE (Raw ingestion tables)
-- ============================================================

CREATE TABLE IF NOT EXISTS raw_orders (
    order_id      UUID PRIMARY KEY,
    customer_id   UUID,
    product_id    UUID,
    quantity      INT,
    unit_price    NUMERIC(10,2),
    status        VARCHAR(20),   -- pending / completed / cancelled
    created_at    TIMESTAMP,
    ingested_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_customers (
    customer_id   UUID PRIMARY KEY,
    full_name     VARCHAR(100),
    email         VARCHAR(100),
    city          VARCHAR(50),
    age           INT,
    created_at    TIMESTAMP,
    ingested_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_products (
    product_id    UUID PRIMARY KEY,
    name          VARCHAR(100),
    category      VARCHAR(50),
    cost_price    NUMERIC(10,2),
    sell_price    NUMERIC(10,2),
    created_at    TIMESTAMP,
    ingested_at   TIMESTAMP DEFAULT NOW()
);
