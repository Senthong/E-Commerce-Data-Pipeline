-- ============================================================
-- LAYER 4: MARTS (Business metrics)
-- ============================================================

CREATE TABLE IF NOT EXISTS mart_daily_revenue (
    report_date       DATE PRIMARY KEY,
    total_orders      INT,
    completed_orders  INT,
    cancelled_orders  INT,
    pending_orders    INT,
    gross_revenue     NUMERIC(14,2),
    net_revenue       NUMERIC(14,2),
    total_profit      NUMERIC(14,2),
    cancel_rate       NUMERIC(5,2),
    avg_order_value   NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS mart_product_performance (
    report_date    DATE,
    product_id     UUID,
    product_name   VARCHAR(100),
    category       VARCHAR(50),
    units_sold     INT,
    revenue        NUMERIC(14,2),
    profit         NUMERIC(14,2),
    profit_margin  NUMERIC(5,2),
    PRIMARY KEY (report_date, product_id)
);

CREATE TABLE IF NOT EXISTS mart_customer_segments (
    report_date      DATE,
    city             VARCHAR(50),
    age_group        VARCHAR(20),
    total_customers  INT,
    total_orders     INT,
    avg_order_value  NUMERIC(10,2),
    total_revenue    NUMERIC(14,2),
    PRIMARY KEY (report_date, city, age_group)
);
