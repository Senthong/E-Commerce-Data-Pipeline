"""
transform.py
Load validated data từ staging vào Data Warehouse (star schema).
- dim_customers: SCD Type 2 (track changes)
- dim_products: upsert
- fct_orders: insert only valid orders
"""
from datetime import date
from db import get_conn


def load_dim_customers(cur, run_date: date):
    """
    SCD Type 2: nếu customer thay đổi thông tin,
    close record cũ và insert record mới.
    """
    # Insert new customers (chưa có trong dim)
    cur.execute(
        """
        INSERT INTO dim_customers
            (customer_id, full_name, email, city, age_group, valid_from, valid_to, is_current)
        SELECT
            s.customer_id,
            s.full_name,
            s.email,
            s.city,
            s.age_group,
            %s,
            '9999-12-31'::date,
            TRUE
        FROM stg_customers s
        WHERE s.is_valid = TRUE
          AND NOT EXISTS (
              SELECT 1 FROM dim_customers d
              WHERE d.customer_id = s.customer_id AND d.is_current = TRUE
          )
        """,
        (run_date,),
    )
    print(f"  [transform] dim_customers new: {cur.rowcount} rows")


def load_dim_products(cur):
    """Simple upsert — products không SCD."""
    cur.execute(
        """
        INSERT INTO dim_products
            (product_id, name, category, cost_price, sell_price, margin_pct)
        SELECT
            product_id, name, category, cost_price, sell_price, margin_pct
        FROM stg_products
        WHERE is_valid = TRUE
        ON CONFLICT (product_id) DO UPDATE SET
            name       = EXCLUDED.name,
            category   = EXCLUDED.category,
            cost_price = EXCLUDED.cost_price,
            sell_price = EXCLUDED.sell_price,
            margin_pct = EXCLUDED.margin_pct
        """
    )
    print(f"  [transform] dim_products upserted: {cur.rowcount} rows")


def load_fct_orders(cur, run_date: date):
    """Load ONLY valid completed/pending orders vào fact table."""
    cur.execute(
        """
        INSERT INTO fct_orders
            (order_id, customer_key, product_key, date_key,
             quantity, unit_price, total_amount, cost_amount, profit, status)
        SELECT
            o.order_id,
            dc.customer_key,
            dp.product_key,
            dd.date_id,
            o.quantity,
            o.unit_price,
            o.total_amount,
            ROUND(dp.cost_price * o.quantity, 2)                          AS cost_amount,
            ROUND(o.total_amount - dp.cost_price * o.quantity, 2)         AS profit,
            o.status
        FROM stg_orders o
        JOIN dim_customers dc ON dc.customer_id = o.customer_id AND dc.is_current = TRUE
        JOIN dim_products  dp ON dp.product_id  = o.product_id
        JOIN dim_date      dd ON dd.full_date    = o.order_date
        WHERE o.is_valid = TRUE
          AND o.order_date = %s
        ON CONFLICT (order_id) DO NOTHING
        """,
        (run_date,),
    )
    print(f"  [transform] fct_orders inserted: {cur.rowcount} rows")


def run_transform(run_date: date = None):
    run_date = run_date or date.today()
    print(f"[transform] Running for {run_date}")
    conn = get_conn()
    cur = conn.cursor()
    load_dim_customers(cur, run_date)
    load_dim_products(cur)
    load_fct_orders(cur, run_date)
    conn.commit()
    cur.close()
    conn.close()
    print("[transform] Done")


if __name__ == "__main__":
    run_transform()
