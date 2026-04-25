"""
marts.py
Tính toán business metrics từ fact/dim tables.
Chạy cuối pipeline sau khi warehouse đã được load.
"""
from datetime import date
from db import get_conn


def build_daily_revenue(cur, run_date: date):
    cur.execute(
        """
        INSERT INTO mart_daily_revenue
            (report_date, total_orders, completed_orders, cancelled_orders,
             pending_orders, gross_revenue, net_revenue, total_profit,
             cancel_rate, avg_order_value)
        SELECT
            %s                                                     AS report_date,
            COUNT(*)                                               AS total_orders,
            COUNT(*) FILTER (WHERE status = 'completed')           AS completed_orders,
            COUNT(*) FILTER (WHERE status = 'cancelled')           AS cancelled_orders,
            COUNT(*) FILTER (WHERE status = 'pending')             AS pending_orders,
            ROUND(SUM(total_amount), 2)                            AS gross_revenue,
            ROUND(SUM(total_amount) FILTER (WHERE status = 'completed'), 2) AS net_revenue,
            ROUND(SUM(profit) FILTER (WHERE status = 'completed'), 2)       AS total_profit,
            ROUND(
                COUNT(*) FILTER (WHERE status = 'cancelled') * 100.0 / NULLIF(COUNT(*), 0),
                2
            )                                                      AS cancel_rate,
            ROUND(AVG(total_amount) FILTER (WHERE status = 'completed'), 2) AS avg_order_value
        FROM fct_orders
        WHERE date_key = (SELECT date_id FROM dim_date WHERE full_date = %s)
        ON CONFLICT (report_date) DO UPDATE SET
            total_orders      = EXCLUDED.total_orders,
            completed_orders  = EXCLUDED.completed_orders,
            cancelled_orders  = EXCLUDED.cancelled_orders,
            pending_orders    = EXCLUDED.pending_orders,
            gross_revenue     = EXCLUDED.gross_revenue,
            net_revenue       = EXCLUDED.net_revenue,
            total_profit      = EXCLUDED.total_profit,
            cancel_rate       = EXCLUDED.cancel_rate,
            avg_order_value   = EXCLUDED.avg_order_value
        """,
        (run_date, run_date),
    )
    print(f"  [marts] mart_daily_revenue updated for {run_date}")


def build_product_performance(cur, run_date: date):
    cur.execute(
        """
        INSERT INTO mart_product_performance
            (report_date, product_id, product_name, category,
             units_sold, revenue, profit, profit_margin)
        SELECT
            %s,
            dp.product_id,
            dp.name,
            dp.category,
            SUM(f.quantity)                                            AS units_sold,
            ROUND(SUM(f.total_amount), 2)                              AS revenue,
            ROUND(SUM(f.profit), 2)                                    AS profit,
            ROUND(SUM(f.profit) / NULLIF(SUM(f.total_amount), 0) * 100, 2) AS profit_margin
        FROM fct_orders f
        JOIN dim_products dp ON dp.product_key = f.product_key
        JOIN dim_date     dd ON dd.date_id = f.date_key
        WHERE dd.full_date = %s
          AND f.status = 'completed'
        GROUP BY dp.product_id, dp.name, dp.category
        ON CONFLICT (report_date, product_id) DO UPDATE SET
            units_sold    = EXCLUDED.units_sold,
            revenue       = EXCLUDED.revenue,
            profit        = EXCLUDED.profit,
            profit_margin = EXCLUDED.profit_margin
        """,
        (run_date, run_date),
    )
    print(f"  [marts] mart_product_performance updated: {cur.rowcount} rows")


def build_customer_segments(cur, run_date: date):
    cur.execute(
        """
        INSERT INTO mart_customer_segments
            (report_date, city, age_group, total_customers,
             total_orders, avg_order_value, total_revenue)
        SELECT
            %s,
            dc.city,
            dc.age_group,
            COUNT(DISTINCT f.customer_key)          AS total_customers,
            COUNT(f.order_id)                       AS total_orders,
            ROUND(AVG(f.total_amount), 2)           AS avg_order_value,
            ROUND(SUM(f.total_amount), 2)           AS total_revenue
        FROM fct_orders f
        JOIN dim_customers dc ON dc.customer_key = f.customer_key
        JOIN dim_date      dd ON dd.date_id = f.date_key
        WHERE dd.full_date = %s
          AND f.status = 'completed'
        GROUP BY dc.city, dc.age_group
        ON CONFLICT (report_date, city, age_group) DO UPDATE SET
            total_customers = EXCLUDED.total_customers,
            total_orders    = EXCLUDED.total_orders,
            avg_order_value = EXCLUDED.avg_order_value,
            total_revenue   = EXCLUDED.total_revenue
        """,
        (run_date, run_date),
    )
    print(f"  [marts] mart_customer_segments updated: {cur.rowcount} rows")


def run_marts(run_date: date = None):
    run_date = run_date or date.today()
    print(f"[marts] Running for {run_date}")
    conn = get_conn()
    cur = conn.cursor()
    build_daily_revenue(cur, run_date)
    build_product_performance(cur, run_date)
    build_customer_segments(cur, run_date)
    conn.commit()
    cur.close()
    conn.close()
    print("[marts] Done")


if __name__ == "__main__":
    run_marts()
