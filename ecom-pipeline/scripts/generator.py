"""
generator.py
Sinh dữ liệu giả lập mỗi ngày: customers, products, orders
Dùng Faker với locale vi_VN để data trông realistic hơn.
"""
import uuid
import random
from datetime import datetime
from faker import Faker
from db import get_conn

fake = Faker("vi_VN")

CITIES = ["Ho Chi Minh", "Ha Noi", "Da Nang", "Can Tho", "Hue", "Hai Phong", "Bien Hoa", "Vung Tau"]
CATEGORIES = ["Electronics", "Clothing", "Food & Beverage", "Books", "Sports", "Home & Living", "Beauty"]
STATUSES = ["completed", "completed", "completed", "pending", "cancelled"]  # weighted realistic
PRODUCT_PREFIXES = ["Smart", "Pro", "Ultra", "Mini", "Eco", "Classic", "Premium"]


# ─── Seed helpers (chỉ chạy lần đầu) ───────────────────────────────────────

def seed_customers(cur, n=200):
    """Tạo initial customer pool."""
    ids = []
    for _ in range(n):
        cid = str(uuid.uuid4())
        cur.execute(
            """
            INSERT INTO raw_customers (customer_id, full_name, email, city, age, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                cid,
                fake.name(),
                fake.email(),
                random.choice(CITIES),
                random.randint(18, 65),
                datetime.now(),
            ),
        )
        ids.append(cid)
    print(f"  [seed] {n} customers created")
    return ids


def seed_products(cur, n=80):
    """Tạo initial product catalog."""
    products = {}
    for _ in range(n):
        pid = str(uuid.uuid4())
        cost = round(random.uniform(15, 800), 2)
        sell = round(cost * random.uniform(1.3, 2.5), 2)
        cur.execute(
            """
            INSERT INTO raw_products (product_id, name, category, cost_price, sell_price, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                pid,
                f"{random.choice(PRODUCT_PREFIXES)} {fake.word().capitalize()} {fake.word().capitalize()}",
                random.choice(CATEGORIES),
                cost,
                sell,
                datetime.now(),
            ),
        )
        products[pid] = sell
    print(f"  [seed] {n} products created")
    return products


# ─── Daily generation ────────────────────────────────────────────────────────

def generate_daily_data(num_orders: int = 250):
    """
    Sinh ~num_orders đơn hàng cho ngày hôm nay.
    Nếu chưa có customers/products thì seed trước.
    """
    print(f"[generator] Starting for {datetime.now().date()} — target {num_orders} orders")
    conn = get_conn()
    cur = conn.cursor()

    # Lấy existing pool
    cur.execute("SELECT customer_id FROM raw_customers")
    customers = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT product_id, sell_price FROM raw_products")
    product_rows = cur.fetchall()
    products = {r[0]: r[1] for r in product_rows}

    # Seed nếu chưa có
    if not customers:
        customers = seed_customers(cur, 200)
        conn.commit()
    if not products:
        products = seed_products(cur, 80)
        conn.commit()

    # Thêm ~5 customers mới mỗi ngày (growth simulation)
    new_customer_ids = seed_customers(cur, random.randint(3, 8))
    customers.extend(new_customer_ids)

    # Sinh orders
    orders = []
    for _ in range(num_orders):
        product_id = random.choice(list(products.keys()))
        quantity = random.randint(1, 6)
        unit_price = products[product_id]
        # Thêm noise nhỏ vào price (discount/surge)
        unit_price = round(unit_price * random.uniform(0.9, 1.05), 2)

        orders.append(
            (
                str(uuid.uuid4()),
                str(random.choice(customers)),
                str(product_id),
                quantity,
                unit_price,
                random.choice(STATUSES),
                datetime.now(),
            )
        )

    cur.executemany(
        """
        INSERT INTO raw_orders (order_id, customer_id, product_id, quantity, unit_price, status, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        orders,
    )

    conn.commit()
    cur.close()
    conn.close()
    print(f"[generator] Done — {len(orders)} orders inserted")


if __name__ == "__main__":
    generate_daily_data()
