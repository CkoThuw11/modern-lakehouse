"""Simulates shop activity on the BikeStores source DB so CDC keeps flowing.

Every tick runs one weighted random action in its own transaction, only on the four
tables Debezium captures: customers, orders, order_items, stocks.
"""

import logging
import os
import random
import time
from datetime import date, timedelta

import psycopg
from faker import Faker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("generator")

INTERVAL = float(os.environ.get("GENERATOR_INTERVAL_SECONDS", "5"))
SEED = os.environ.get("GENERATOR_SEED")
DISCOUNTS = [0, 0.05, 0.07, 0.1, 0.2]  # the values used in the BikeStores data

fake = Faker("en_US")
if SEED:  # same seed + same DB state -> same sequence of actions
    random.seed(SEED)
    Faker.seed(SEED)


def ids(conn, sql, params=()):
    return [row[0] for row in conn.execute(sql, params).fetchall()]


def load_reference(conn):
    stores = ids(conn, "SELECT store_id FROM sales.stores ORDER BY 1")
    staff = {
        s: ids(conn, "SELECT staff_id FROM sales.staffs WHERE store_id = %s AND active = 1 ORDER BY 1", (s,))
        for s in stores
    }
    products = dict(conn.execute("SELECT product_id, list_price FROM production.products ORDER BY 1").fetchall())
    return stores, staff, products


def place_order(conn, ref):
    stores, staff, products = ref
    store = random.choice(stores)
    # Only products this store has in stock, so an order never oversells.
    in_stock = dict(conn.execute(
        "SELECT product_id, quantity FROM production.stocks WHERE store_id = %s AND quantity > 0 ORDER BY 1",
        (store,),
    ).fetchall())
    if not in_stock:
        return None
    customer = random.choice(ids(conn, "SELECT customer_id FROM sales.customers ORDER BY 1"))
    today = date.today()
    order_id = conn.execute(
        """INSERT INTO sales.orders (customer_id, order_status, order_date, required_date, store_id, staff_id)
           VALUES (%s, 1, %s, %s, %s, %s) RETURNING order_id""",
        (customer, today, today + timedelta(days=random.randint(2, 5)), store, random.choice(staff[store])),
    ).fetchone()[0]
    items = random.sample(list(in_stock), k=min(random.randint(1, 3), len(in_stock)))
    for item_id, product in enumerate(items, start=1):
        quantity = random.randint(1, min(2, in_stock[product]))  # never more than available
        conn.execute(
            """INSERT INTO sales.order_items (order_id, item_id, product_id, quantity, list_price, discount)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (order_id, item_id, product, quantity, products[product], random.choice(DISCOUNTS)),
        )
        conn.execute(
            "UPDATE production.stocks SET quantity = quantity - %s WHERE store_id = %s AND product_id = %s",
            (quantity, store, product),
        )
    return f"new order {order_id} ({len(items)} items) for customer {customer}"


def advance_order(conn, ref):
    open_orders = conn.execute(
        "SELECT order_id, order_status FROM sales.orders WHERE order_status IN (1, 2) ORDER BY 1"
    ).fetchall()
    if not open_orders:
        return None
    order_id, status = random.choice(open_orders)
    if status == 1:
        conn.execute("UPDATE sales.orders SET order_status = 2 WHERE order_id = %s", (order_id,))
        return f"order {order_id}: pending -> processing"
    if random.random() < 0.9:
        conn.execute(
            "UPDATE sales.orders SET order_status = 4, shipped_date = %s WHERE order_id = %s", (date.today(), order_id)
        )
        return f"order {order_id}: processing -> completed"
    conn.execute("UPDATE sales.orders SET order_status = 3 WHERE order_id = %s", (order_id,))
    return f"order {order_id}: processing -> rejected"


def new_customer(conn, ref):
    first, last = fake.first_name(), fake.last_name()
    customer_id = conn.execute(
        """INSERT INTO sales.customers (first_name, last_name, phone, email, street, city, state, zip_code)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING customer_id""",
        (
            first, last, fake.numerify("(###) ###-####"),
            f"{first}.{last}{random.randint(1, 999)}@example.com".lower(),
            fake.street_address(), fake.city(), fake.state_abbr(), fake.zipcode(),
        ),
    ).fetchone()[0]
    return f"new customer {customer_id}: {first} {last}"


def move_customer(conn, ref):
    customer = random.choice(ids(conn, "SELECT customer_id FROM sales.customers ORDER BY 1"))
    city, state = fake.city(), fake.state_abbr()
    conn.execute(
        "UPDATE sales.customers SET street = %s, city = %s, state = %s, zip_code = %s WHERE customer_id = %s",
        (fake.street_address(), city, state, fake.zipcode(), customer),
    )
    return f"customer {customer} moved to {city}, {state}"


def restock(conn, ref):
    store, product = random.choice(
        conn.execute("SELECT store_id, product_id FROM production.stocks ORDER BY 1, 2").fetchall()
    )
    added = random.randint(5, 20)
    conn.execute(
        "UPDATE production.stocks SET quantity = quantity + %s WHERE store_id = %s AND product_id = %s",
        (added, store, product),
    )
    return f"restock store {store} product {product}: +{added}"


ACTIONS = [
    (place_order, 45),
    (advance_order, 30),
    (new_customer, 10),
    (move_customer, 10),
    (restock, 5),
]


def main():
    conn = psycopg.connect(
        host="postgres",  # service name + container port inside the Docker network
        port=5432,
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        autocommit=True,
    )
    ref = load_reference(conn)
    actions, weights = zip(*ACTIONS)
    log.info("generator started: one action every %ss", INTERVAL)
    while True:
        action = random.choices(actions, weights)[0]
        with conn.transaction():  # all statements of one action commit together
            result = action(conn, ref)
        if result:
            log.info(result)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
