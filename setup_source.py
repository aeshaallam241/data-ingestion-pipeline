import os
import random
import getpass
import psycopg2
from faker import Faker

fake = Faker()
password = os.environ.get("PG_PASSWORD") or getpass.getpass("Postgres password: ")
conn = psycopg2.connect(host="localhost", port=5432, dbname="postgres",
                        user="postgres", password=password)
cur = conn.cursor()

cur.execute("""
DROP TABLE IF EXISTS orders, customers, products;
CREATE TABLE customers (customer_id INT, name TEXT, email TEXT, country TEXT,
                        updated_at TIMESTAMP DEFAULT NOW());
CREATE TABLE products (product_id INT, name TEXT, price NUMERIC(10,2),
                       updated_at TIMESTAMP DEFAULT NOW());
CREATE TABLE orders (order_id INT, customer_id INT, product_id INT, quantity INT,
                     amount NUMERIC(10,2), order_date DATE,
                     updated_at TIMESTAMP DEFAULT NOW());
""")

for i in range(1, 201):
    email = None if i % 25 == 0 else fake.email()
    cur.execute("INSERT INTO customers (customer_id, name, email, country) "
                "VALUES (%s,%s,%s,%s)", (i, fake.name(), email, fake.country()))

cur.execute("INSERT INTO customers (customer_id, name, email, country) "
            "SELECT customer_id, name, email, country FROM customers "
            "WHERE customer_id <= 5")

for i in range(1, 51):
    price = -9.99 if i % 10 == 0 else round(random.uniform(5, 300), 2)
    cur.execute("INSERT INTO products (product_id, name, price) VALUES (%s,%s,%s)",
                (i, fake.word().capitalize(), price))

for i in range(1, 1001):
    customer_id = 9999 if i % 50 == 0 else random.randint(1, 200)
    product_id = None if i % 90 == 0 else random.randint(1, 50)
    quantity = 0 if i % 70 == 0 else random.randint(1, 5)
    amount = -50 if i % 77 == 0 else round(random.uniform(5, 500), 2)
    order_date = "2099-01-01" if i % 111 == 0 else fake.date_between("-1y", "today")
    cur.execute("INSERT INTO orders (order_id, customer_id, product_id, quantity, "
                "amount, order_date) VALUES (%s,%s,%s,%s,%s,%s)",
                (i, customer_id, product_id, quantity, amount, order_date))

conn.commit()

for t in ("customers", "products", "orders"):
    cur.execute(f"SELECT COUNT(*) FROM {t}")
    print(t, cur.fetchone()[0], "rows")

conn.close()