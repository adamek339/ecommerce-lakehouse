import random
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from load_repo_env import bootstrap, require_env

bootstrap()

import pyodbc
from faker import Faker

fake = Faker()

host = require_env("MSSQL_HOST")
port = require_env("MSSQL_PORT")
database = require_env("MSSQL_DATABASE")
user = require_env("MSSQL_USER")
password = require_env("MSSQL_PASSWORD")

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={host},{port};"
    f"DATABASE={database};"
    f"UID={user};"
    f"PWD={password}"
)

cursor = conn.cursor()

cursor.execute("DELETE FROM dbo.orders")
cursor.execute("DELETE FROM dbo.products")
cursor.execute("DELETE FROM dbo.customers")

# Generuj 100 klientów
for i in range(100):
    cursor.execute(
        """
        INSERT INTO customers (first_name, last_name, email, country)
        VALUES (?, ?, ?, ?)
    """,
        fake.first_name(),
        fake.last_name(),
        fake.email(),
        fake.country(),
    )

# Generuj 50 produktów
categories = ["Electronics", "Clothing", "Books", "Sports", "Home"]

for i in range(50):
    cursor.execute(
        """
        INSERT INTO products (name, category, price)
        VALUES (?, ?, ?)
    """,
        fake.word().capitalize() + " " + fake.word().capitalize(),
        random.choice(categories),
        round(random.uniform(5.0, 500.0), 2),
    )

conn.commit()

# Pobierz ID klientów i produktów żeby zrobić zamówienia
cursor.execute("SELECT customer_id FROM customers")
customer_ids = [row[0] for row in cursor.fetchall()]

cursor.execute("SELECT product_id, price FROM products")
products = [(row[0], row[1]) for row in cursor.fetchall()]

# Generuj 500 zamówień
statuses = ["completed", "pending", "cancelled", "refunded"]

for i in range(500):
    customer_id = random.choice(customer_ids)
    product_id, price = random.choice(products)
    quantity = random.randint(1, 5)
    total = round(price * quantity, 2)

    cursor.execute(
        """
        INSERT INTO orders (customer_id, product_id, quantity, total_amount, status, order_date)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        customer_id,
        product_id,
        quantity,
        total,
        random.choice(statuses),
        fake.date_time_between(start_date="-1y", end_date="now"),
    )

conn.commit()
print("Gotowe! Wstawiono 100 klientów, 50 produktów, 500 zamówień.")
