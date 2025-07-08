import psycopg2
from crate import client as crate_client
import time

# --- Configuration for NEW instances ---
PG_HOST = "localhost"
PG_PORT = 5436  # New PostgreSQL Port
PG_DBNAME = "postgres"
PG_USER = "postgres"
PG_PASSWORD = "your_new_strong_password_v2" # !! IMPORTANT: Use the password set above

CRATE_HOST = "localhost"
CRATE_PORT = 4203 # New CrateDB Port

# --- Database Schema Definitions ---
# Note: CrateDB does not enforce FOREIGN KEY constraints, they are for documentation.
# CrateDB also uses 'STRING' instead of 'VARCHAR' and 'FLOAT' instead of 'NUMERIC'.
# And we add a FULLTEXT index to product description for CrateDB.
tables_schema = [
    """
    CREATE TABLE IF NOT EXISTS customers (
        customer_id INTEGER PRIMARY KEY,
        name VARCHAR(100),
        email VARCHAR(100),
        registration_date TIMESTAMP,
        status VARCHAR(20)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS products (
        product_id INTEGER PRIMARY KEY,
        name VARCHAR(255),
        description TEXT,
        price NUMERIC(10, 2),
        category VARCHAR(100)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS orders (
        order_id INTEGER PRIMARY KEY,
        customer_id INTEGER,
        order_date TIMESTAMP,
        total_amount NUMERIC(10, 2),
        status VARCHAR(50)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS order_items (
        item_id INTEGER PRIMARY KEY,
        order_id INTEGER,
        product_id INTEGER,
        quantity INTEGER,
        unit_price NUMERIC(10, 2)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS inventory (
        inventory_id INTEGER PRIMARY KEY,
        product_id INTEGER,
        quantity INTEGER,
        warehouse VARCHAR(100),
        last_updated TIMESTAMP
    )
    """
]

# CrateDB specific schema adjustments (including FULLTEXT index and additional regular indexes)
crate_tables_schema = [
    """
    CREATE TABLE IF NOT EXISTS customers (
        customer_id INTEGER PRIMARY KEY,
        name STRING,
        email STRING,
        registration_date TIMESTAMP,
        status STRING,
        INDEX status_idx USING BTREE (status)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS products (
        product_id INTEGER PRIMARY KEY,
        name STRING,
        description TEXT INDEX USING FULLTEXT WITH (analyzer = 'standard'),
        price FLOAT,
        category STRING,
        INDEX category_idx USING BTREE (category)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS orders (
        order_id INTEGER PRIMARY KEY,
        customer_id INTEGER,
        order_date TIMESTAMP,
        total_amount FLOAT,
        status STRING
        -- Removed BTREE indexes on INTEGER and TIMESTAMP columns as they are not supported
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS order_items (
        item_id INTEGER PRIMARY KEY,
        order_id INTEGER,
        product_id INTEGER,
        quantity INTEGER,
        unit_price FLOAT
        -- Removed BTREE indexes on INTEGER columns as they are not supported
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS inventory (
        inventory_id INTEGER PRIMARY KEY,
        product_id INTEGER,
        quantity INTEGER,
        warehouse STRING,
        last_updated TIMESTAMP
    )
    """
]


# --- Connect to PostgreSQL ---
try:
    pg_conn = psycopg2.connect(f"host={PG_HOST} port={PG_PORT} dbname={PG_DBNAME} user={PG_USER} password={PG_PASSWORD}")
    pg_cursor = pg_conn.cursor()
    print("Connected to PostgreSQL successfully!")
except Exception as e:
    print(f"Error connecting to PostgreSQL: {e}")
    exit()

# --- Connect to CrateDB ---
try:
    crate_conn = crate_client.connect(f"{CRATE_HOST}:{CRATE_PORT}")
    crate_cursor = crate_conn.cursor()
    print("Connected to CrateDB successfully!")
except Exception as e:
    print(f"Error connecting to CrateDB: {e}")
    exit()

# --- Create tables in PostgreSQL ---
print("\nCreating tables in PostgreSQL...")
start_time_pg = time.time()
for table_sql in tables_schema:
    try:
        pg_cursor.execute(table_sql)
        pg_conn.commit() # Commit DDL changes for PostgreSQL
        print(f"  PostgreSQL: Created table: {table_sql.split('TABLE IF NOT EXISTS ')[1].split(' ')[0]}")
    except Exception as e:
        print(f"  PostgreSQL Error creating table: {e} - SQL: {table_sql}")
pg_cursor.close()
pg_conn.close()
end_time_pg = time.time()
print(f"PostgreSQL tables created in {end_time_pg - start_time_pg:.2f} seconds.")

# --- Create tables in CrateDB ---
print("\nCreating tables in CrateDB...")
start_time_crate = time.time()
for table_sql in crate_tables_schema: # Use crate_tables_schema for CrateDB
    try:
        crate_cursor.execute(table_sql)
        print(f"  CrateDB: Created table: {table_sql.split('TABLE IF NOT EXISTS ')[1].split(' ')[0]}")
    except Exception as e:
        print(f"  CrateDB Error creating table: {e} - SQL: {table_sql}")
crate_conn.close() # CrateDB client auto-commits DDL, but good practice to close
end_time_crate = time.time()
print(f"CrateDB tables created in {end_time_crate - start_time_crate:.2f} seconds.")

print("\nDatabase setup complete!")
