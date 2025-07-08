import psycopg2
import psycopg2.extras # Import for execute_values
from crate import client as crate_client
from faker import Faker
import random
from datetime import datetime, timedelta
import time
import uuid

fake = Faker()

# --- Configuration for NEW instances ---
PG_HOST = "localhost"
PG_PORT = 5436 # NEW PostgreSQL Port
PG_DBNAME = "postgres"
PG_USER = "postgres"
PG_PASSWORD = "your_new_strong_password_v2" # !! IMPORTANT: Use the password set during docker run

CRATE_HOST = "localhost"
CRATE_PORT = 4203 # NEW CrateDB Port (HTTP/Admin UI)

RECORD_COUNT = 1000000 # !! VERY HIGH DATA VOLUME: 1 Million records per table

# --- Optimized Batch Sizes for Medium Hardware ---
# Adjust these based on your system's RAM and CPU.
# Larger is generally faster, up to a point where memory/network becomes a bottleneck.
PG_BATCH_SIZE = 100000 # Increased for execute_values, try 50k-200k
CRATE_BULK_CHUNK_SIZE = 100000 # Can go higher for CrateDB, try 50k-200k

# --- Data Generation Functions ---
# (These remain the same, as the speedup is in insertion)
def generate_customers(count):
    print(f"Generating {count} customers...")
    customers = []
    for i in range(1, count + 1):
        customers.append((
            i,  # customer_id
            fake.name(),
            fake.unique.email(), # Keep unique for primary key, but know it adds overhead
            fake.date_time_between(start_date="-5y", end_date="now"), # Wider date range
            random.choice(["active", "inactive", "pending"])
        ))
    return customers

def generate_products(count):
    print(f"Generating {count} products...")
    products = []
    categories = ["Electronics", "Clothing", "Books", "Home", "Sports", "Food", "Toys", "Automotive", "Beauty", "Garden"]
    for i in range(1, count + 1):
        products.append((
            i,  # product_id
            fake.word().capitalize() + " " + fake.color_name(),
            fake.text(max_nb_chars=200), # Longer description for FTS
            round(random.uniform(9.99, 999.99), 2),
            random.choice(categories)
        ))
    return products

def generate_orders(count, customer_count):
    print(f"Generating {count} orders...")
    orders = []
    for i in range(1, count + 1):
        orders.append((
            i,  # order_id
            random.randint(1, customer_count),
            fake.date_time_between(start_date="-5y", end_date="now"), # Wider date range for time-series
            round(random.uniform(10.00, 5000.00), 2),
            random.choice(["completed", "processing", "shipped", "cancelled"])
        ))
    return orders

def generate_order_items(count, order_count, product_count):
    print(f"Generating {count} order_items...")
    items = []
    for i in range(1, count + 1):
        order_id = random.randint(1, order_count)
        product_id = random.randint(1, product_count)
        unit_price = round(random.uniform(9.99, 499.99), 2)
        quantity = random.randint(1, 10)
        
        items.append((
            i,  # item_id
            order_id,
            product_id,
            quantity,
            unit_price
        ))
    return items

def generate_inventory(count, product_count):
    print(f"Generating {count} inventory records...")
    inventory = []
    warehouses = ["North", "South", "East", "West", "Central", "Online Fulfillment"]
    for i in range(1, count + 1):
        inventory.append((
            i,  # inventory_id
            random.randint(1, product_count),
            random.randint(0, 1000),
            random.choice(warehouses),
            fake.date_time_between(start_date="-5y", end_date="now")
        ))
    return inventory

# --- Connect to Databases ---
try:
    pg_conn = psycopg2.connect(f"host={PG_HOST} port={PG_PORT} dbname={PG_DBNAME} user={PG_USER} password={PG_PASSWORD}")
    # Set autocommit to False for better bulk insert performance (commit explicitly at end of table insert)
    pg_conn.autocommit = False 
    pg_cursor = pg_conn.cursor()
    print("Connected to PostgreSQL successfully!")
except Exception as e:
    print(f"Error connecting to PostgreSQL (Port {PG_PORT}): {e}")
    exit()

try:
    crate_conn = crate_client.connect(f"{CRATE_HOST}:{CRATE_PORT}")
    crate_cursor = crate_conn.cursor()
    print("Connected to CrateDB successfully!")
except Exception as e:
    print(f"Error connecting to CrateDB (Port {CRATE_PORT}): {e}")
    exit()

# --- Data Cleanup Function ---
def cleanup_data(pg_cursor, crate_cursor):
    tables = ["order_items", "orders", "inventory", "products", "customers"] # Order matters for FK constraints if not TRUNCATE
    
    print("\n--- Cleaning up existing data ---")
    
    # PostgreSQL Cleanup
    pg_start_time = time.time()
    try:
        for table in tables:
            pg_cursor.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;") # CASCADE handles FK dependencies
            pg_conn.commit()
            print(f"  PostgreSQL: Truncated {table}.")
        print(f"PostgreSQL cleanup completed in {time.time() - pg_start_time:.2f} seconds.")
    except Exception as e:
        print(f"PostgreSQL cleanup FAILED: {e}")
        pg_conn.rollback()

    # CrateDB Cleanup - Changed from TRUNCATE to DELETE FROM
    crate_start_time = time.time()
    try:
        for table in tables:
            # CrateDB does not support TRUNCATE in the same way; use DELETE FROM
            crate_cursor.execute(f"DELETE FROM {table};")
            # CrateDB DELETE is implicitly committed or committed with next DDL/DML.
            print(f"  CrateDB: Deleted all rows from {table}.")
        print(f"CrateDB cleanup completed in {time.time() - crate_start_time:.2f} seconds.")
    except Exception as e:
        print(f"CrateDB cleanup FAILED: {e}")


# --- Insertion Helper Function (Optimized for CrateDB Bulk & PG execute_values) ---
def insert_data_in_batches(db_cursor, db_conn, table_name, columns, data, is_crate=False):
    print(f"  Inserting into {table_name}...")
    column_str = ", ".join(columns)
    
    if is_crate:
        placeholders = ", ".join(["?"] * len(columns))
        insert_sql = f"INSERT INTO {table_name} ({column_str}) VALUES ({placeholders})"
        
        for i in range(0, len(data), CRATE_BULK_CHUNK_SIZE):
            chunk = data[i:i + CRATE_BULK_CHUNK_SIZE]
            try:
                # CrateDB's client.execute with a list of tuples automatically does a bulk insert efficiently
                db_cursor.executemany(insert_sql, chunk) 
                print(f"    Inserted {i + len(chunk)} records into {table_name} (CrateDB - Bulk)")
            except Exception as e:
                print(f"    Error during CrateDB bulk insert into {table_name} at chunk {i}: {e}")
                # For CrateDB, errors in one batch might not stop others, but you might want to break.
                break
                
    else: # PostgreSQL using psycopg2.extras.execute_values
        placeholders = ", ".join(["%s"] * len(columns)) # %s for positional parameters
        insert_sql = f"INSERT INTO {table_name} ({column_str}) VALUES %s" # %s for execute_values
        
        try:
            # Using execute_values for much faster PostgreSQL bulk inserts
            # It builds a single INSERT statement with multiple VALUES clauses
            psycopg2.extras.execute_values(db_cursor, insert_sql, data, page_size=PG_BATCH_SIZE)
            db_conn.commit() # Commit once after all data for the table is sent
            print(f"    Inserted {len(data)} records into {table_name} (PostgreSQL - execute_values)")
        except Exception as e:
            print(f"    Error during PostgreSQL bulk insert into {table_name}: {e}")
            db_conn.rollback() # Rollback the entire table's insertion if an error occurs

    print(f"  Finished inserting {len(data)} records into {table_name}.")

# --- Main Data Ingestion Process ---
print("\n--- Starting Data Ingestion ---")
total_start_time = time.time()

# 1. Clean up existing data
cleanup_data(pg_cursor, crate_cursor)

# 2. Generate all data in memory
print("\n--- Generating All Data in Memory ---")
all_customers = generate_customers(RECORD_COUNT)
all_products = generate_products(RECORD_COUNT)
all_orders = generate_orders(RECORD_COUNT, RECORD_COUNT)
all_order_items = generate_order_items(RECORD_COUNT, RECORD_COUNT, RECORD_COUNT)
all_inventory = generate_inventory(RECORD_COUNT, RECORD_COUNT)

data_to_insert = {
    "customers": (all_customers, ["customer_id", "name", "email", "registration_date", "status"]),
    "products": (all_products, ["product_id", "name", "description", "price", "category"]),
    "orders": (all_orders, ["order_id", "customer_id", "order_date", "total_amount", "status"]),
    "order_items": (all_order_items, ["item_id", "order_id", "product_id", "quantity", "unit_price"]),
    "inventory": (all_inventory, ["inventory_id", "product_id", "quantity", "warehouse", "last_updated"])
}

# 3. Insert into PostgreSQL
print("\nInserting data into PostgreSQL...")
pg_insert_start_time = time.time()
for table_name, (data, columns) in data_to_insert.items():
    insert_data_in_batches(pg_cursor, pg_conn, table_name, columns, data, is_crate=False)
pg_insert_end_time = time.time()
print(f"PostgreSQL data insertion completed in {pg_insert_end_time - pg_insert_start_time:.2f} seconds.")

# 4. Insert into CrateDB
print("\nInserting data into CrateDB...")
crate_insert_start_time = time.time()
for table_name, (data, columns) in data_to_insert.items():
    insert_data_in_batches(crate_cursor, crate_conn, table_name, columns, data, is_crate=True)
crate_insert_end_time = time.time()
print(f"CrateDB data insertion completed in {crate_insert_end_time - crate_insert_start_time:.2f} seconds.")

total_end_time = time.time()
print(f"\n--- Total Data Ingestion Time (including cleanup): {total_end_time - total_start_time:.2f} seconds ---")

# --- Close connections ---
pg_cursor.close()
pg_conn.close()
crate_conn.close()
print("Connections closed.")
