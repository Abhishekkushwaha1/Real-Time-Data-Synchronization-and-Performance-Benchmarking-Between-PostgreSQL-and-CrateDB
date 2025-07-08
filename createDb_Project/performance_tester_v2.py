import psycopg2
from crate import client as crate_client
import time
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

# --- Database Configuration (ADJUST THESE TO YOUR TEST INSTANCES) ---
PG_HOST = "localhost"
PG_PORT = 5436 # IMPORTANT: New PostgreSQL Port
PG_DBNAME = "postgres"
PG_USER = "postgres"
PG_PASSWORD = "your_new_strong_password_v2" # IMPORTANT: Your new PG password

CRATE_HOST = "localhost"
CRATE_PORT = 4203 # IMPORTANT: New CrateDB Port

RECORD_COUNT = 1000000 # Total records inserted per table

# --- Connect to PostgreSQL ---
try:
    pg_conn = psycopg2.connect(f"host={PG_HOST} port={PG_PORT} dbname={PG_DBNAME} user={PG_USER} password={PG_PASSWORD}")
    pg_cursor = pg_conn.cursor()
    print("Connected to PostgreSQL successfully!")
except Exception as e:
    print(f"Error connecting to PostgreSQL (Port {PG_PORT}): {e}")
    exit()

# --- Connect to CrateDB ---
try:
    crate_conn = crate_client.connect(f"{CRATE_HOST}:{CRATE_PORT}")
    crate_cursor = crate_conn.cursor()
    print("Connected to CrateDB successfully!")
except Exception as e:
    print(f"Error connecting to CrateDB (Port {CRATE_PORT}): {e}")
    exit()

# --- Helper function for running and measuring tests ---
def run_test(db_cursor, db_type, test_name, query, params=None, commit_required=False, fetch_results=False, explain_query=False):
    """
    Executes a query and measures its execution time.
    :param db_cursor: The database cursor (psycopg2 or crate client).
    :param db_type: String, either "PostgreSQL" or "CrateDB".
    :param test_name: String, a descriptive name for the test.
    :param query: The SQL query string.
    :param params: A tuple of parameters for the query (or None).
    :param commit_required: Boolean, True if a commit is needed (e.g., for DML in PostgreSQL).
    :param fetch_results: Boolean, True if results should be fetched (e.g., for SELECT COUNT).
    :param explain_query: Boolean, True if EXPLAIN output should be fetched.
    :return: The execution time in seconds, or -1 if an error occurred.
    """
    start_time = time.time()
    try:
        if explain_query:
            explain_sql = f"EXPLAIN {query}"
            db_cursor.execute(explain_sql, params)
            explain_output = db_cursor.fetchall()
            print(f"    {db_type} - {test_name} EXPLAIN Plan:")
            for row in explain_output:
                print(f"        {row[0]}") # EXPLAIN output is usually in one column
            start_time = time.time() # Reset timer for actual query execution
            db_cursor.execute(query, params) # Execute actual query
        else:
            db_cursor.execute(query, params)

        if commit_required and db_type == "PostgreSQL":
            pg_conn.commit()

        result = None
        if fetch_results:
            result = db_cursor.fetchone()
        
        end_time = time.time()
        duration = end_time - start_time
        result_str = f"Result: {result[0]}" if result and len(result) > 0 else ""
        print(f"  {db_type} - {test_name}: {duration:.4f} seconds {result_str}")
        return duration
    except Exception as e:
        print(f"  {db_type} - {test_name} FAILED: {e}")
        if commit_required and db_type == "PostgreSQL":
            pg_conn.rollback()
        return -1

# --- Performance Test Scenarios ---
print("\n--- Starting Performance Tests ---")
print(f"Testing {RECORD_COUNT} records per table.")

# --- Test 1: Massive Bulk Data Ingestion (Re-demonstrate the initial win) ---
# This re-runs a smaller bulk insert to show CrateDB's speed for this specific operation.
print("\n--- Test 1: Massive Bulk Data Ingestion (100,000 Additional Customers) ---")
BULK_INSERT_TEST_COUNT = 100000
STARTING_CUSTOMER_ID_TEST = RECORD_COUNT + 1 # Ensure no ID conflict

customer_data_batch_test = []
for i in range(BULK_INSERT_TEST_COUNT):
    customer_id = STARTING_CUSTOMER_ID_TEST + i
    customer_data_batch_test.append((customer_id, fake.name(), fake.unique.email(), datetime.now(), "active"))

pg_insert_bulk_query_test = "INSERT INTO customers (customer_id, name, email, registration_date, status) VALUES (%s, %s, %s, %s, %s);"
crate_insert_bulk_query_test = "INSERT INTO customers (customer_id, name, email, registration_date, status) VALUES (?, ?, ?, ?, ?);"

print("  Preparing PostgreSQL bulk insert test...")
pg_start_time = time.time()
try:
    pg_cursor.executemany(pg_insert_bulk_query_test, customer_data_batch_test)
    pg_conn.commit()
    print(f"  PostgreSQL - Bulk Insert Test: {time.time() - pg_start_time:.4f} seconds")
except Exception as e:
    print(f"  PostgreSQL - Bulk Insert Test FAILED: {e}")
    pg_conn.rollback()

print("  Preparing CrateDB bulk insert test...")
crate_start_time = time.time()
try:
    crate_cursor.executemany(crate_insert_bulk_query_test, customer_data_batch_test)
    print(f"  CrateDB - Bulk Insert Test: {time.time() - crate_start_time:.4f} seconds")
except Exception as e:
    print(f"  CrateDB - Bulk Insert Test FAILED: {e}")

# Clean up this test data
run_test(pg_cursor, "PostgreSQL", "Cleanup PG Test Data", f"DELETE FROM customers WHERE customer_id >= {STARTING_CUSTOMER_ID_TEST};", commit_required=True)
run_test(crate_cursor, "CrateDB", "Cleanup CrateDB Test Data", f"DELETE FROM customers WHERE customer_id >= {STARTING_CUSTOMER_ID_TEST};")


# --- Test 2: Complex Analytical Aggregation (Sales by Category, full dataset) ---
# This should favor CrateDB with very large datasets due to columnar processing.
print("\n--- Test 2: Aggregate Total Sales by Category (Full Dataset) ---")
pg_agg_query = """
SELECT p.category, SUM(oi.quantity * oi.unit_price) AS total_sales
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
GROUP BY p.category
ORDER BY total_sales DESC;
"""
crate_agg_query = """
SELECT p.category, SUM(oi.quantity * oi.unit_price) AS total_sales
FROM order_items AS oi
INNER JOIN products AS p ON oi.product_id = p.product_id
GROUP BY p.category
ORDER BY total_sales DESC;
"""
run_test(pg_cursor, "PostgreSQL", "Total Sales by Category", pg_agg_query)
# MODIFIED: Added explain_query=True for CrateDB
run_test(crate_cursor, "CrateDB", "Total Sales by Category", crate_agg_query, explain_query=True)

# --- Test 3: Time-Series Aggregation (Daily Orders in Last Year) ---
# CrateDB's time-series optimizations and DATE_BIN function should perform well here.
print("\n--- Test 3: Daily Order Count (Last 365 Days) ---")
pg_time_series_query = """
SELECT DATE_TRUNC('day', order_date) AS order_day, COUNT(order_id) AS daily_orders
FROM orders
WHERE order_date >= NOW() - INTERVAL '365 days'
GROUP BY 1
ORDER BY 1;
"""
# CORRECTED CRATEDB QUERY: Added the third 'origin' argument (0::timestamp)
crate_time_series_query = """
SELECT DATE_BIN(INTERVAL '1 day', order_date::timestamp with time zone, 0::timestamp) AS order_day, COUNT(order_id) AS daily_orders
FROM orders
WHERE order_date >= NOW() - INTERVAL '365 day'
GROUP BY 1
ORDER BY 1;
"""
run_test(pg_cursor, "PostgreSQL", "Daily Order Count", pg_time_series_query)
run_test(crate_cursor, "CrateDB", "Daily Order Count", crate_time_series_query)

# --- Test 4: Full-Text Search on Product Descriptions ---
# This highlights CrateDB's built-in Lucene integration.
print("\n--- Test 4: Full-Text Search on Product Descriptions ---")
# Use a common word that should appear frequently in fake.text()
search_term = "lorem" # 'lorem' is common in Faker's text. Adjust if your Faker version doesn't use it.

# PostgreSQL Full-Text Search (basic ILIKE for comparison; actual FTS is more complex in PG)
pg_fts_query = """
SELECT COUNT(*) FROM products WHERE description ILIKE %s;
"""
# CrateDB Full-Text Search (uses the FULLTEXT index created in db_setup_v2.py)
crate_fts_query = """
SELECT COUNT(*) FROM products WHERE MATCH(description, ?);
"""
run_test(pg_cursor, "PostgreSQL", "Full-Text Search (ILIKE)", pg_fts_query, (f"%{search_term}%",), fetch_results=True)
run_test(crate_cursor, "CrateDB", "Full-Text Search (MATCH)", crate_fts_query, (search_term,), fetch_results=True)


# --- Test 5: Complex Join with Aggregation on a Large Subset ---
# This simulates a dashboard query, e.g., total amount spent by customers in a specific status
print("\n--- Test 5: Total Spend by Customers in 'active' Status (Complex Join/Agg) ---")
pg_complex_query = """
SELECT c.status, SUM(o.total_amount) AS total_amount_spent
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE c.status = %s
GROUP BY c.status;
"""
crate_complex_query = """
SELECT c.status, SUM(o.total_amount) AS total_amount_spent
FROM customers AS c
INNER JOIN orders AS o ON c.customer_id = o.customer_id
WHERE c.status = ?
GROUP BY c.status;
"""
run_test(pg_cursor, "PostgreSQL", "Spend by Active Customers", pg_complex_query, ("active",))
# MODIFIED: Added explain_query=True for CrateDB
run_test(crate_cursor, "CrateDB", "Spend by Active Customers", crate_complex_query, ("active",), explain_query=True)


print("\n--- All Performance Tests Complete ---")

# --- Close connections ---
pg_cursor.close()
pg_conn.close()
crate_conn.close()
print("Connections closed.")
