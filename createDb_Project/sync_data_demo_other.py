import psycopg2
from crate import client as crate_client
import time
from datetime import datetime, timedelta
from faker import Faker
import random

fake = Faker()


PG_HOST = "localhost"
PG_PORT = 5434 
PG_DBNAME = "postgres"
PG_USER = "postgres"
PG_PASSWORD = "MyStongP@ssw0rd!" 

CRATE_HOST = "localhost"
CRATE_PORT = 4201 


RECORD_COUNT = 100000
BULK_INSERT_TEST_COUNT = 10000 
CONCURRENT_INSERT_COUNT = 10000 


try:
    pg_conn_sync = psycopg2.connect(f"host={PG_HOST} port={PG_PORT} dbname={PG_DBNAME} user={PG_USER} password={PG_PASSWORD}")
    pg_cursor_sync = pg_conn_sync.cursor()
    print("Connected to PostgreSQL for sync operations successfully!")
except Exception as e:
    print(f"Error connecting to PostgreSQL for sync operations (Port {PG_PORT}): {e}")
    exit()


try:
    crate_conn_sync = crate_client.connect(f"{CRATE_HOST}:{CRATE_PORT}")
    crate_cursor_sync = crate_conn_sync.cursor()
    print("Connected to CrateDB for sync operations successfully!")
except Exception as e:
    print(f"Error connecting to CrateDB for sync operations (Port {CRATE_PORT}): {e}")
    exit()

def get_unique_email_for_pg(cursor, retries=100):
    """Generates a unique email and checks if it already exists in PostgreSQL."""
    for _ in range(retries):
        email = fake.unique.email()
        cursor.execute("SELECT COUNT(*) FROM customers WHERE email = %s;", (email,))
        if cursor.fetchone()[0] == 0:
            return email
    raise Exception(f"Could not generate a unique email after {retries} attempts.")

def sync_customer_updates():
    print("\n--- Starting Data Synchronization Demo (Customers Table) ---")

   
    print("\n--- Demonstrating UPDATE synchronization ---")
    
    pg_cursor_sync.execute("SELECT customer_id, name, email, status FROM customers ORDER BY RANDOM() LIMIT 1;")
    customer_to_update_pg = pg_cursor_sync.fetchone()

  
    if not customer_to_update_pg:
        print("  No customers found to update for demo. Skipping UPDATE demo.")
  
    else:
        customer_id = customer_to_update_pg[0]
        original_name = customer_to_update_pg[1]
        original_email = customer_to_update_pg[2]
        original_status = customer_to_update_pg[3]

        new_name = f"SYNCED {fake.first_name()} {fake.last_name()}"
       
        new_email = get_unique_email_for_pg(pg_cursor_sync)
        new_status = random.choice(["inactive", "suspended", "active"])

        print(f"  Attempting to update customer_id {customer_id}:")
        print(f"    PG Original: Name='{original_name}', Email='{original_email}', Status='{original_status}'")
        print(f"    PG New:      Name='{new_name}', Email='{new_email}', Status='{new_status}'")


        
        update_pg_query = "UPDATE customers SET name = %s, email = %s, status = %s WHERE customer_id = %s;"
        pg_cursor_sync.execute(update_pg_query, (new_name, new_email, new_status, customer_id))
        pg_conn_sync.commit()
        print("  PostgreSQL updated successfully.")

        
        pg_cursor_sync.execute(f"SELECT name, email, status FROM customers WHERE customer_id = {customer_id};")
        pg_updated_data = pg_cursor_sync.fetchone()
        print(f"    PG Current: Name='{pg_updated_data[0]}', Email='{pg_updated_data[1]}', Status='{pg_updated_data[2]}'")

        
        print("  Propagating update to CrateDB...")
        
        pg_cursor_sync.execute(f"SELECT registration_date FROM customers WHERE customer_id = {customer_id};")
        registration_date = pg_cursor_sync.fetchone()[0]

        update_crate_query = (
            "INSERT INTO customers (customer_id, name, email, registration_date, status) VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT (customer_id) DO UPDATE SET name = ?, email = ?, status = ?;"
        )
        crate_cursor_sync.execute(
            update_crate_query,
            (customer_id, new_name, new_email, registration_date, new_status, 
             new_name, new_email, new_status) 
        )
        print("  CrateDB update propagated successfully.")

        
        crate_cursor_sync.execute(f"SELECT name, email, status FROM customers WHERE customer_id = {customer_id};")
        crate_updated_data = crate_cursor_sync.fetchone()
        print(f"    CrateDB Current: Name='{crate_updated_data[0]}', Email='{crate_updated_data[1]}', Status='{crate_updated_data[2]}'")
        print("  UPDATE synchronization complete.")

    
    print("\n--- Demonstrating INSERT synchronization ---")
    
    pg_cursor_sync.execute("SELECT MAX(customer_id) FROM customers;")
    max_pg_id = pg_cursor_sync.fetchone()[0] or 0 

    crate_cursor_sync.execute("SELECT MAX(customer_id) FROM customers;")
    max_crate_id = crate_cursor_sync.fetchone()[0] or 0 

    
    
    base_id = max(max_pg_id, max_crate_id, RECORD_COUNT, BULK_INSERT_TEST_COUNT, CONCURRENT_INSERT_COUNT)
    new_customer_id = base_id + 1 + random.randint(1, 1000) 


    new_customer_name = f"NEW {fake.first_name()} {fake.last_name()}"
    
    new_customer_email = get_unique_email_for_pg(pg_cursor_sync)
    new_customer_reg_date = datetime.now()
    new_customer_status = "pending"

    print(f"  Inserting new customer_id {new_customer_id} into PostgreSQL...")
    insert_pg_query = "INSERT INTO customers (customer_id, name, email, registration_date, status) VALUES (%s, %s, %s, %s, %s);"
    pg_cursor_sync.execute(insert_pg_query, (new_customer_id, new_customer_name, new_customer_email, new_customer_reg_date, new_customer_status))
    pg_conn_sync.commit()
    print("  PostgreSQL insert successful.")

  
    pg_cursor_sync.execute(f"SELECT name, email FROM customers WHERE customer_id = {new_customer_id};")
    print(f"    PG New Customer: {pg_cursor_sync.fetchone()}")

    
    print("  Propagating new customer to CrateDB...")
    insert_crate_query = "INSERT INTO customers (customer_id, name, email, registration_date, status) VALUES (?, ?, ?, ?, ?);"
    crate_cursor_sync.execute(insert_crate_query, (new_customer_id, new_customer_name, new_customer_email, new_customer_reg_date, new_customer_status))
    print("  CrateDB insert propagated successfully.")

    
    crate_cursor_sync.execute(f"SELECT name, email FROM customers WHERE customer_id = {new_customer_id};")
    print(f"    CrateDB New Customer: {crate_cursor_sync.fetchone()}")
    print("  INSERT synchronization complete.")

    
    print("\n--- Demonstrating DELETE synchronization (cleaning up new customer) ---")
    print(f"  Deleting customer_id {new_customer_id} from PostgreSQL...")
    delete_pg_query = "DELETE FROM customers WHERE customer_id = %s;"
    pg_cursor_sync.execute(delete_pg_query, (new_customer_id,))
    pg_conn_sync.commit()
    print("  PostgreSQL delete successful.")

    
    pg_cursor_sync.execute(f"SELECT COUNT(*) FROM customers WHERE customer_id = {new_customer_id};")
    print(f"    PG Count of deleted customer: {pg_cursor_sync.fetchone()[0]}")

    
    print("  Propagating delete to CrateDB...")
    delete_crate_query = "DELETE FROM customers WHERE customer_id = ?;"
    crate_cursor_sync.execute(delete_crate_query, (new_customer_id,))
    print("  CrateDB delete propagated successfully.")

    
    crate_cursor_sync.execute(f"SELECT COUNT(*) FROM customers WHERE customer_id = {new_customer_id};")
    print(f"    CrateDB Count of deleted customer: {crate_cursor_sync.fetchone()[0]}")
    print("  DELETE synchronization complete.")

    print("\n--- All Data Synchronization Demos Complete ---")


if __name__ == "__main__":
    sync_customer_updates()
    pg_cursor_sync.close()
    pg_conn_sync.close()
    crate_cursor_sync.close()
    crate_conn_sync.close()
    print("Sync connections closed.")
