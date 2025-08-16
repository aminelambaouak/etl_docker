import requests
import pandas as pd
import logging
import psycopg2
from psycopg2.extras import execute_values

# -----------------------------
# Configure logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("etl.log"),
        logging.StreamHandler()
    ]
)

print("ETL script started")

# -----------------------------
# API fetch
# -----------------------------
URL = "https://my.api.mockaroo.com/trans?key=986c0130"
CSV_FILE = "/home/amine/first_project/api_data/api_data.csv"

try:
    logging.info(f"Fetching data from {URL}")
    print(f"Fetching data from {URL}")
    response = requests.get(URL, timeout=10)
    response.raise_for_status()

    data = response.json()
    df = pd.DataFrame(data)
    logging.info(f"Fetched {len(df)} records from API")
    print(f"Fetched {len(df)} records from API")

    # Save to CSV
    df.to_csv(CSV_FILE, index=False)
    logging.info(f"Data saved to {CSV_FILE}")
    print(f"Data saved to {CSV_FILE}")

except requests.exceptions.RequestException as e:
    logging.error(f"Error fetching data: {e}", exc_info=True)
    print(f"Error fetching data: {e}")
    df = pd.DataFrame()

# -----------------------------
# Handle missing IDs
# -----------------------------
df["transaction_id"] = df["transaction_id"].mask(df["transaction_id"].isna(), pd.Series(range(1, len(df)+1)))
df["product_id"] = df["product_id"].mask(df["product_id"].isna(), pd.Series(range(1, len(df)+1)))

# -----------------------------
# Transformations
# -----------------------------
df["total_price"] = (df["quantity"] * df["price_per_unit"]).round(2)
df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")
if "is_returned" in df.columns:
    df = df.drop(columns=["is_returned"])

logging.info("Transformations applied: total_price calculated, missing IDs filled, return_flag removed")
print("Transformations applied: total_price calculated, missing IDs filled, return_flag removed")

# -----------------------------
# Save transformed data to CSV
# -----------------------------
OUTPUT_CSV = "/home/amine/first_project/transactions_transformed.csv"
df.to_csv(OUTPUT_CSV, index=False)
logging.info(f"Transformed data saved to {OUTPUT_CSV}")
print(f"Transformed data saved to {OUTPUT_CSV}")

# -----------------------------
# PostgreSQL connection info
# -----------------------------
PG_HOST = "db"
PG_PORT = "5432"
PG_DB = "first_project_db"  # must exist
PG_USER = "postgres"        # must exist
PG_PASSWORD = "amine070193"
PG_TABLE = "transactions"

# -----------------------------
# Write to PostgreSQL using psycopg2
# -----------------------------
# -----------------------------
# Write to PostgreSQL using psycopg2
# -----------------------------
cursor = None
conn = None
try:
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )
    cursor = conn.cursor()

    # -----------------------------
    # Create table if not exists
    # -----------------------------
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id TEXT PRIMARY KEY,
        user_id INT,
        product_id TEXT,
        product_category TEXT,
        quantity INT,
        price_per_unit NUMERIC(10,2),
        payment_method TEXT,
        transaction_date TIMESTAMP,
        shipping_country TEXT,
        total_price NUMERIC(10,2)
);

    """
    cursor.execute(create_table_query)
    conn.commit()

    # -----------------------------
    # Prepare insert query
    # -----------------------------
    columns = list(df.columns)
    query = f"INSERT INTO {PG_TABLE} ({', '.join(columns)}) VALUES %s"

    # Convert DataFrame to list of tuples
    values = [tuple(x) for x in df.to_numpy()]

    # Bulk insert
    execute_values(cursor, query, values)
    conn.commit()

    logging.info(f"Data loaded into PostgreSQL table '{PG_TABLE}' successfully")
    print(f"Data loaded into PostgreSQL table '{PG_TABLE}' successfully")

except Exception as e:
    logging.error(f"Error loading data into PostgreSQL: {e}", exc_info=True)
    print(f"Error loading data into PostgreSQL: {e}")

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()

logging.info("ETL job finished successfully")
print("ETL job finished successfully")
