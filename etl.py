import requests
import pandas as pd
import logging
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

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
API_KEY = os.getenv("API_KEY")
URL = f"https://my.api.mockaroo.com/trans?key={API_KEY}"
CSV_FILE = "/home/amine/first_project/transactions_transformed.csv"
OUTPUT_CSV = "/home/amine/first_project/transactions_transformed.csv"

try:
    logging.info(f"Fetching data from {URL}")
    print(f"Fetching data from {URL}")
    response = requests.get(URL, timeout=10)
    response.raise_for_status()

    data = response.json()
    df = pd.DataFrame(data)
    logging.info(f"Fetched {len(df)} records from API")
    print(f"Fetched {len(df)} records from API")

    # Save raw CSV
    df.to_csv(CSV_FILE, index=False)
    logging.info(f"Data saved to {CSV_FILE}")
    print(f"Data saved to {CSV_FILE}")

except requests.exceptions.RequestException as e:
    logging.error(f"Error fetching data: {e}", exc_info=True)
    print(f"Error fetching data: {e}")
    df = pd.DataFrame()

# -----------------------------
# Transformations
# -----------------------------
if not df.empty:
    # Fill missing IDs
    df["transaction_id"] = df["transaction_id"].mask(
        df["transaction_id"].isna(), pd.Series(range(1, len(df)+1))
    )
    df["product_id"] = df["product_id"].mask(
        df["product_id"].isna(), pd.Series(range(1, len(df)+1))
    )

    # Calculate total_price
    df["total_price"] = (df["quantity"] * df["price_per_unit"]).round(2)
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")

    # Drop return_flag or is_returned if exists
    if "is_returned" in df.columns:
        df = df.drop(columns=["is_returned"])
    if "return_flag" in df.columns:
        df = df.drop(columns=["return_flag"])

    logging.info("Transformations applied: total_price calculated, missing IDs filled, return_flag removed")
    print("Transformations applied: total_price calculated, missing IDs filled, return_flag removed")

    # Save transformed CSV
    OUTPUT_CSV = "/home/amine/first_project/transactions_transformed.csv"
    df.to_csv(OUTPUT_CSV, index=False)
    logging.info(f"Transformed data saved to {OUTPUT_CSV}")
    print(f"Transformed data saved to {OUTPUT_CSV}")
else:
    logging.warning("DataFrame is empty. Skipping transformations.")
    print("DataFrame is empty. Skipping transformations.")

# -----------------------------
# PostgreSQL connection info
# -----------------------------
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5434")
DB_NAME = os.getenv("DB_NAME")
DB_TABLE = os.getenv("DB_TABLE", "transactions")

# -----------------------------
# Write to PostgreSQL
# -----------------------------
cursor = None
conn = None
try:
    if not df.empty:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        # Create table if not exists
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {DB_TABLE} (
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

        # Insert data
        columns = list(df.columns)
        query = f"INSERT INTO {DB_TABLE} ({', '.join(columns)}) VALUES %s"
        values = [tuple(x) for x in df.to_numpy()]
        execute_values(cursor, query, values)
        conn.commit()

        logging.info(f"Data loaded into PostgreSQL table '{DB_TABLE}' successfully")
        print(f"Data loaded into PostgreSQL table '{DB_TABLE}' successfully")
    else:
        logging.warning("DataFrame is empty. Skipping PostgreSQL loading.")
        print("DataFrame is empty. Skipping PostgreSQL loading.")

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
