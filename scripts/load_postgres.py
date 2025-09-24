import logging
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Load environment variables from .env file
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../.env"))

# Load configs from environment variables
MINIO_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
DATABASE = os.getenv("DATABASE")

# MinIO storage options
storage_options = {
    "key": MINIO_KEY,
    "secret": MINIO_SECRET,
    "client_kwargs": {"endpoint_url": MINIO_ENDPOINT},
}

# Create SQLAlchemy engine for Postgres
engine = create_engine(DATABASE)

def load_to_postgres(table_name: str, s3_path: str):
    logging.info(f"Reading data from {s3_path}...")
    df = pd.read_parquet(s3_path, storage_options=storage_options, engine="pyarrow")

    logging.info(f"Loaded {len(df)} records from {s3_path}. Writing to Postgres table '{table_name}'...")
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    
    logging.info(f"Successfully loaded {len(df)} rows into '{table_name}'.")



def main():
    load_to_postgres("weather", "s3://nse-weather/staging/weather/2025-09-17.parquet")
    load_to_postgres("equities", "s3://nse-weather/staging/equities/2025-09-17.parquet")

if __name__ == "__main__":
    main()