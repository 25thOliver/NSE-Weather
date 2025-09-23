import logging
import pandas as pd
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)

# MinIO storage options
storage_options = {
	"key": "MINIO_ACCESS_KEY",
	"secret": "MINIO_SECRET_KEYS",
	"client_kwargs": {"endpoint_url": "MINIO_ENDPOINT"},
}

# Postgres connection string
conn = "DATABASE"
def load_weather():
	df = pd.read_parquet(
		"s3://nse-weather/staging/weather/2025-09-17.parquet",
		storage_options=storage_options,
	)

	engine = create_engine(conn)
	df.to_sql("weather", engine, if_exists="append", index=False)
	logging.info("Loaded weather data into Postgres")

def load_equities():
	df = pd.read_parquet(
		"s3://nse-weather/staging/equities/2025-09-17.parquet",
		storage_options=storage_options,
	)

	engine = create_engine(conn)
	df.to_sql("equities", engine, if_exists="append", index=False)
	logging.info("Load equities data into Postgres")

if __name__ == "__main__":
	load_weather()
	load_equities()
