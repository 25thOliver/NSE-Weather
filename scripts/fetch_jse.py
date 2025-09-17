import os
import logging
from datetime import datetime, timezone
import pandas as pd
import yfinance as yf
yf.set_proxy(None)
from minio import Minio
from minio.error import S3Error

logging.basicConfig(level=logging.INFO)

def fetch_jse():
	# Config
	minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
	minio_access_key = os.getenv("MINIO_ACCESS_KEY", "oliverminio")
	minio_secret_key = os.getenv("MINIO_SECRET_KEY", "oliver#0L")
	bucket = os.getenv("MINIO_BUCKET", "nse-weather")

	today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

	# Fetch JSE data via Yahoo Finance
	tickers = ["NPN.JO", "SOL.JO", "SBK.JO", "FSR.JO", "MTN.JO"]
	data = []

	for ticker in tickers:
		stock = yf.Ticker(ticker)
		hist = stock.history(period="6mo")
		if not hist.empty:
			row = hist.iloc[-1]
			data.append(
				{
					"date": today,
					"ticker": ticker,
					"close": row["Close"],
					"volume": int(row["Volume"]),
				}
			)
		else:
			logging.warning("No data found for {ticker}")

	if not data:
		raise ValueError("NO JSE data retrieved today")

	df = pd.DataFrame(data)

	# Save locally 
	local_dir = os.path.join(os.path.dirname(__file__), "..", "data", "raw", "jse")
	os.makedirs(local_dir, exist_ok=True)
	local_path = os.path.join(local_dir, f"{today}.csv")
	df.to_csv(local_path, index=False)
	logging.info("Saved JSE data locally at {local_path}")

	# MinIO Client
	client = Minio(
		endpoint=minio_endpoint.replace("http://", "").replace("https://", ""),
		access_key=minio_access_key,
		secret_key=minio_secret_key,
		secure=minio_endpoint.startswith("https"),
	)

	if not client.bucket_exists(bucket):
		client.make_bucket(bucket)

	# Upload to MinIO
	s3_path = f"raw/jse/{today}.csv"
	client.fput_object(bucket, s3_path, local_path)
	logging.info(f"Uploaded JSE data to s3://{bucket}/{s3_path}")

	return s3_path

if __name__ == "__main__":
	try:
		fetch_jse()
	except S3Error as e:
		logging.error("MinIO error: %s", e)
