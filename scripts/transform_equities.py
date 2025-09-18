import os
import logging
import pandas as pd
import s3fs

logging.basicConfig(level=logging.INFO)

def transform_equities():
	bucket = "nse-weather"
	s3_prefix_raw = "raw/equities"
	s3_prefix_stage = "staging/equities"

	storage_options = {
		"key": "oliveradmin",
		"secret": "oliver#0L",
		"client_kwargs": {"endpoint_url": "http://localhost:9000"}
	}

	# Listing files using s3fs
	fs = s3fs.S3FileSystem(**storage_options)
	files = fs.ls(f"{bucket}/{s3_prefix_raw}")
	files = [f for f in files if f.endswith(".csv")]
	if not files:
		raise FileNotFoundError("No equities CSV file found in raw zone")

	latest_file = sorted(files)[-1]
	logging.info(f"Transforming equities file: {latest_file}")

	# Load CSV
	df = pd.read_csv(f"s3://{latest_file}", storage_options=storage_options)

	# Clean Columns
	df["Date"] = pd.to_datetime(df["Date"], dayfirst=True)
	for col in ["Price", "Open", "High", "Low"]:
		df[col] = df[col].astype(float)

	df["Change %"] = df["Change %"].astype(float)
	
	# Save as Parquet
	filename = os.path.basename(latest_file).replace(".csv", ".parquet")
	s3_path = f"s3://{bucket}/{s3_prefix_stage}/{filename}"

	df.to_parquet(s3_path, index=False, storage_options=storage_options)

	logging.info(f"Equities transformed and save to {s3_path}")
	return s3_path

if __name__ == "__main__":
	transform_equities()



