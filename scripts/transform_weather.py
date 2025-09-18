import os
import logging
import pandas as pd
import s3fs
import json

logging.basicConfig(level=logging.INFO)

def transform_weather():
	bucket = "nse-weather"
	s3_prefix_raw = "raw/weather"
	s3_prefix_stage = "staging/weather"

	storage_options = {
        "key": "oliveradmin",
        "secret": "oliver#0L",
        "client_kwargs": {"endpoint_url": "http://localhost:9000"}
	}
	
	# List files using s3fs
	fs = s3fs.S3FileSystem(**storage_options)
	files = fs.ls(f"{bucket}/{s3_prefix_raw}")
	files = [f for f in files if f.endswith(".json")]
	if not files:
		raise FileNotFoundError("No weather JSON files found in raw zone")

	latest_file = sorted(files)[-1]
	logging.info(f"Transforming weather file: {latest_file}")

	# Load JSON Manually
	with fs.open(latest_file, "r") as f:
		data = json.load(f)

		# Normalize JSON into flat dataframe
		df = pd.json_normalize(data)

		# Save as Parquet
		filename = os.path.basename(latest_file).replace(".json", ".parquet")
		s3_path = f"s3://{bucket}/{s3_prefix_stage}/{filename}"

		df.to_parquet(s3_path, index=False, storage_options=storage_options)

		logging.info(f"Weather transformed and saved to {s3_path}")
		return s3_path

if __name__ == "__main__":
	transform_weather()

