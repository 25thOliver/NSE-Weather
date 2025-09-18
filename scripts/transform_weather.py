import os
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)

def transform_weather():
	bucket = "nse-weather"
	s3_prefix_stage = "raw/weather"
	s3_prefix_stage = "staging/weather"

	storage_options = {
        "key": "oliveradmin",
        "secret": "oliver#0L",
        "client_kwargs": {"endpoint_url": "http://localhost:9000"}
	}
	
	# Find latest raw JSON file
	fs = pd.io.common.get_handle(
		f"s3://{bucket}/{s3_prefix_raw}/",
		mode="rb",
		storage_options=storage_options,
		is_text=False
	).fs

	files = fs.ls(f"{bucket}/{s3_prefix_raw}")
	files = [f for f in files if f.endswith(".json")]
	if not files:
		raise FileNotFoundError("No weather JSON files found in raw zone")

	latest_file = sorted(files)[-1]
	logging.info(f"Transforming weather file: {latest_file}")

	# Read JSON
	df = pd.read_json(f"s3://{bucket}/{latest_file}", storage_options=storage_options)

	# Flatten if nested
	if "main" in df.columns:
		main_df = pd.json_normalize(df["main"])
		df = pd.concat([df.drop(columns=["main"]), main_df], axis=1)

	# Save as Parquet
	filename = os.path.basename(latest_file).replace(".json", ".parquet")
	s3_path = f"s3://{bucket}/{s3_prefix_stage}/{filename}"

	df.to_parquet(s3_path, index=False, storage_options=storage_options)

	logging.info(f"Weather transformed and saved to {s3_path}")
	return s3_path

if __name__ == "__main__":
	transform_weather()

