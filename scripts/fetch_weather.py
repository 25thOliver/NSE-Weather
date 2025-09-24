import os
import json
from datetime import datetime
import requests
import fsspec
from dotenv import load_dotenv
load_dotenv()


def fetch_weather():
	# Config from environment variables
	api_key = os.getenv("OPENWEATHER_API_KEY")
	if not api_key:
		raise ValueError("OPENWEATHER_API_KEY not set in environment")

	endpoint_url = os.getenv("MINIO_ENDPOINT", "http://172.17.0.1:9000")
	minio_access_key = os.getenv("MINIO_ACCESS_KEY", "oliveradmin")
	minio_secret_key = os.getenv("MINIO_SECRET_KEY", "oliver#0L")
	bucket = os.getenv("MINIO_BUCKET", "nse-weather")
		

	# API call
	url = f"http://api.openweathermap.org/data/2.5/weather?q=Nairobi&appid={api_key}&units=metric"
	response = requests.get(url)
	response.raise_for_status()
	weather_data = response.json()

	# Filename
	today = datetime.utcnow().strftime("%Y-%m-%d")
	object_key = f"raw/weather/{today}.json"
	s3_path = f"s3://{bucket}/{object_key}"

	# Save locally first(though option, since you can directly save in s3 bucket)
	local_dir = os.path.join(os.path.dirname(__file__), "..", "data", "raw", "weather")
	os.makedirs(local_dir, exist_ok=True)
	with open(os.path.join(local_dir, f"{today}.json"), "w") as f:
		json.dump(weather_data, f, indent=2)

	# Upload to MinIO
	storage_options = {
		"key": minio_access_key,
		"secret": minio_secret_key,
		"client_kwargs": {"endpoint_url": endpoint_url},
	}	
	with fsspec.open(s3_path, "w", **storage_options) as f:
		f.write(json.dumps(weather_data, indent=2))

	print(f"Uploaded Nairobi weather data to {s3_path}")

if __name__ == "__main__":
	fetch_weather()
