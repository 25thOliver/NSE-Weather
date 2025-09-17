from datetime import datetime, timedelta
import os
import shutil
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
import os 

# Base path 
BASE_PATH = os.path.join(
	os.path.dirname(__file__), 
	"data"
)


# Default args
default_args = {
	"owner": "oliver",
	"depends_on_past": False,
	"email_on_Failure": False,
	"email_on_retry": False,
	"retries": 1,
	"retry_delay": timedelta(minutes=5),
}

# DAG Definition
with DAG(
	dag_id="weather_nse_dag",
	default_args=default_args,
	description="Dummy DAG for Nairobi Weather + NSE pipeline (s3/MinIO integration will come later)",
	schedule_interval="@daily",
	start_date=days_ago(1),
	catchup=False,
	tags=["nse", "weather", "minio", "airflow-tutorial"],
) as dag:

	@task
	def start():
		logging.info("Starting weather_nse_dag (dummy run)")
		return {"started_at": datetime.utcnow().isoformat()}

	@task
	def fetch_weather_dummy():
		out_dir = os.path.join(BASE_PATH, "raw/weather")
		os.makedirs(out_dir, exist_ok=True)
		marker = os.path.join(out_dir, f"dummy_weather_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.json")
		with open(marker, "w") as fh:
			fh.write('{"source":"dummy","city":"Nairobi","note":"this is a placeholder file"}\n')
		logging.info("fetch_weather_dummy wrote %s", marker)
		return marker

	@task
	def fetch_nse_dummy():
		out_dir = os.path.join(BASE_PATH, "raw/nse")
		os.makedirs(out_dir, exist_ok=True)
		marker = os.path.join(out_dir, f"dummy_nse_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv")
		with open(marker, "w") as fh:
			fh.write("date,ticker,volume,close\n2025-09-17,TEST,1000,10.0\n")
		logging.info("fetch_nse_dummy wrote %s", marker)
		return marker

	@task
	def transform_dummy(weather_marker: str, nse_marker: str):
		out_dir = os.path.join(BASE_PATH, "processed/weather_nse")
		os.makedirs(out_dir, exist_ok=True)
		processed_file = os.path.join(out_dir, f"processed_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv")
		
		# simple mock transform: write a tiny CSV using current date
		with open(processed_file, "w") as fh:
			fh.write("date,city,avg_temp,nse_volume\n")
			fh.write(f"2025-09-17,Nairobi,---,1000\n")

		logging.info("transform_dummy wrote %s (joined %s & %s)", processed_file, weather_marker, nse_marker)
		return processed_file

	@task
	def load_dummy(processed_marker: str):
		out_dir = os.path.join(BASE_PATH, "analytics")
		os.makedirs(out_dir, exist_ok=True)
		dest = os.path.join(out_dir, os.path.basename(processed_marker))
		shutil.copy(processed_marker, dest)
		logging.info(("load_dummy copied %s to %s", processed_marker, dest))
		return dest

	# DAG topology
	start_task = start()
	weather_task = fetch_weather_dummy()
	nse_task = fetch_nse_dummy()
	processed_task =  transform_dummy(weather_task, nse_task)
	loaded_task = load_dummy(processed_task)

	start_task >> [weather_task, nse_task] >> processed_task >> loaded_task

