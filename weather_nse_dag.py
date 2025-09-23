from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


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
	description="ETL DAG for Nairobi Weather + JSE pipeline (stored in MinIO)",
	schedule_interval="@daily",
	start_date=days_ago(1),
	catchup=False,
	tags=["nse", "weather", "minio", "airflow-tutorial"],
) as dag:

	# Step 1: Fetch Weather Data
	fetch_weather = BashOperator(
		task_id="fetch_weather",
		bash_command="python /home/oliver/airflow-tutorial/airflow/dags/nse_weather/scripts/fetch_weather.py",
	)

	# Step 2: Fetch Equities Data(from Kaggle CSV for now)
	fetch_kaggle = BashOperator(
		task_id="fetch_kaggle",
		bash_command="python /home/oliver/airflow-tutorial/airflow/dags/nse_weather/scripts/fetch_kaggle.py",
	)

	# Step 3: Transformation Weather  --> staging parquet
	transform_weather =  BashOperator(
		task_id="transform_weather",
		bash_command="python /home/oliver/airflow-tutorial/airflow/dags/nse_weather/scripts/transform_weather.py",
	)

	# Step 4: Transform Equities CSV --> staiging parquet
	transform_equities = BashOperator(
		task_id="transform_equities",
		bash_command="python /home/oliver/airflow-tutorial/airflow/dags/nse_weather/scripts/transform_equities.py",
	)

	# Step 5: Load Weather into Postgres
	load_weather = BashOperator(
		task_id="load_weather",
		bash_command="python /home/oliver/airflow-tutorial/airflow/dags/nse_weather/scripts/load_postgres.py --weather",
	)

	# Step 6: Load Equities into Postgres
	load_equities = BashOperator(
		task_id="load_equities",
		bash_command="python /home/oliver/airflow-tutorial/airflow/dags/nse_weather/scripts/load_postgres.py --equities",
	)

	# Dependencies
	fetch_weather >> transform_weather >> load_weather
	fetch_kaggle >> transform_equities >> load_equities

	
