from airflow.sdk import dag, task
import pendulum
import requests
import json
from pathlib import Path

LOCATION = "Irvine"

@dag(
    dag_id="sample_http_request",
    schedule=None,
    start_date=pendulum.datetime(2026, 7, 1),
    catchup=False,
    tags=["example"]
)
def sample_filter_data_dag():
    @task(retries=0)
    def fetch_temperature_data(**kwargs):
        end_date_str = kwargs['ds']
        end_date_obj = pendulum.parse(end_date_str)
        start_date_str = end_date_obj.subtract(months=1).to_date_string()

        location_url = f"https://geocoding-api.open-meteo.com/v1/search?name={LOCATION}&count=1&language=en&format=json"
        response = requests.get(location_url)
        latitude = response.json()['results'][0]['latitude']
        longitude = response.json()['results'][0]['longitude']

        url = f"https://archive-api.open-meteo.com/v1/archive?latitude={latitude}&longitude={longitude}&start_date={start_date_str}&end_date={end_date_str}&hourly=temperature_2m"
        payload = requests.get(url).json()

        year = end_date_obj.format("YYYY")
        month = end_date_obj.format("MM")
        day = end_date_obj.format("DD")

        base_path = Path("/opt/airflow/dags/data/weather")
        partitioned_dir = base_path / f"year={year}" / f"month={month}" / f"day={day}"

        partitioned_dir.mkdir(parents=True, exist_ok=True)

        file_path = partitioned_dir / "raw.json"
        with open(file_path, "w") as f:
            json.dump(payload, f)

        return str(file_path)

    fetch_temperature_data()
sample_filter_data_dag()