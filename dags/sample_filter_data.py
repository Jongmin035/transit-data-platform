from airflow.sdk import dag, task
from pendulum import datetime

@dag(
    dag_id="sample_filter_data",
    schedule=None,
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["example"]
)
def tutorial_filter_data():
    @task(retries=0)
    def generate_mock_delays():
        return {
            "Route_5": 22,
            "Route_16": 38,
            "Route_145": 2,
            "Route_42": 12
        }
    
    @task(retries=0)
    def filter_delays(delays: dict):
        return {route: delay for route, delay in delays.items() if delay > 10}
    
    @task(retries=0)
    def print_results(filtered_delays: dict):
        for route, delay in filtered_delays.items():
            print(f"Route: {route}, Delay: {delay} minutes")
    
    delays = generate_mock_delays()
    filtered_delays = filter_delays(delays)
    print_results(filtered_delays)

tutorial_filter_data()