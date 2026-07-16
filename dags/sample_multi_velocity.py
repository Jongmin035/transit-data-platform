from airflow.sdk import dag, task
from pendulum import datetime

@task(retries=0)
def start_task(msg: str):
    print(f"Starting task with message: {msg}")

@dag(
    schedule="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["example"]
)
def sample_daily_task():
    @task(retries=0)
    def daily_task():
        print("This is a daily task.")
    
    start_task("Hello, world!") >> daily_task()
sample_daily_task()

@dag(
    schedule="0 */2 * * *",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["example"]
)
def sample_bi_hourly_task():
    @task(retries=0)
    def bi_hourly_task():
        print("This is a bi-hourly task.")
    
    start_task("Hello, world!") >> bi_hourly_task()
sample_bi_hourly_task()