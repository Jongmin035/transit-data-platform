import json
import boto3
import os
import requests
from airflow.sdk import dag, task
import pendulum
from botocore.exceptions import NoCredentialsError, ClientError

BUCKET_NAME = os.getenv("S3_BUCKET_ARN")

def get_ssm_parameter(parameter_name: str) -> str:
    session = boto3.Session(profile_name='data-eng')
    ssm_client = session.client('ssm')

    try:
        response = ssm_client.get_parameter(
            Name=parameter_name,
            WithDecryption=True
        )
        return response['Parameter']['Value']
    except ClientError as e:
        raise RuntimeError(f"Failed to retrieve {parameter_name} from SSM: {e}")

@dag(
    dag_id="cta_vehicle_data_to_s3",
    schedule="0 */2 * * *",
    start_date=pendulum.datetime(2026, 7, 1),
    catchup=False,
    tags=["production"]
)
def cta_vehicle_data_to_s3():
    @task(retries=3)
    def extract_cta_data():
        cta_key = get_ssm_parameter("/transit/cta_api_key")
        url = f"https://www.ctabustracker.com/bustime/api/v3/getvehicles"
        params = {
            "key": cta_key,
            "rt": "20,56,66,72",
            "format": "json"
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if "bustime-response" in data and "error" in data["bustime-response"]:
            error_msg = data["bustime-response"]["error"][0].get("msg", "Unknown error")
            print(f"CTA API returned no tracking data: {error_msg}")

        return data

    @task(retries=3)
    def load_cta_data_to_s3(data, **kwargs):
        datetime_str = kwargs['ts']
        datetime_obj = pendulum.parse(datetime_str)

        year = datetime_obj.format("YYYY")
        month = datetime_obj.format("MM")
        day = datetime_obj.format("DD")
        hour = datetime_obj.format("HH")
        base_key = f"raw/transit/vehicles/year={year}/month={month}/day={day}/hour={hour}/vehicles.json"

        session = boto3.Session(profile_name='data-eng')
        s3_client = session.client('s3')
        try:
            print(f"Attempting in-memory upload of CTA data to s3://{BUCKET_NAME}/{base_key}...")
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=base_key,
                Body=json.dumps(data),
                ContentType="application/json"
            )
            print("S3 Vehicle data upload successful")
        except NoCredentialsError:
            raise RuntimeError(
                "S3 Upload Failed: AWS credentials were not found inside the container"
            )
        except ClientError as e:
            raise RuntimeError(
                f"S3 Upload Failed: {e.response['Error']['Message']}"
            )
        except Exception as e:
            raise RuntimeError(f"Unexpected Error: {str(e)}")

    extracted_data = extract_cta_data()
    load_cta_data_to_s3(extracted_data)

cta_vehicle_data_to_s3()