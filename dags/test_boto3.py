import json
import boto3
import os
from airflow.sdk import dag, task
from pendulum import datetime
from botocore.exceptions import NoCredentialsError, ClientError

BUCKET_NAME = os.getenv("S3_BUCKET_ARN")

@dag(
    schedule=None,  # Manual trigger only!
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["testing"]
)
def aws_s3_handshake_test():

    @task(retries=0)
    def test_s3_upload():
        # 1. Create a dummy test payload
        dummy_data = {
            "test_status": "connected",
            "message": "Hello from inside the Airflow Docker container!",
            "timestamp": "2026-07-16T17:15:00"
        }
        
        s3_key = "raw/testing/handshake_test.json"
        
        # 2. Attempt S3 upload using Boto3
        session = boto3.Session(profile_name='data-eng')
        s3_client = session.client('s3')
        
        try:
            print(f"Attempting S3 upload to bucket: {BUCKET_NAME}...")
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                Body=json.dumps(dummy_data, indent=4),
                ContentType="application/json"
            )
            print(f"🎉 SUCCESS! Handshake complete. File written to s3://{BUCKET_NAME}/{s3_key}")
            
        except NoCredentialsError:
            raise RuntimeError(
                "❌ S3 Upload Failed: AWS credentials were not found inside the container! "
                "We need to pass them via environment variables or a volume mount."
            )
        except ClientError as e:
            raise RuntimeError(
                f"❌ S3 Upload Failed: Handshake reached AWS, but failed on permissions/settings. "
                f"Error: {e.response['Error']['Message']}"
            )
        except Exception as e:
            raise RuntimeError(f"❌ Unexpected Error: {str(e)}")

    test_s3_upload()

aws_s3_handshake_test()