import os
import tempfile
import pendulum
from airflow.decorators import dag, task
from pyspark.sql import SparkSession

S3_BUCKET_NAME = os.getenv("S3_BUCKET_ARN")
S3_PARQUET_PATH = f"s3a://{S3_BUCKET_NAME}/processed/transit/vehicles/"

POSTGRES_HOST = "postgres"
POSTGRES_PORT = "5432"
POSTGRES_DB = "sandbox_db"
POSTGRES_USER = "jongmin"
POSTGRES_PASSWORD = os.getenv("MY_DB_PASSWORD")

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
TARGET_TABLE = "raw.cta_vehicle_positions"

os.environ["AWS_PROFILE"] = "data-eng"

@dag(
    dag_id="cta_load_s3_parquet_to_postgres",
    schedule=None,
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    tags=["production"]
)
def cta_load_s3_parquet_to_postgres():

    @task(retries=1)
    def load_s3_parquet_to_postgres():
        spark = (
            SparkSession.builder.appName("S3ToPostgres")
            .master("local[*]")
            .config("spark.jars.ivy", tempfile.mkdtemp(prefix="ivy-"))
            .config(
                "spark.jars.packages",
                "org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.4"
            )
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
            .getOrCreate()
        )

        print(f"Reading partitioned Parquet data from: {S3_PARQUET_PATH}")

        try:
            df = spark.read.parquet(S3_PARQUET_PATH)
        except Exception as e:
            spark.stop()
            raise e

        print(f"Writing data to Postgres table: {TARGET_TABLE}...")

        try:
            (
                df.write
                .format("jdbc")
                .option("url", JDBC_URL)
                .option("dbtable", TARGET_TABLE)
                .option("driver", "org.postgresql.Driver")
                .option("user", POSTGRES_USER)
                .option("password", POSTGRES_PASSWORD)
                .mode("append")
                .save()
            )
        except Exception as e:
            spark.stop()
            raise e

        print("Successfully loaded Parquet data from S3 into PostgreSQL!")
        spark.stop()

    load_s3_parquet_to_postgres()

cta_load_s3_parquet_to_postgres()