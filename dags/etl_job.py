import os
import tempfile
import boto3
import pendulum
from airflow.decorators import dag, task
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import col, explode, to_timestamp, year, month, dayofmonth, hour, minute
from pyspark.sql.types import IntegerType
from urllib.parse import urlparse

BUCKET_NAME = os.getenv("S3_BUCKET_ARN")

@dag(
    dag_id="cta_vehicle_data_etl",
    schedule=None,
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    tags=["production"]
)
def cta_vehicle_data_etl():

    @task(retries=1)
    def get_missing_path_list():
        session = boto3.Session(profile_name="data-eng")
        s3_client = session.client("s3")
        paginator = s3_client.get_paginator("list_objects_v2")

        raw_keys = set()
        for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix="raw/transit/vehicles/"):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith("vehicles.json"):
                    raw_keys.add(obj["Key"])

        done_keys = set()
        for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix="processed/_checkpoints/"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".done"):
                    done_keys.add(
                        key.replace("processed/_checkpoints/", "raw/transit/vehicles/").replace(".done", "/vehicles.json")
                    )

        missing = sorted(raw_keys - done_keys)
        return [f"s3a://{BUCKET_NAME}/{key}" for key in missing]

    @task(retries=1)
    def run_spark_etl(target_paths):
        session = boto3.Session(profile_name="data-eng")
        s3_client = session.client("s3")
        spark = (
            SparkSession.builder.appName("CTAVehicleETL")
            .master("local[*]")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.jars.ivy", tempfile.mkdtemp(prefix="ivy-"))
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
            .getOrCreate()
        )

        for target_slot in target_paths:
            parsed_url = urlparse(target_slot)
            s3_bucket = parsed_url.netloc
            s3_key = parsed_url.path.lstrip("/")
            checkpoint_key = s3_key.replace("raw/transit/vehicles/", "processed/_checkpoints/") \
                            .replace("/vehicles.json", ".done")

            try:
                df = spark.read.json(target_slot)
            except Exception as e:
                spark.stop()
                raise e

            df_flat = df.select(
                explode(col("`bustime-response`.vehicle")).alias("vehicle")
            ).select(
                col("vehicle.vid").cast(IntegerType()).alias("vehicle_id"),
                col("vehicle.rt").alias("route"),
                col("vehicle.des").alias("destination"),
                col("vehicle.lat").cast("double").alias("latitude"),
                col("vehicle.lon").cast("double").alias("longitude"),
                col("vehicle.dly").alias("is_delayed"),
                to_timestamp(col("vehicle.tmstmp"), "yyyyMMdd HH:mm").alias("event_timestamp")
            )

            df_flat = df_flat.withColumn("year", year("event_timestamp")) \
                    .withColumn("month", month("event_timestamp")) \
                    .withColumn("day", dayofmonth("event_timestamp")) \
                    .withColumn("hour", hour("event_timestamp")) \
                    .withColumn("minute", minute("event_timestamp"))

            target_partitions = (
                df_flat.select("year", "month", "day", "hour")
                .distinct()
                .collect()
            )

            if not target_partitions:
                print(f"Skipping: {target_slot} has no vehicle records.")
                continue

            filter_conditions = None
            for row in target_partitions:
                cond = (
                    (col("year") == int(row["year"])) &
                    (col("month") == int(row["month"])) &
                    (col("day") == int(row["day"])) &
                    (col("hour") == int(row["hour"]))
                )
                filter_conditions = cond if filter_conditions is None else (filter_conditions | cond)

            s3_processed_path = f"s3a://{BUCKET_NAME}/processed/transit/vehicles/"
            try:
                df_existing = spark.read.parquet(s3_processed_path).filter(filter_conditions)
            except AnalysisException:
                df_existing = None
            except Exception as e:
                spark.stop()
                raise e

            try:
                if df_existing is not None and not df_existing.isEmpty():
                    df_existing = df_existing \
                                .withColumn("vehicle_id", col("vehicle_id").cast(IntegerType())) \
                                .withColumn("year", col("year").cast(IntegerType())) \
                                .withColumn("month", col("month").cast(IntegerType())) \
                                .withColumn("day", col("day").cast(IntegerType())) \
                                .withColumn("hour", col("hour").cast(IntegerType()))

                    df_combined = df_existing.unionByName(df_flat, allowMissingColumns=True)
                else:
                    df_combined = df_flat

                df_final = df_combined.dropDuplicates(["vehicle_id", "event_timestamp"])

                (
                    df_final.sortWithinPartitions("minute")
                    .write
                    .mode("overwrite")
                    .partitionBy("year", "month", "day", "hour", "route")
                    .parquet(s3_processed_path)
                )
                s3_client.put_object(Bucket=s3_bucket, Key=checkpoint_key, Body=b"")
                print(f"Processed: {target_slot}")
            except Exception as e:
                spark.stop()
                raise e
        spark.stop()

    run_spark_etl(get_missing_path_list())
    # In real production setting use .expand() to parallel process
    # I was using local setup limited in task nodes

cta_vehicle_data_etl()