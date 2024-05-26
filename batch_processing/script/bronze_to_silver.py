from minio import Minio
from glob import glob
import os
from pyspark.sql import SparkSession

from helpers import load_cfg 

CFG_FILE = "config.yaml"
def main():
    cfg = load_cfg(CFG_FILE)
    lakehouse_cfg = cfg["minio_lakehouse"]
    raw_data_cfg = cfg["raw_data"]

    # Create a client with the MinIO server playground, its access key
    # and secret key.
    client = Minio(
        endpoint=lakehouse_cfg["endpoint"],
        access_key=lakehouse_cfg["access_key"],
        secret_key=lakehouse_cfg["secret_key"],
        secure=False,
    )

    # Create bucket if not exist.
    found = client.bucket_exists(bucket_name=lakehouse_cfg["bucket_name"])
    if not found:
        client.make_bucket(bucket_name=lakehouse_cfg["bucket_name"])
    else:
        print(f'Bucket {lakehouse_cfg["bucket_name"]} already exists, skip creating!')

    ## Spark session
    spark = (SparkSession.builder
         .appName("prestudy")
         .config("spark.hadoop.fs.s3a.access.key", lakehouse_cfg["access_key"])
         .config("spark.hadoop.fs.s3a.secret.key", lakehouse_cfg["secret_key"])
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
         .getOrCreate())
    
    # Read data
    df = spark.read.parquet(f"s3a://{lakehouse_cfg['bucket_name']}/{lakehouse_cfg['folder_name']}")

if __name__ == "__main__":
    main()