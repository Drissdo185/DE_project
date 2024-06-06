from minio import Minio
from minio.error import S3Error
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
import io
from helpers import load_cfg

CFG_FILE = "config.yaml"

def create_minio_client(config):
    """Create and return a MinIO client."""
    return Minio(
        endpoint=config["endpoint"],
        access_key=config["access_key"],
        secret_key=config["secret_key"],
        secure=False
    )

def ensure_bucket_exists(client, bucket_name):
    """Ensure that the specified bucket exists."""
    if not client.bucket_exists(bucket_name=bucket_name):
        client.make_bucket(bucket_name=bucket_name)
    else:
        print(f'Bucket {bucket_name} already exists, skip creating!')

def create_folder_in_bucket(client, bucket_name, folder_name):
    """Create a folder in the specified bucket."""
    try:
        client.put_object(bucket_name, folder_name + "/", io.BytesIO(b''), 0)
        print(f'Folder {folder_name} created in bucket {bucket_name}')
    except S3Error as err:
        print(f'Error: {err}')

def create_spark_session(config):
    """Create and return a Spark session with the given configuration."""
    return ( SparkSession.builder.master("local[*]")
    .config(
        "spark.jars",
        "../jars/aws-java-sdk-bundle-1.11.375.jar,../jars/hadoop-aws-3.2.0.jar",
    )
    .config("spark.hadoop.fs.s3a.endpoint", f'http://localhost:9000')
    .config("spark.hadoop.fs.s3a.access.key", 'minio_access_key')
    .config("spark.hadoop.fs.s3a.secret.key", 'minio_secret_key')
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1")
    .config(
        "spark.jars.repositories",
        "https://maven-central.storage-download.googleapis.com/maven2/",
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.executor.memory", "18g")
    .config("spark.driver.memory", "5g")
    .appName("Python Spark transform Bronze to Silver")
    .getOrCreate())


def transform_data(df):
    """Perform data transformations on the DataFrame."""
    df = df.withColumn('default', when(df['default'] == 'yes', 1).otherwise(0))
    df = df.withColumn('housing', when(df['housing'] == 'yes', 1).otherwise(0))
    df = df.withColumn('loan', when(df['loan'] == 'yes', 1).otherwise(0))
    
    df = df.withColumn('marital', when(df['marital'] == 'divorced', 0)
                                    .when(df['marital'] == 'single', 1)
                                    .when(df['marital'] == 'married', 2)
                                    .otherwise(df['marital']))

    df = df.withColumn('education', when(df['education'] == 'unknown', 0)
                                        .when(df['education'] == 'primary', 1)
                                        .when(df['education'] == 'secondary', 2)
                                        .when(df['education'] == 'tertiary', 3)
                                        .otherwise(df['education']))

    df = df.withColumn('job', when(df['job'] == 'admin.', 0)
                            .when(df['job'] == 'unknown', 1)
                            .when(df['job'] == 'unemployed', 2)
                            .when(df['job'] == 'management', 3)
                            .when(df['job'] == 'housemaid', 4)
                            .when(df['job'] == 'entrepreneur', 5)
                            .when(df['job'] == 'student', 6)
                            .when(df['job'] == 'blue-collar', 7)
                            .when(df['job'] == 'self-employed', 8)
                            .when(df['job'] == 'retired', 9)
                            .when(df['job'] == 'technician', 10)
                            .when(df['job'] == 'services', 11)
                            .otherwise(df['job']))

    df = df.withColumn('contact', when(df['contact'] == 'unknown', 0)
                                .when(df['contact'] == 'telephone', 1)
                                .when(df['contact'] == 'cellular', 2)
                                .otherwise(df['contact']))

    df = df.withColumn('poutcome', when(df['poutcome'] == 'unknown', 0)
                                    .when(df['poutcome'] == 'other', 1)
                                    .when(df['poutcome'] == 'failure', 2)
                                    .when(df['poutcome'] == 'success', 3)
                                    .otherwise(df['poutcome']))

    return df

def main():
    # Load configuration
    cfg = load_cfg(CFG_FILE)
    lakehouse_cfg = cfg["minio_lakehouse"]
    datalake_cfg = cfg["minio_datalake"]

    # Create MinIO client and ensure bucket and folder exist
    client = create_minio_client(lakehouse_cfg)
    ensure_bucket_exists(client, lakehouse_cfg["bucket_name"])
    create_folder_in_bucket(client, lakehouse_cfg["bucket_name"], lakehouse_cfg["folder_name"])

    # Create Spark session
    spark = create_spark_session(datalake_cfg)

    # Read data from the bronze bucket
    bronze_file_path = f"s3a://bronze-data/raw_data"
    df = spark.read.parquet(bronze_file_path)
    df.show()

    # Transform data
    transformed_df = transform_data(df)
    transformed_df.show()

    # Write transformed data to the silver bucket in Delta Lake format
    silver_file_path = f"s3a://silver-data/transformed_data"
    transformed_df.write.format("delta").mode("overwrite").save(silver_file_path)

if __name__ == "__main__":
    main()
