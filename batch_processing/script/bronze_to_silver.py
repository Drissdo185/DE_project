from minio import Minio
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer
from minio.error import S3Error
from pyspark.sql.functions import when
import io
from helpers import load_cfg 

CFG_FILE = "config.yaml"

def main():
    cfg = load_cfg(CFG_FILE)
    lakehouse_cfg = cfg["minio_lakehouse"]
    datalake_cfg = cfg["minio_datalake"]

    # Create a client with the MinIO server playground, its access key and secret key.
    client = Minio(
        endpoint=lakehouse_cfg["endpoint"],
        access_key=lakehouse_cfg["access_key"],
        secret_key=lakehouse_cfg["secret_key"],
        secure=False,
    )

   # Create a bucket if it doesn't exist
    found = client.bucket_exists(bucket_name=lakehouse_cfg["bucket_name"])
    if not found:
        client.make_bucket(bucket_name=lakehouse_cfg["bucket_name"])
    else:
        print(f'Bucket {lakehouse_cfg["bucket_name"]} already exists, skip creating!')

    # Create a folder in the bucket
    folder_name = lakehouse_cfg["folder_name"] + "/"
    try:
        client.put_object(lakehouse_cfg["bucket_name"], folder_name, io.BytesIO(b''), 0)
        print(f'Folder {folder_name} created in bucket {lakehouse_cfg["bucket_name"]}')
    except S3Error as err:
        print(f'Error: {err}')

    # Spark session
    spark = (SparkSession.builder.master("local")
             .appName("Bronze to Silver")
             .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.375")
             .config("spark.hadoop.fs.s3a.access.key", "minio_access_key")
             .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key")
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
             .getOrCreate())

    # Read data
    df = spark.read.parquet(f"s3a://bronze-data/raw_data")
    df.show()


    # Transform data
    # Label encoding with category columns
    # With yes or no columns
    df = df.withColumn('default', when(df['default'] == 'yes', 1).otherwise(0))
    df = df.withColumn('housing', when(df['housing'] == 'yes', 1).otherwise(0))
    df = df.withColumn('loan', when(df['loan'] == 'yes', 1).otherwise(0))
    # With other category columns
    # Replace 'divorced' with '0', 'single' with '1', and 'married' with '2'
    # in the 'marital' column
    df = df.withColumn('marital', when(df['marital'] == 'divorced', 0)
                                    .when(df['marital'] == 'single', 1)
                                    .when(df['marital'] == 'married', 2)
                                    .otherwise(df['marital']))

    # Replace 'unknown' with '0', 'primary' with '1', 'secondary' with '2', 
    # and 'tertiary' with '3' in the 'education' column
    df = df.withColumn('education', when(df['education'] == 'unknown', 0).
                                        when(df['education'] == 'primary', 1).
                                        when(df['education'] == 'secondary', 2).
                                        when(df['education'] == 'tertiary', 3).
                                        otherwise(df['education']))

    # Replace job categories with numerical values in the 'job' column
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

    # Replace 'unknown' with '0', 'telephone' with '1', and 'cellular' with '2'
    # in the 'contact' column
    df = df.withColumn('contact', when(df['contact'] == 'unknown', 0)
                                .when(df['contact'] == 'telephone', 1)
                                .when(df['contact'] == 'cellular', 2)
                                .otherwise(df['contact']))

    # Replace 'unknown' with '0', 'other' with '1', 'failure' with '2', 
    # and 'success' with '3' in the 'poutcome' column
    df = df.withColumn('poutcome', when(df['poutcome'] == 'unknown', 0)
                                    .when(df['poutcome'] == 'other', 1)
                                    .when(df['poutcome'] == 'failure', 2)
                                    .when(df['poutcome'] == 'success', 3)
                                    .otherwise(df['poutcome']))

    # Set data type
    
    # Write delta table
    df.write.format("delta").mode("overwrite").save(f"s3://{lakehouse_cfg['bucket_name']}/transformed_data")


    
    
    

if __name__ == "__main__":
    main()
