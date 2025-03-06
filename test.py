# This script creates an Iceberg table, writes some data into it,
# and then compacts the table and removes the old snapshots.
#
# Make sure you run `pip install pyspark` before running this
# script and Nessie and MinIO are running (you can use the provided
# docker-compose.yml file to run them).
#
# Also make sure the Docker volume doesn't exists before running
# this script. You may need to run `docker volume rm <xxx>_data`.

from datetime import datetime
from os import environ
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import hours
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

# Set the MinIO credentials
environ["AWS_REGION"] = "us-east-1"
environ["AWS_ACCESS_KEY_ID"] = "admin"
environ["AWS_SECRET_ACCESS_KEY"] = "password"

catalog_name = 'cat'
schema_name = 'test'
table_name = 'app_logging'

spark = SparkSession.builder \
    .appName("example") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{catalog_name}.warehouse", "s3a://warehouse") \
    .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config(f"spark.sql.catalog.{catalog_name}.s3.endpoint", "http://localhost:9000") \
    .config(f"spark.sql.catalog.{catalog_name}.s3.path-style-access", "true") \
    .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config(f"spark.sql.catalog.{catalog_name}.uri", "http://localhost:19120/api/v1") \
    .config("iceberg.expire-snapshots.min-retention", "interval '0' second")  \
    .config("iceberg.remove-orphan-files.min-retention", "interval '0' second")  \
    .getOrCreate()

# Use INFO log level to get more information
spark.sparkContext.setLogLevel("INFO")

# Create Iceberg schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name} LOCATION 's3://warehouse/{catalog_name}'")

# Create Iceberg table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.{table_name} (
        timestamp   TIMESTAMP,
        application VARCHAR(30),
        machine     VARCHAR(30),
        message     VARCHAR(1000)
    )
    USING iceberg
    PARTITIONED BY (application, machine, date_hour(timestamp))
    TBLPROPERTIES (
          'write.metadata.metrics.default' = 'full',
          'gc.enabled' = 'true'
    )
""")

print(f"Created the '{table_name}' table (press ENTER to continue)")
input()

# Define schema
schema = StructType([
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("application", StringType(), nullable=False),
    StructField("machine", StringType(), nullable=False),
    StructField("message", StringType(), nullable=False),
])

# Write first batch of data
df = spark.createDataFrame([
    Row(datetime(2025, 2, 26, 13, 0, 1), 'test 1', 'machine1', 'This is an example log message'),	
    Row(datetime(2025, 2, 26, 13, 0, 2), 'test 1', 'machine2', 'This is an example log message'),	
    Row(datetime(2025, 2, 26, 13, 0, 3), 'test 1', 'machine1', 'This is an example log message'),	
    Row(datetime(2025, 2, 26, 13, 0, 4), 'test 1', 'machine2', 'This is an example log message'),	
    Row(datetime(2025, 2, 26, 13, 0, 5), 'test 2', 'machine1', 'This is an example log message'),	
    Row(datetime(2025, 2, 26, 13, 0, 6), 'test 2', 'machine2', 'This is an example log message'),	
    Row(datetime(2025, 2, 26, 13, 0, 7), 'test 2', 'machine1', 'This is an example log message'),	
    Row(datetime(2025, 2, 26, 13, 0, 8), 'test 2', 'machine2', 'This is an example log message'),	
], schema) \
    .writeTo(f"{catalog_name}.{schema_name}.{table_name}") \
    .using("iceberg") \
    .partitionedBy("application", "machine", hours("timestamp")) \
    .append()

print(f"Wrote some rows into the '{table_name}' table (press ENTER to continue)")
input()

# Write second batch of data
df = spark.createDataFrame([
    Row(datetime(2025, 2, 26, 13, 1, 1), 'test 1', 'machine1', 'This is an example log message'),	
    Row(datetime(2025, 2, 26, 13, 1, 2), 'test 1', 'machine2', 'This is an example log message'),	
    Row(datetime(2025, 2, 26, 13, 1, 3), 'test 1', 'machine1', 'This is an example log message'),	
    Row(datetime(2025, 2, 26, 13, 1, 4), 'test 1', 'machine2', 'This is an example log message'),	
    Row(datetime(2025, 2, 26, 13, 1, 5), 'test 2', 'machine1', 'This is an example log message'),	
    Row(datetime(2025, 2, 26, 13, 1, 6), 'test 2', 'machine2', 'This is an example log message'),	
    Row(datetime(2025, 2, 26, 13, 1, 7), 'test 2', 'machine1', 'This is an example log message'),	
    Row(datetime(2025, 2, 26, 13, 1, 8), 'test 2', 'machine2', 'This is an example log message'),	
], schema) \
    .writeTo(f"{catalog_name}.{schema_name}.{table_name}") \
    .using("iceberg") \
    .partitionedBy("application", "machine", hours("timestamp")) \
    .append()

print(f"Wrote some more rows into the '{table_name}' table (press ENTER to continue)")
input()

expire_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

spark.sql(f"CALL {catalog_name}.system.rewrite_data_files('{schema_name}.{table_name}')")
spark.sql(f"CALL {catalog_name}.system.expire_snapshots(table => '{schema_name}.{table_name}', older_than => TIMESTAMP '{expire_time}')")

# Removing orphaned files isn't possible, because:
#  1. minimum orphan file deletion time is hardcoded to minimum of 24 hours
#  2. https://github.com/apache/iceberg/issues/10539
#spark.sql(f"CALL {catalog_name}.system.remove_orphan_files(table => '{schema_name}.{table_name}', older_than => TIMESTAMP '{expire_time}')")

print("Compacted and removed shapshots (orphaned files can't be removed)")

spark.stop()
