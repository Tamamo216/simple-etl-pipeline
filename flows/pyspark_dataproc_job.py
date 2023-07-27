from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import when, col
from pyspark.sql.types import TimestampType
import sys

def load_file_from_gcs(gcs_filepath: str, spark: SparkSession) -> tuple[DataFrame, str]: 
    df = spark.read.parquet(gcs_filepath)
    taxi_color = 'green' if 'green' in gcs_filepath else 'yellow'
    return df, taxi_color

def data_transform(df: DataFrame, taxi_color: str) -> DataFrame:
    #Drop duplicated rows
    df = df.dropDuplicates()
    #Drop 'ehail_fee' column for green taxi trips data since this column usually has almost NA values
    if (taxi_color == 'green'):
        df = df.drop('ehail_fee')
    #Drop rows having NA values
    df = df.dropna()
    #Cast from timestamp_ntz type to timestamp type to not get error when writing dataframe to Big Query
    if (taxi_color == 'green'):
        df = df.withColumnRenamed('lpep_pickup_datetime','pickup_datetime')\
        .withColumnRenamed('lpep_dropoff_datetime','dropoff_datetime')
    elif (taxi_color == 'yellow'):
        df = df.withColumnRenamed('tpep_pickup_datetime','pickup_datetime')\
        .withColumnRenamed('tpep_dropoff_datetime','dropoff_datetime')
    df = df.withColumn('pickup_datetime', col('pickup_datetime').cast(TimestampType())).withColumn('dropoff_datetime', col('dropoff_datetime').cast(TimestampType()))
    #Replace negative values to 0 for numerical columns
    numeric_types = ['int', 'bigint', 'float', 'double']
    df = df.select([when(col(c) < 0, 0).otherwise(col(c)).alias(c) if t in numeric_types else col(c) for c,t in df.dtypes])
    return df

def write_data_to_bq(data: DataFrame, taxi_color: str):
    target_table = 'green_taxi_tripdata_partitioned' if taxi_color == 'green' else 'yellow_taxi_tripdata_partitioned'
    data.write.format('bigquery')\
        .option('table', f'trips_data_all.{target_table}')\
        .option("temporaryGcsBucket","dataproc-temp-asia-southeast1-95827859007-bhzkey3w") \
        .option("partitionType", "HOUR")\
        .mode('append')\
        .save()

def main_task(gcs_paths: list):
    spark = SparkSession.builder \
        .appName('spark-dataproc-job') \
        .getOrCreate()
    for gcs_path in gcs_paths:
        df, taxi_color = load_file_from_gcs(gcs_path, spark)
        data = data_transform(df, taxi_color)
        write_data_to_bq(data, taxi_color)
    spark.stop()

if __name__ == '__main__':
    gcs_paths = sys.argv[1].split(',')
    main_task(gcs_paths)