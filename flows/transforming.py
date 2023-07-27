from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage
from pathlib import Path
import os
import re
from prefect import flow, task

project_id = 'de-learning-391102'
region = 'asia-southeast1'
cluster = 'de-learning-cluster'
pyspark_job_path = 'gs://ny_taxi_trip_de-learning-391102/pyspark_transforming_jobs/pyspark_dataproc_job.py'


@task(log_prints=True, retries=1, retry_delay_seconds=2)
def submit_pyspark_job(gcs_paths: list):
  # Create the job client.
  job_client = dataproc.JobControllerClient(
    client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
  )
  # Create the job config.
  job = {
    "placement": {"cluster_name": cluster},
    "pyspark_job": {
      "main_python_file_uri": pyspark_job_path,
      "args": [','.join(gcs_paths)],
      "jar_file_uris": ['gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.32.0.jar']
    },
  }

  operation = job_client.submit_job_as_operation(
    request={"project_id": project_id, "region": region, "job": job}
  )
  response = operation.result()
  # Dataproc job output gets saved to the Google Cloud Storage bucket
  # allocated to the job. Use a regex to obtain the bucket and blob info.
  matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)

  output = (
      storage.Client()
      .get_bucket(matches.group(1))
      .blob(f"{matches.group(2)}.000000000")
      .download_as_bytes()
      .decode("utf-8")
  )

  print(f"Job finished successfully: {output}")

@flow(name='submit pyspark job to dataproc')
def dataproc_submit_job_flow(gcs_paths: list):
  submit_pyspark_job(gcs_paths)

if __name__ == '__main__':
  #test
  gcs_paths = [
    'gs://ny_taxi_trip_de-learning-391102/taxi_trip_data/green_taxi/green_tripdata_2023-01.parquet',
    'gs://ny_taxi_trip_de-learning-391102/taxi_trip_data/yellow_taxi/yellow_tripdata_2023-01.parquet',
    'gs://ny_taxi_trip_de-learning-391102/taxi_trip_data/green_taxi/green_tripdata_2023-02.parquet',
    'gs://ny_taxi_trip_de-learning-391102/taxi_trip_data/yellow_taxi/yellow_tripdata_2023-02.parquet',
    'gs://ny_taxi_trip_de-learning-391102/taxi_trip_data/green_taxi/green_tripdata_2023-03.parquet',
    'gs://ny_taxi_trip_de-learning-391102/taxi_trip_data/yellow_taxi/yellow_tripdata_2023-03.parquet',
    'gs://ny_taxi_trip_de-learning-391102/taxi_trip_data/green_taxi/green_tripdata_2023-04.parquet',
    'gs://ny_taxi_trip_de-learning-391102/taxi_trip_data/yellow_taxi/yellow_tripdata_2023-04.parquet'
 ]
  dataproc_submit_job_flow(gcs_paths)