from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from prefect.task_runners import ConcurrentTaskRunner
from prefect_dask import DaskTaskRunner
from google.cloud import bigquery
from timeit import default_timer as timer
from datetime import timedelta
import pandas as pd
from pathlib import Path
import os
from transforming import dataproc_submit_job_flow
from extract_data_link import extract_links

bq_client = bigquery.Client()

def get_gsutil_uri(file_path: str) -> str:
  gcs_bucket = GcsBucket.load('my-gcs-bucket')
  return f'gs://{gcs_bucket.bucket}/{file_path}'

@task(log_prints=True, retries=3, retry_delay_seconds=2)
def load_data(url: str) -> tuple[pd.DataFrame, str]:
  filename = url.split('/')[-1]
  file_format = filename.split('.')[-1]
  start = timer()
  if (file_format == 'parquet'):
    df = pd.read_parquet(url)
  elif (file_format == 'csv'):
    df = pd.read_csv(url)
  end = timer()
  print(f'Successfully loaded the data from file {filename}.\nTime execution: {round(end-start,2)}s')
  return df, filename

@task(log_prints=True, retries=3, retry_delay_seconds=2)
def upload_to_gcs(data_future) -> str:
  print('Uploading data file to Google Cloud Storage...')
  data, filename = data_future
  file_format = ''
  dest_folder = ''
  start = timer()
  gcs_bucket = GcsBucket.load('my-gcs-bucket')
  if '.csv' in filename:
    file_format = 'csv'
  else:
    file_format = 'parquet'
  
  if 'green' in filename:
    dest_folder = 'green_taxi'
  elif 'yellow' in filename:
    dest_folder = 'yellow_taxi'
  else:
    dest_folder = 'zone_lookup'
  
  upload_path = gcs_bucket.upload_from_dataframe(data, to_path=f'{dest_folder}/{filename}', serialization_format=file_format)
  end = timer()
  print(f'The data file ({filename}) has been successfully uploaded to GCS. Time execution: {round(end-start,2)}s')
  return upload_path

@flow(name='Extract and load data into GCS', retries=2, log_prints=True, task_runner=ConcurrentTaskRunner())
def extract_load_to_gcs(urls: list) -> list:
  results = []
  for url1, url2 in urls:
    data_future = load_data.submit(url1)
    gcs_path_future1 = upload_to_gcs.submit(data_future)
    data_future = load_data.submit(url2)
    gcs_path_future2 = upload_to_gcs.submit(data_future)
    results.extend([get_gsutil_uri(gcs_path_future1.result()),get_gsutil_uri(gcs_path_future2.result())])
  return results

@flow(retries=2, name='ETL to Big Query', log_prints=True)
def main_flow():
  info = extract_links('https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page')
  if len(info['return_links']) == 0:
    print('There is no available link to extract')
    return None
  if info['first_run']:
    zone_lookup_data_url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv'
    zone_lookup_future = load_data(zone_lookup_data_url)
    upload_to_gcs(zone_lookup_future)
  gcs_paths = extract_load_to_gcs(info['return_links'])
  dataproc_submit_job_flow(gcs_paths)

if __name__ == '__main__':
  main_flow()