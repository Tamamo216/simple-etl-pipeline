from bs4 import BeautifulSoup
import requests
import re
from prefect import flow
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import cloud_storage_download_blob_as_bytes, cloud_storage_upload_blob_from_string

@flow(name='extract data link from website', log_prints=True, retries=2, retry_delay_seconds=2)
def extract_links(url: str) -> dict:
  response = requests.get(url)
  soup = BeautifulSoup(response.text, 'lxml')
  links_by_month = []
  result = {'return_links': [], 'first_run': False}
  run_times = 0
  bucket_name = 'ny_taxi_trip_de-learning-391102'
  blob_name = 'run_times.txt'
  try:
    gcp_creds = GcpCredentials.load('my-service-account-creds')
    content = cloud_storage_download_blob_as_bytes(
      bucket=bucket_name, blob=blob_name, gcp_credentials=gcp_creds
    )
    text = content.decode('utf-8')
    match = re.search(r'[\w\s]*:\s*(\d+)', text)
    if match:
      run_times = int(match.group(1))
  except Exception:
    cloud_storage_upload_blob_from_string(
      data=f'run times: {run_times}', bucket=bucket_name, blob=blob_name, gcp_credentials=gcp_creds
    )
    result['first_run'] = True
  for data_by_year in soup.find_all('div', class_='faq-answers', limit=2):
    for column in data_by_year.css.select('table tbody td'):
      for month in column.find_all('ul'):
        yellow_taxi_data = month.css.select('li > a')[0].get('href').strip()
        green_taxi_data = month.css.select('li > a')[1].get('href').strip()
        links_by_month.append((green_taxi_data, yellow_taxi_data))
  
  print(f'Extracted {2*len(result)} link')
  cloud_storage_upload_blob_from_string(
    data=f'run times: {run_times+1}', bucket=bucket_name, blob=blob_name, gcp_credentials=gcp_creds
  )
  if 2*run_times < len(links_by_month):
    result['return_links'].append(links_by_month[2*run_times])
  if 2*run_times+1 < len(links_by_month):
    result['return_links'].append(links_by_month[2*run_times+1])
  return result

if __name__ == '__main__':
  extract_links('https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page')