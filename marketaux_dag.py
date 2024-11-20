from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import json
import http.client, urllib.parse
import pandas as pd
import os


def fetch_marketaux_data(api_key, industry):
    """
    Fetch data from MarketAux API using the given API key and industry filter.
    """
    conn = http.client.HTTPSConnection('api.marketaux.com')
    params = urllib.parse.urlencode({
        'sentiment_gte': -1,
        'filter_entities': 'true',
        'industries': industry,  # Filter by industry
        'language': 'en',
        'api_token': api_key,
    })
    conn.request('GET', '/v1/news/all?{}'.format(params))
    res = conn.getresponse()
    data = res.read()
    return json.loads(data.decode('utf-8'))


def marketaux_etl_run():
    """
    Fetch data from MarketAux API using multiple API keys for different industries,
    transform it, and save it as separate CSV files in two different local directories.
    """

    # API keys and their corresponding industries
     # API keys and their corresponding industries
    api_config = [
        {'api_key': "", 'industry': 'Technology'}, 
        {'api_key': "", 'industry': 'Industrials'}, 
        {'api_key': "", 'industry': 'Services'}, 
        {'api_key': "", 'industry': 'Communication Services'} 
    ]

    # Fetch data for each API key and industry
    all_data = []
    for config in api_config:
        json_data = fetch_marketaux_data(config['api_key'], config['industry'])
        for article in json_data.get('data', []):
            first_entity = article.get('entities', [{}])[0]
            all_data.append({
                'uuid': article.get('uuid'),
                'title': article.get('title'),
                'description': article.get('description'),
                'url': article.get('url'),
                'image': article.get('image_url'),
                'published_at': article.get('published_at'),
                'entities_name': first_entity.get('name'),
                'entities_symbol': first_entity.get('symbol'),
                'entities_country': first_entity.get('country'),
                'entities_type': first_entity.get('type'),
                'entities_industry': first_entity.get('industry'),
                'entities_match_score': first_entity.get('match_score'),
                'entities_sentiment_score': first_entity.get('sentiment_score')
            })

    # Convert all data to a DataFrame
    df = pd.DataFrame(all_data)

    # Define storage directories
    storage_dir_1 = '/home/azureuser/all_csv'
    storage_dir_2 = '/home/azureuser/append_csv'
    os.makedirs(storage_dir_1, exist_ok=True)
    os.makedirs(storage_dir_2, exist_ok=True)

    # Save data to two different CSV files
    file_1 = os.path.join(storage_dir_1, 'marketaux_data.csv')
    file_2 = os.path.join(storage_dir_2, 'marketaux_data.csv')
    df.to_csv(file_1, index=False, encoding='utf-8')
    df.to_csv(file_2, index=False, encoding='utf-8')

    print(f"ETL completed. CSV files saved in:\n  {file_1}\n  {file_2}")
    return file_1, file_2



def upload_to_s3(file_name, folder_name, unique_name=True):
    """
    Upload file CSV ke S3 bucket.
    Jika `unique_name` adalah False, nama file di S3 akan tetap dan file di S3 akan ditimpa.
    """
    # AWS credentials dan bucket name
    aws_access_key = ""
    aws_secret_key = ""
    bucket_name = ""

    # Membuat nama file untuk S3
    if unique_name:
        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%S')
        s3_file_name = f"{folder_name}/{os.path.basename(file_name).split('.')[0]}_{timestamp}.csv"
    else:
        # Nama tetap di folder yang ditentukan
        s3_file_name = f"{folder_name}/{os.path.basename(file_name)}"

    # S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )

    # Upload file ke S3
    s3_client.upload_file(file_name, bucket_name, s3_file_name)
    print(f"File {file_name} uploaded to S3 bucket {bucket_name} as {s3_file_name}")
    return s3_file_name



# Default arguments untuk DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['s4aip.hm9@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 11, 19),
}

# DAG definition
with DAG(
    'marketaux_etl_dag',
    default_args=default_args,
    schedule_interval='*/20 * * * *',  # Setiap 20 menit
    catchup=False
) as dag:

    # Task 1: Menjalankan ETL
    run_etl = PythonOperator(
        task_id='run_etl',
        python_callable=marketaux_etl_run,
    )

    # Task 2: Upload file dari all_csv dengan nama unik
    upload_all_csv = PythonOperator(
        task_id='upload_all_csv',
        python_callable=upload_to_s3,
        op_args=['/home/azureuser/all_csv/marketaux_data.csv', 'all_csv'],  # Path file lokal dan folder S3
        op_kwargs={'unique_name': True},  # Nama file unik
    )

    # Task 3: Upload file dari append_csv dengan nama tetap
    upload_append_csv = PythonOperator(
        task_id='upload_append_csv',
        python_callable=upload_to_s3,
        op_args=['/home/azureuser/append_csv/marketaux_data.csv', 'append_csv'],  # Path file lokal dan folder S3
        op_kwargs={'unique_name': False},  # Nama file tetap
    )

    # Alur tugas
    run_etl >> [upload_all_csv, upload_append_csv]


