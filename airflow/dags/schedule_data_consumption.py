from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
sys.path.append('./wikimedia/')
from unzip_file import read_gzip_file
from base import WikimediaUrl

wiki = WikimediaUrl()
get_url = wiki.get_url()
current_url=wiki.fetch_filenames(get_url)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_download_task(api_endpoint):
    task_id = f'download_file_{str(api_endpoint).replace("/", "_")}'  
    download_file_task = SimpleHttpOperator(
        task_id=task_id,
        method='GET',
        endpoint=api_endpoint,
        response_check=None,
        response_filter=read_gzip_file,
        dag=dag,
    )
    return download_file_task

with DAG(
    'download_file_from_endpoint',
    default_args=default_args,
    description='A DAG to download a file from a given endpoint',
    schedule_interval='0 12 * * *',  # Daily at 12:00 PM
    catchup=False,
    start_date=days_ago(1),
    tags=['download from wikipedia'],
) as dag:
    for endpoint, date, size in current_url:
        print(endpoint)
        download_task=create_download_task(endpoint)

with DAG(
    'consume_kafka',
    default_args=default_args,
    description='Consume data from producer using spark',
    schedule_interval='3 12 * * *',  # Run daily at 12:03 PM
    start_date=days_ago(1),
    catchup=False,
    tags=['consume from kafka producer using spark'],
) as consume_kafka:
    # Task to execute the Python file after waiting for dag1
    run_spark_consumer = BashOperator(
        task_id='execute_python_script',
        bash_command="source ./wiki/bin/activate &&  python3 ./spark/stream_consumer.py",
        dag=consume_kafka,
    )
