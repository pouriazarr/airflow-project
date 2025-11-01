import datetime as dt
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import os
import logging


default_args = {
        'owner': 'airflow',
        'start_date': dt.datetime(2025, 3, 3, 10, 00, 00),
        'concurrency': 1,
        'retries': 0
}

	
dag = DAG('curl_dump_dag',
        default_args=default_args,
        schedule_interval='@once',
	catchup=False,template_searchpath='/opt/airflow/scripts'
	)



def upload_json_to_s3_with_hook(**kwargs):
    s3_hook = S3Hook(aws_conn_id='arvan_s3_conn')
    
    source_dir = kwargs['source_dir']
    bucket_name = kwargs['bucket_name']
    s3_prefix = kwargs.get('s3_prefix', '')

    json_files = [f for f in os.listdir(source_dir) if f.lower().endswith('.json')]

    for file_name in json_files:
        file_path = os.path.join(source_dir, file_name)
        s3_key = os.path.join(s3_prefix, file_name).replace('\\', '/')
        s3_hook.load_file(
            filename=file_path,
            bucket_name=bucket_name,
            key=s3_key,
            replace=True
        )
        logging.info(f"Uploaded {s3_key}")





# DAG Definition
with DAG(
    dag_id='upload_json_to_arvan_s3',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['s3', 'arvan', 'json']
) as dag:

    send_json_to_s3 = PythonOperator(
        task_id='upload_json_files',
        python_callable=upload_json_to_s3_with_hook,
        op_kwargs={
            'bucket_name': 'jsonbucketsendedfor_save39423',
            'source_dir': '/tmp/stage/json',
            's3_prefix': 'raw/json/'                     # Optional
        },
        provide_context=True
    )

curl_and_dump_local = BashOperator(task_id='curl_and_dump_local', bash_command='parse-json-response', dag=dag)

curl_and_dump_local >> send_json_to_s3