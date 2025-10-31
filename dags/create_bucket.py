import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import boto3
import logging
from botocore.exceptions import ClientError
import json
from airflow.hooks.base import BaseHook



default_args = {
        'owner': 'airflow',
        'start_date': dt.datetime(2025, 3, 3, 10, 00, 00),
        'concurrency': 1,
        'retries': 0
}

def create_bucket(bucket_name='dumped_bucket',**kwargs):    
    try:
        # Retrieve connection details from Airflow
        conn = BaseHook.get_connection('arvan_s3_conn')
        access_key = conn.login
        secret_key = conn.password
        extra = json.loads(conn.extra or '{}')
        endpoint_url = extra.get('endpoint_url')

        # Initialize boto3 S3 resource
        s3_resource = boto3.resource(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )

        # Create the bucket
        bucket = s3_resource.Bucket(bucket_name)
        bucket.create(ACL='public-read')
        logging.info(f"Successfully created bucket: {bucket_name}")
        return True

    except json.JSONDecodeError as exc:
        logging.error(f"Failed to parse connection extra: {exc}")
        return False
    except ClientError as exc:
        logging.error(f"Failed to create bucket {bucket_name}: {exc}")
        return False
    except Exception as exc:
        logging.error(f"Unexpected error while creating bucket {bucket_name}: {exc}")
        return False




dag = DAG('plugin_hook_dag',
        default_args=default_args,
        schedule_interval='@once',
	catchup=False
	)

create_bucket = PythonOperator(task_id='create_bucket', python_callable=create_bucket, dag=dag)

create_bucket