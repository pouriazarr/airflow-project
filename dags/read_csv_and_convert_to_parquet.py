import datetime as dt
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import glob


default_args = {
        'owner': 'airflow',
        'start_date': dt.datetime(2025, 3, 3, 10, 00, 00),
        'concurrency': 1,
        'retries': 0
}

STAGE2="/tmp/stage/csv"
LAKE="/tmp/stage/parquet"

def convert_files_to_parquet(**kwargs): 
    flist= glob.glob(f"{STAGE2}/*.csv")
    for i in flist :
        df = pd.read_csv(i, header=None , names=['id','sendTime','sendTimePersian', 'senderName', 'senderUsername', 'type', 'content'],dtype={'content': object})
        df.to_parquet( f"{LAKE}/{i.split('/')[-1].split('.')[0]}.parquet")


dag = DAG('curl_csv_tweets_dump_dag',
        default_args=default_args,
        schedule_interval='@once',
	catchup=False,template_searchpath='/opt/airflow/scripts'
	)

get_csv_tweets = BashOperator(
    task_id='Get-CSV-Tweets',
    bash_command= "curl -s -H 'User-Agent:Chrome/133.0' https://www.sahamyab.com/guest/twiter/list?v=0.1 | "
                  "jq \'.items[] | [.id, .sendTime, .sendTimePersian, .senderName, "
                  ".senderUsername, .type, .content] | join(\",\") \' > /tmp/stage/csv/$(date +%s).csv" ,
    dag=dag,
)


convert_to_parquet = PythonOperator(task_id='convert_to_parquet', python_callable=convert_files_to_parquet, dag=dag)


get_csv_tweets >> convert_to_parquet