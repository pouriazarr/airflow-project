import datetime as dt
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from elasticsearch_plugin.hooks.elasticsearch_hook import ElasticsearchHook

default_args = {
        'owner': 'airflow',
        'start_date': dt.datetime(2025, 3, 3, 10, 00, 00),
        'concurrency': 1,
        'retries': 0
}

def query_elastic():
	es_hook = ElasticsearchHook()
	print(es_hook.info())
	print(es_hook.search(index='tweets',body=
		{ 
		"query": 
			{   "term":
					{  "content": 
						{   "value": "داده"   }     
					}   
			} 
		}))
	
dag = DAG('plugin_hook_dag',
        default_args=default_args,
        schedule_interval='@once',
	catchup=False
	)

hook_es = PythonOperator(task_id='hook_es', python_callable=query_elastic, dag=dag)

opr_end = BashOperator(task_id='opr_end', bash_command='echo "Done"', dag=dag)

hook_es >> opr_end