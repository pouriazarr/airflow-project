import datetime as dt
import json
import csv
import logging
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from elasticsearch_plugin.hooks.elasticsearch_hook import ElasticsearchHook

ES_CONN_ID = "elasticsearch_default"
INDEX_NAME = "sahamyab"
QUERY_BODY = {
    "query": {
        "term": {
            "content": {"value": "سهم"}
        }
    }
}

OUTPUT_DIR = Path("/tmp/es_exports")
OUTPUT_JSON = OUTPUT_DIR / f"tweets_{{ ds_nodash }}.json"
OUTPUT_CSV  = OUTPUT_DIR / f"tweets_{{ ds_nodash }}.csv"

default_args = {
    "owner": "airflow",
    "start_date": dt.datetime(2025, 3, 3, 10, 0),
    "retries": 0,
}


#DAG
with DAG(
    dag_id="es_query_save_local",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
    tags=["elasticsearch", "export"],
) as dag:
    
	def query_and_save(**context):

		es_hook = ElasticsearchHook(elasticsearch_conn_id=ES_CONN_ID)
		logging.info("ES cluster info: %s", es_hook.info())

		response = es_hook.search(index=INDEX_NAME, body=QUERY_BODY)
		hits = response.get("hits", {}).get("hits", [])
		logging.info("Got %d documents", len(hits))

		OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
		ds_nodash = context["ds_nodash"]
		json_path = str(OUTPUT_JSON).replace("{{ ds_nodash }}", ds_nodash)
		csv_path = str(OUTPUT_CSV).replace("{{ ds_nodash }}", ds_nodash)

		# --- Save JSON (only serializable parts) ---
		serializable_response = {
			"took": response.get("took"),
			"timed_out": response.get("timed_out"),
			"_shards": response.get("_shards"),
			"hits": {
				"total": response["hits"].get("total"),
				"max_score": response["hits"].get("max_score"),
				"hits": [
					{
						"_index": hit.get("_index"),
						"_id": hit.get("_id"),
						"_score": hit.get("_score"),
						"_source": hit.get("_source"),
					}
					for hit in hits
				]
			}
		}

		with open(json_path, "w", encoding="utf-8") as f:
			json.dump(serializable_response, f, ensure_ascii=False, indent=2)
		logging.info("JSON written to %s", json_path)

		# --- Save CSV ---
		if hits:
			fieldnames = hits[0]["_source"].keys()
			with open(csv_path, "w", newline="", encoding="utf-8") as f:
				writer = csv.DictWriter(f, fieldnames=fieldnames)
				writer.writeheader()
				for hit in hits:
					writer.writerow(hit["_source"])
			logging.info("CSV written to %s", csv_path)
		else:
			Path(csv_path).touch()
			logging.info("Empty CSV created at %s", csv_path)

		return {"json": json_path, "csv": csv_path}


	query_task = PythonOperator(
        task_id="query_and_save",
        python_callable=query_and_save,
        provide_context=True,
    )

	end_task = BashOperator(
        task_id="done",
        bash_command='echo "Export finished – check {{ ti.xcom_pull(task_ids=\"query_and_save\")[\"json\"] }}"',
    )

	query_task >> end_task