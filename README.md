# Airflow Project :

This end-to-end data pipeline fetches real-time tweets mentioning stock tickers using the Sahamyab API, processes them with Apache Airflow, indexes the structured data into Elasticsearch for querying specific words in tweets, and persists both raw and processed data in JSON and Parquet formats to ArvanCloud S3 buckets. The entire stack is fully containerized with Docker and orchestrated via Airflow DAGs for reliability, scheduling, and monitoring.
<div style="text-align: center;">
  <img src="images/mainDiagram.png" alt="diagram" style="max-width: 100%; height: auto;" />
  <p></p>
  <p></p>
  Dags list :
  <p></p>
  <img src="images/airflow dags.png" alt="dags" style="max-width: 100%; height: auto;" />
    
</div>
