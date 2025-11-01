# Airflow Project :

This end-to-end data pipeline fetches real-time tweets mentioning stock tickers using the Sahamyab API , processes them with Apache Airflow, indexes structured data into Elasticsearch and queries for find some specefic words in tweets, 
and persists raw & processed data in JSON + Parquet formats to AbrArvan S3 Buckets.
The entire stack is fully containerized with Docker and orchestrated via Airflow DAGs for reliability, scheduling, and monitoring.
<div style="text-align: center;">
  <img src="images/mainDiagram.png" alt="diagram" style="max-width: 100%; height: auto;" />
  <p></p>
  <p></p>
  Dags list :
  <p></p>
  <img src="images/airflow dags.png" alt="dags" style="max-width: 100%; height: auto;" />
    
</div>
