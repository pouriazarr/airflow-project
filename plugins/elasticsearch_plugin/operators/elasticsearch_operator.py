import json
import MySQLdb
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.mysql_hook import MySqlHook
from elasticsearch_plugin.hooks.elasticsearch_hook import ElasticsearchHook

class MySqlToElasticsearchTransfer(BaseOperator):
    """
    Moves data from MySQL to Elasticsearch.
    
    :param sql: SQL query to execute against MySQL.
    :type sql: str

    :param index: Index where to save the data into Elasticsearch
    :type index: str

    :param mysql_conn_id: source MySQL connection
    :type mysql_conn_id: str

    :param elasticsearch_conn_id: source Elasticsearch connection
    :type elasticsearch_conn_id: str
    """
    
    @apply_defaults
    def __init__(self, sql, index, mysql_conn_id='mysql_default', elasticsearch_conn_id='elasticsearch_default', *args, **kwargs):
        super(MySqlToElasticsearchTransfer, self).__init__(*args, **kwargs)
        self.sql = sql
        self.index = index
        self.mysql_conn_id = mysql_conn_id
        self.elasticsearch_conn_id = elasticsearch_conn_id

    def execute(self, context):
        mysql = MySqlHook(mysql_conn_id=self.mysql_conn_id).get_conn()
        es = ElasticsearchHook(elasticsearch_conn_id=self.elasticsearch_conn_id)

        self.log.info("Extracting data from MySQL: %s", self.sql)

        with MySQLdb.cursors.DictCursor(mysql) as mysql_cursor:
            mysql_cursor.execute(self.sql)
            for row in mysql_cursor:
                doc_id = row['id']  # Assuming 'id' is the primary key
                doc = json.loads(json.dumps(row))  # Convert to a dictionary
                es.add_doc(index=self.index, doc=doc, doc_id=doc_id)  # Ensure doc_id is used correctly