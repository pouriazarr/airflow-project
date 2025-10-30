from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin
from elasticsearch import Elasticsearch

class ElasticsearchHook(BaseHook, LoggingMixin):
    def __init__(self, elasticsearch_conn_id='elasticsearch_default'):
        conn = self.get_connection(elasticsearch_conn_id)
        
        conn_config = {}
        hosts = []

        # Building the hosts list with URL format
        scheme = conn.extra_dejson.get('scheme', 'http')
        if conn.host:
            hosts = [f"{scheme}://{host}:{conn.port}" for host in conn.host.split(',')]

        if conn.login:
            conn_config['http_auth'] = (conn.login, conn.password)

        # Initialize Elasticsearch client with hosts and configuration
        self.es = Elasticsearch(hosts, **conn_config)
        self.index = conn.schema

    def get_conn(self):
        return self.es

    def get_index(self):
        return self.index

    def set_index(self, index):
        self.index = index

    def search(self, index, body):
        self.set_index(index)
        res = self.es.search(index=self.index, body=body)
        return res

    def create_index(self, index, body):
        self.set_index(index)
        res = self.es.indices.create(index=self.index, body=body)
        return res

    def add_doc(self, index, doc):
        self.set_index(index)
        res = self.es.index(index=index, body=doc)
        return res

    def info(self):
        return self.es.info()

    def ping(self):
        return self.es.ping()

