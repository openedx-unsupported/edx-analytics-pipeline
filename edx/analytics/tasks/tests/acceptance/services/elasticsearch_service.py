try:
    import elasticsearch
    from elasticsearch.connection import Urllib3HttpConnection
except ImportError:
    elasticsearch = None

from edx.analytics.tasks.util.aws_elasticsearch_connection import AwsHttpConnection


class ElasticsearchService(object):

    def __init__(self, config, alias):
        if config.get('elasticsearch_connection_class') == 'aws':
            connection_class = AwsHttpConnection
        else:
            connection_class = Urllib3HttpConnection

        self._disabled = not bool(config.get('elasticsearch_host'))
        self._alias = alias
        if not self._disabled:
            self._elasticsearch_client = elasticsearch.Elasticsearch(hosts=[config['elasticsearch_host']], connection_class=connection_class)
        else:
            self._elasticsearch_client = None

    @property
    def client(self):
        return self._elasticsearch_client

    @property
    def alias(self):
        return self._alias

    def reset(self):
        if self._disabled:
            return

        response = self._elasticsearch_client.indices.get_aliases(name=self._alias)
        for index, alias_info in response.iteritems():
            for alias in alias_info['aliases'].keys():
                if alias == self._alias:
                    self._elasticsearch_client.indices.delete(index=index)

        # Get documents from the marker index which have their target_index set to current alias.
        # Note that there should be only 1 marker document per test run.
        if self._elasticsearch_client.indices.exists(index='index_updates'):
            query = {"query": {"match": {"target_index": self._alias}}}
            response = self._elasticsearch_client.search(index='index_updates', body=query)

            for doc in response['hits']['hits']:
                self._elasticsearch_client.delete(index='index_updates', doc_type='marker', id=doc['_id'])
