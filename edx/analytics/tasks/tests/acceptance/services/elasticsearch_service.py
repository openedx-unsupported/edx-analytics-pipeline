import logging

from elasticsearch.exceptions import ElasticsearchException, TransportError

try:
    import boto3
    import elasticsearch
    from elasticsearch import Elasticsearch, RequestsHttpConnection
    from elasticsearch.connection import Urllib3HttpConnection
    from requests_aws4auth import AWS4Auth
except ImportError:
    elasticsearch = None

logger = logging.getLogger(__name__)


class ElasticsearchService(object):

    def __init__(self, config, alias):
        if config.get('elasticsearch_connection_class') == 'aws':
            connection_class = RequestsHttpConnection
        else:
            connection_class = Urllib3HttpConnection

        self._disabled = not bool(config.get('elasticsearch_host'))
        self._alias = alias
        if not self._disabled:
            if connection_class == RequestsHttpConnection:
                service = 'es'
                region = 'us-east-1'
                credentials = boto3.Session().get_credentials()
                awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
                self._elasticsearch_client = Elasticsearch(
                    hosts=[{'host': config['elasticsearch_host'], 'port': 443}],
                    http_auth=awsauth,
                    use_ssl=True,
                    verify_certs=True,
                    connection_class=RequestsHttpConnection
                )
            elif connection_class == Urllib3HttpConnection:
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

        try:
            logger.info("ElasticsearchService: {}".format(str(self._alias)))
            response = self._elasticsearch_client.indices.get_alias(name=self._alias, ignore_unavailable=True)
        except TransportError as ex:
            logger.error("Elasticsearch transport error while getting index by alias: %r", ex)
            raise ex
        except ElasticsearchException as ex:
            logger.error("Elasticsearch error while getting index by alias: %r", ex)
            raise ex
        if response:
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
                self._elasticsearch_client.delete(index='index_updates', id=doc['_id'])
