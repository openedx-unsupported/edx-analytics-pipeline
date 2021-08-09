"""Load records into elasticsearch clusters."""

import json
import logging
import random
import time
from itertools import islice

import luigi

from edx.analytics.tasks.common.mapreduce import MapReduceJobTask
from edx.analytics.tasks.util.elasticsearch_target import ElasticsearchTarget
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin

try:
    import boto3
    import elasticsearch
    import elasticsearch.helpers
    import requests_aws4auth
    from elasticsearch import RequestsHttpConnection, compat, exceptions, serializer
    from elasticsearch.exceptions import TransportError
except ImportError:
    elasticsearch = None


log = logging.getLogger(__name__)


# These are standard HTTP status codes used by elasticsearch to represent various error conditions
HTTP_CONNECT_TIMEOUT_STATUS_CODE = 408
REJECTED_REQUEST_STATUS = 429
HTTP_SERVICE_UNAVAILABLE_STATUS_CODE = 503
HTTP_GATEWAY_TIMEOUT_STATUS_CODE = 504


if elasticsearch:
    class JSONSerializerPython2(serializer.JSONSerializer):
        def dumps(self, data):
            # don't serialize strings
            if isinstance(data, compat.string_types):
                return data
            try:
                return json.dumps(data, default=self.default)
            except (ValueError, TypeError) as e:
                raise exceptions.SerializationError(data, e)


class ElasticsearchIndexTask(OverwriteOutputMixin, MapReduceJobTask):
    """
    Index a stream of documents in an elasticsearch index.

    This task is intended to do the following:
    * Create a new index that is unique to this task run (all significant parameters).
    * Load all of the documents into this unique index.
    * If the alias is already pointing at one or more indexes, switch it so that it only points at this newly loaded
      index.
    * Delete any indexes that were previously pointed at by the alias, leaving only the newly loaded index.

    """

    host = luigi.Parameter(
        config_path={'section': 'elasticsearch', 'name': 'host'},
        description='Hostnames for the elasticsearch cluster nodes. They can be specified in any of the formats'
                    ' accepted by the elasticsearch-py library. This includes complete URLs such as http://foo.com/, or'
                    ' host port pairs such as foo:8000. Note that if you wish to use SSL you should specify a full URL'
                    ' and the "https" scheme.'
    )
    timeout = luigi.FloatParameter(
        config_path={'section': 'elasticsearch', 'name': 'timeout'},
        significant=False,
        default=60,
        description='Maximum number of seconds to wait when attempting to make connections to the elasticsearch cluster'
                    ' before assuming the cluster is not responding and giving up with a timeout error.'
    )
    connection_type = luigi.Parameter(
        config_path={'section': 'elasticsearch', 'name': 'connection_type'},
        significant=False,
        default='urllib',
        description='If not specified, default to using urllib3 to make HTTP requests to elasticsearch. The other valid'
                    ' value is "aws" which can be used to connect to clusters that are managed by AWS. See'
                    ' `AWS elasticsearch service <https://aws.amazon.com/elasticsearch-service/>`_'
    )
    alias = luigi.Parameter(
        description='Name of the alias in elasticsearch that will point to the complete index when loaded. This value '
                    ' should match the settings of edx-analytics-data-api.'
    )
    number_of_shards = luigi.Parameter(
        default=None,
        description='Number of `shards <https://www.elastic.co/guide/en/elasticsearch/reference/current/glossary.html'
                    '#glossary-shard>`_ to use in the elasticsearch index.'
    )
    throttle = luigi.FloatParameter(
        default=0.1,
        significant=False,
        description='Wait this many seconds between batches of records submitted to the cluster to be indexed. This can'
                    ' be used to tune the indexing process, allowing the cluster to successfully "keep up" with the'
                    ' loader. Note that often the hadoop cluster can load records much more quickly than the cluster'
                    ' can index them, which eventually causes queues to overflow within the elasticsearch cluster.'
    )
    batch_size = luigi.IntParameter(
        default=1000,
        significant=False,
        description='Number of records to submit to the cluster to be indexed in a single request. A small value here'
                    ' will result in more, smaller, requests and a larger value will result in fewer, bigger requests.'
    )
    indexing_tasks = luigi.IntParameter(
        default=None,
        significant=False,
        description='Number of parallel processes to use to submit records to be indexed from. The stream of records'
                    ' will be divided up evenly among these processes during the indexing procedure.'
    )
    max_attempts = luigi.IntParameter(
        default=10,
        significant=False,
        description='If the elasticsearch cluster rejects a batch of records (usually because it is too busy) the'
                    ' indexing process will retry up to this many times before giving up. It uses an exponential back-'
                    'off strategy, so a high value here can result in very significant wait times before retrying.'
    )

    # These attributes should be overridden, but don't need to be.
    settings = {}
    properties = {}

    def __init__(self, *args, **kwargs):
        super(ElasticsearchIndexTask, self).__init__(*args, **kwargs)

        self.other_reduce_tasks = self.n_reduce_tasks
        if self.indexing_tasks is not None:
            self.n_reduce_tasks = self.indexing_tasks

        self.batch_index = 0
        self.index = self.alias + '_' + str(hash(self.update_id()))
        self.indexes_for_alias = set()

    def init_local(self):
        super(ElasticsearchIndexTask, self).init_local()

        elasticsearch_client = self.create_elasticsearch_client()

        # Find all indexes that are referred to by this alias (currently). These will be deleted after a successful
        # load of the new index.
        try:
            aliases = elasticsearch_client.indices.get_alias(name=self.alias)
            self.indexes_for_alias.update(
                [index for index, alias_info in aliases.iteritems() if self.alias in alias_info['aliases'].keys()]
            )
        except elasticsearch.exceptions.NotFoundError:
            log.warn("No indices found for alias %s", self.alias)

        if self.index in self.indexes_for_alias:
            if not self.overwrite:
                raise RuntimeError('Index {0} is currently in use by alias {1}'.format(self.index, self.alias))
            else:
                # These indexes will be deleted, after the alias swap, make sure we don't delete the index we just
                # populated.
                self.indexes_for_alias.remove(self.index)

        if not self.overwrite and len(self.indexes_for_alias) > 1:
            raise RuntimeError(
                'Invalid state, multiple existing indexes ({0}) found for alias {1}'.format(
                    ', '.join(self.indexes_for_alias),
                    self.alias
                )
            )

        # In order for the OverwriteOutputMixin to recognize that this task has run we need to let it know. This will
        # allow it to actually check if the task is complete after it is run.
        self.attempted_removal = True

        if elasticsearch_client.indices.exists(index=self.index):
            elasticsearch_client.indices.delete(index=self.index)

        settings = {
            'refresh_interval': -1,
        }
        if self.number_of_shards is not None:
            settings['number_of_shards'] = self.number_of_shards

        if self.settings:
            settings.update(self.settings)

        elasticsearch_client.indices.create(index=self.index, body={
            'settings': settings,
            'mappings': {
                'properties': self.properties
            }
        })

    def create_elasticsearch_client(self):
        """Build an elasticsearch client using the various parameters passed into this task."""

        if self.connection_type == 'aws':
            service = 'es'
            region = 'us-east-1'
            credentials = boto3.Session().get_credentials()
            awsauth = requests_aws4auth.AWS4Auth(region=region, service=service, refreshable_credentials=credentials)

            return elasticsearch.Elasticsearch(
                hosts=[{'host': self.host, 'port': 443}],
                http_auth=awsauth,
                use_ssl=True,
                verify_certs=True,
                timeout=self.timeout,
                retry_on_status=(HTTP_CONNECT_TIMEOUT_STATUS_CODE, HTTP_GATEWAY_TIMEOUT_STATUS_CODE),
                retry_on_timeout=True,
                connection_class=RequestsHttpConnection,
                serializer=JSONSerializerPython2(),
            )
        else:
            return elasticsearch.Elasticsearch(
                hosts=self.host,
                timeout=self.timeout,
                retry_on_status=(HTTP_CONNECT_TIMEOUT_STATUS_CODE, HTTP_GATEWAY_TIMEOUT_STATUS_CODE),
                retry_on_timeout=True,
            )

    def mapper(self, line):
        yield (random.randrange(int(self.n_reduce_tasks)), line.rstrip('\r\n'))

    def reducer(self, _key, lines):
        """
        Given a batch of records, transmit them to the elasticsearch cluster to be indexed.

        There should be one reducer per parallel indexing thread. Controlling the number of reducers is the way to
        control the level of parallelism in the load process.
        """
        elasticsearch_client = self.create_elasticsearch_client()

        document_iterator = self.document_generator(lines)
        first_batch = True
        while True:
            bulk_action_batch = self.next_bulk_action_batch(document_iterator)

            if not bulk_action_batch:
                break

            if not first_batch and self.throttle:
                time.sleep(self.throttle)
            first_batch = False

            if self.send_bulk_action_batch(elasticsearch_client, bulk_action_batch):
                self.incr_counter('Elasticsearch', 'Committed Batches', 1)

                # Note that each document produces two entries in the bulk_action_batch list.
                num_records = len(bulk_action_batch) / 2
                self.incr_counter('Elasticsearch', 'Records Indexed', num_records)
            else:
                raise IndexingError('Batch of records rejected too many times. Aborting.')

        # Luigi requires the reducer to actually return something, so we just return empty strings that are written
        # to a temp file in HDFS that is immediately cleaned up after the job finishes.
        yield ('', '')

    def next_bulk_action_batch(self, document_iterator):
        """
        Read a batch of documents from the iterator and convert them into bulk index actions.

        Elasticsearch expects each document to actually be transmitted on two lines the first of which details the
        action to take, and the second contains the actual document.

        See the `Cheaper in Bulk <https://www.elastic.co/guide/en/elasticsearch/guide/1.x/bulk.html>`_ guide.

        Arguments:
            document_iterator (iterator of dicts):

        Returns: A list of dicts that can be transmitted to elasticsearch using the "bulk" request.
        """
        bulk_action_batch = []
        for raw_data in islice(document_iterator, self.batch_size):
            action, data = elasticsearch.helpers.expand_action(raw_data)
            bulk_action_batch.append(action)
            if data is not None:
                bulk_action_batch.append(data)
        return bulk_action_batch

    def send_bulk_action_batch(self, elasticsearch_client, bulk_action_batch):
        """
        Given a batch of actions, transmit them in bulk to the elasticsearch cluster.

        This method handles back-pressure from the elasticsearch cluster which queues up writes. When the queue is full
        the cluster will start rejecting additional bulk indexing requests. This method implements an exponential
        back-off, allowing the cluster to catch-up with the client.

        Arguments:
            elasticsearch_client (elasticsearch.Elasticsearch): A reference to an elasticsearch client.
            bulk_action_batch (list of dicts): A list of bulk actions followed by their respective documents.

        Raises:
            IndexingError: If a record cannot be indexed by elasticsearch this method assumes that is a fatal error and
                it immediately raises this exception. If we try to transmit a batch repeatedly and it is continually
                rejected by the cluster, this method will give up after `max_attempts` and raise this error.

        Returns: True iff the batch of actions was successfully transmitted to and acknowledged by the elasticsearch
            cluster.
        """
        attempts = 0
        batch_written_successfully = False
        while True:
            try:
                resp = elasticsearch_client.bulk(bulk_action_batch, index=self.index)
            except TransportError as transport_error:
                if transport_error.status_code not in (REJECTED_REQUEST_STATUS, HTTP_SERVICE_UNAVAILABLE_STATUS_CODE):
                    raise transport_error
            else:
                num_errors = 0
                for raw_data in resp['items']:
                    _op_type, item = raw_data.popitem()
                    successful = 200 <= item.get('status', 500) < 300
                    if not successful:
                        log.error('Failed to index: %s', str(item))
                        num_errors += 1

                if num_errors == 0:
                    batch_written_successfully = True
                    break
                else:
                    raise IndexingError('Failed to index {0} records. Aborting.'.format(num_errors))

            attempts += 1
            if attempts < self.max_attempts:
                sleep_duration = 2 ** attempts
                self.incr_counter('Elasticsearch', 'Rejected Batches', 1)
                log.warn(
                    'Batch of records rejected. Sleeping for %d seconds before retrying.',
                    sleep_duration
                )
                time.sleep(sleep_duration)
            else:
                batch_written_successfully = False
                break

        return batch_written_successfully

    def document_generator(self, lines):
        """
        Given lines of raw text, generates structured documents that will be indexed by elasticsearch.

        The returned document should have roughly the following structure:

            {
                "_id": "(optional) your custom identifier for the document",
                "_source": {
                    "prop0": "you should have one key-value pair for each property and its value"
                }
            }

        Note that you can also specify other "special" fields other than "_id":

        - _index
        - _parent
        - _percolate
        - _routing
        - _timestamp
        - _ttl
        - _type
        - _version
        - _version_type
        - _retry_on_conflict

        The "_source" field is required.

        Arguments:
            lines (iterable of unicode strings): This is the raw data to be indexed.

        Yields:
            dict: The document to index in the format expected by the elasticsearch bulk loading process.
        """
        raise NotImplementedError

    def extra_modules(self):
        import urllib3

        packages = [elasticsearch, urllib3, boto3, requests_aws4auth]

        return packages

    def jobconfs(self):
        jcs = super(ElasticsearchIndexTask, self).jobconfs()
        jcs.append('mapred.reduce.tasks.speculative.execution=false')
        return jcs

    def update_id(self):
        """A unique identifier for this task instance that is used to determine if it should be run again."""
        return self.task_id

    def output(self):
        return ElasticsearchTarget(
            client=self.create_elasticsearch_client(),
            index=self.alias,
            update_id=self.update_id()
        )

    def commit(self):
        """
        If all documents have been loaded successfully, make the changes visible to users.
        """
        # The ordering of operations here is sensitive.

        elasticsearch_client = self.create_elasticsearch_client()

        # First "refresh" the newly loaded index. We disable refreshes during the load to keep throughput high. This
        # step is necessary to ensure all of the documents are properly indexed and user-visible.
        elasticsearch_client.indices.refresh(index=self.index)

        # Perform an atomic swap of the alias.
        old_indexes = [ix for ix in self.indexes_for_alias if elasticsearch_client.indices.exists(index=ix)]
        actions = [{"remove": {"index": old_index, "alias": self.alias}} for old_index in old_indexes]
        actions.append({"add": {"index": self.index, "alias": self.alias}})
        elasticsearch_client.indices.update_aliases({"actions": actions})

        # Update the luigi metadata to indicate that the task ran successfully.
        self.output().touch()

        # Attempt to remove any old indexes that are now no longer user-visible.
        for old_index in old_indexes:
            elasticsearch_client.indices.delete(index=old_index)

    def rollback(self):
        """
        If something goes wrong during the load, attempt to clean up the partially loaded index.
        """
        elasticsearch_client = self.create_elasticsearch_client()
        try:
            elasticsearch_client.indices.delete(index=self.index, ignore=[400, 404])
        except Exception:  # pylint: disable=broad-except
            log.exception("Unable to rollback the elasticsearch load.")

    def run(self):
        try:
            super(ElasticsearchIndexTask, self).run()
        except Exception:  # pylint: disable=broad-except
            self.rollback()
            raise
        else:
            self.commit()


class IndexingError(RuntimeError):
    """Something went wrong during the indexing operation."""
    pass
