import random

from itertools import islice

import luigi
import time
import logging

from edx.analytics.tasks.mapreduce import MapReduceJobTask
from edx.analytics.tasks.util.elasticsearch_target import ElasticsearchTarget
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin

try:
    import elasticsearch
    import elasticsearch.helpers
    from elasticsearch.exceptions import TransportError
except ImportError:
    elasticsearch = None

try:
    from edx.analytics.tasks.util.boto_connection import BotoHttpConnection
except ImportError:
    BotoHttpConnection = None


log = logging.getLogger(__name__)


REJECTED_REQUEST_STATUS = 429
HTTP_CONNECT_TIMEOUT_STATUS_CODE = 408
HTTP_GATEWAY_TIMEOUT_STATUS_CODE = 504


class ElasticsearchIndexTaskMixin(OverwriteOutputMixin):
    host = luigi.Parameter(
        is_list=True,
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
                    ' value is "boto" which can be used to connect to clusters that are managed by AWS. See'
                    ' `AWS elasticsearch service <https://aws.amazon.com/elasticsearch-service/>`_'
    )
    alias = luigi.Parameter(
        description='Name of the alias in elasticsearch that will point to the complete index when loaded. This value '
                    'should match the settings of edx-analytics-data-api.'
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
                    'will result in more, smaller, requests and a larger value will result in fewer, bigger requests.'
    )
    indexing_tasks = luigi.IntParameter(
        default=None,
        significant=False,
        description='Number of parallel processes to use to submit records to be indexed from. The stream of records'
                    ' will be divided up evenly among these processes during the indexing procedure.'
    )
    max_attempts = luigi.IntParameter(
        default=10,
        description='If the elasticsearch cluster rejects a batch of records (usually because it is too busy) the'
                    ' indexing process will retry up to this many times before giving up. It uses an exponential back-'
                    'off strategy, so a high value here can result in very significant wait times before retrying.'
    )


class ElasticsearchIndexTask(ElasticsearchIndexTaskMixin, MapReduceJobTask):
    """
    Index a stream of documents in an elasticsearch index.

    The is intended to do the following:
    * Create a new index that is unique to this task run (all significant parameters).
    * Load all of the documents into this unique index.
    * If the alias is already pointing at one or more indexes, switch it so that it only points at this newly loaded
      index.
    * Delete any indexes that were previously pointed at by the alias, leaving only the newly loaded index.

    """

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

        es = self.create_elasticsearch_client()

        # Find all indexes that are referred to by this alias (currently). These will be deleted after a successful
        # load of the new index.
        for index_name, aliases in es.indices.get_aliases(name=self.alias).iteritems():
            if self.alias in aliases.get('aliases', {}):
                self.indexes_for_alias.add(index_name)

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

        if es.indices.exists(index=self.index):
            es.indices.delete(index=self.index)

        settings = {
            'refresh_interval': -1,
        }
        if self.number_of_shards is not None:
            settings['number_of_shards'] = self.number_of_shards

        if self.settings:
            settings.update(self.settings)

        es.indices.create(index=self.index, body={
            'settings': settings,
            'mappings': {
                self.doc_type: {
                    'properties': self.properties
                }
            }
        })

    def create_elasticsearch_client(self):
        kwargs = {}
        if self.connection_type == 'boto':
            kwargs['connection_class'] = BotoHttpConnection
        return elasticsearch.Elasticsearch(
            hosts=self.host,
            timeout=self.timeout,
            retry_on_status=(HTTP_CONNECT_TIMEOUT_STATUS_CODE, HTTP_GATEWAY_TIMEOUT_STATUS_CODE),
            retry_on_timeout=True,
            **kwargs
        )

    def mapper(self, line):
        yield (random.randrange(int(self.n_reduce_tasks)), line.rstrip('\r\n'))

    def reducer(self, _key, lines):
        es = self.create_elasticsearch_client()

        document_iterator = self.document_generator(lines)
        first_batch = True
        while True:
            bulk_action_batch = self.next_bulk_action_batch(document_iterator)

            if not bulk_action_batch:
                break

            if not first_batch and self.throttle:
                time.sleep(self.throttle)
            first_batch = False

            if self.send_bulk_action_batch(es, bulk_action_batch):
                self.incr_counter('Elasticsearch', 'Committed Batches', 1)

                # Note that each document produces two entries in the bulk_action_batch list.
                num_records = len(bulk_action_batch) / 2
                self.incr_counter('Elasticsearch', 'Records Indexed', num_records)
            else:
                raise IndexingError('Batch of records rejected too many times. Aborting.')

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

    def send_bulk_action_batch(self, es, bulk_action_batch):
        """
        Given a batch of actions, transmit them in bulk to the elasticsearch cluster.

        This method handles back-pressure from the elasticsearch cluster which queues up writes. When the queue is full
        the cluster will start rejecting additional bulk indexing requests. This method implements an exponential
        back-off, allowing the cluster to catch-up with the client.

        Arguments:
            es (elasticsearch.Elasticsearch): A reference to an elasticsearch client.
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
                resp = es.bulk(bulk_action_batch, index=self.index, doc_type=self.doc_type)
            except TransportError as e:
                if e.status_code != REJECTED_REQUEST_STATUS:
                    raise e
            else:
                num_errors = 0
                for raw_data in resp['items']:
                    op_type, item = raw_data.popitem()
                    ok = 200 <= item.get('status', 500) < 300
                    if not ok:
                        log.error('Failed to index: {0}'.format(str(item)))
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
                    'Batch of records rejected. Sleeping for {0} seconds before retrying.'.format(sleep_duration)
                )
                time.sleep(sleep_duration)
            else:
                batch_written_successfully = False
                break

        return batch_written_successfully

    def document_generator(self, lines):
        raise NotImplementedError

    @property
    def doc_type(self):
        raise NotImplementedError

    def extra_modules(self):
        import elasticsearch
        import urllib3

        packages = [elasticsearch, urllib3]

        return packages

    def jobconfs(self):
        jcs = super(ElasticsearchIndexTask, self).jobconfs()
        jcs.append('mapred.reduce.tasks.speculative.execution=false')
        return jcs

    def update_id(self):
        return self.task_id

    def output(self):
        return ElasticsearchTarget(
            client=self.create_elasticsearch_client(),
            index=self.alias,
            doc_type=self.doc_type,
            update_id=self.update_id()
        )

    def commit(self):
        es = self.create_elasticsearch_client()
        es.indices.refresh(index=self.index)
        actions = []
        for old_index in self.indexes_for_alias:
            actions.append({"remove": {"index": old_index, "alias": self.alias}})
        actions.append({"add": {"index": self.index, "alias": self.alias}})
        es.indices.update_aliases({"actions": actions})
        self.output().touch()
        for old_index in self.indexes_for_alias:
            es.indices.delete(index=old_index)

    def run(self):
        super(ElasticsearchIndexTask, self).run()
        self.commit()


class IndexingError(RuntimeError):
    pass
