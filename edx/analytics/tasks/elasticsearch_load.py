import random

import time
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


class ElasticsearchIndexTaskMixin(OverwriteOutputMixin):
    host = luigi.Parameter(
        is_list=True,
        config_path={'section': 'elasticsearch', 'name': 'host'}
    )
    timeout = luigi.FloatParameter(
        config_path={'section': 'elasticsearch', 'name': 'timeout'},
        significant=False,
    )
    connection_type = luigi.Parameter(
        config_path={'section': 'elasticsearch', 'name': 'connection_type'},
        significant=False,
    )
    alias = luigi.Parameter()
    index = luigi.Parameter(default=None)
    number_of_shards = luigi.Parameter(default=None)
    throttle = luigi.FloatParameter(default=0.1, significant=False)
    batch_size = luigi.IntParameter(default=1000, significant=False)
    indexing_tasks = luigi.IntParameter(default=None, significant=False)
    max_attempts = luigi.IntParameter(default=10)


class ElasticsearchIndexTask(ElasticsearchIndexTaskMixin, MapReduceJobTask):

    settings = {}
    properties = {}

    def __init__(self, *args, **kwargs):
        super(ElasticsearchIndexTask, self).__init__(*args, **kwargs)

        self.other_reduce_tasks = self.n_reduce_tasks
        if self.indexing_tasks is not None:
            self.n_reduce_tasks = self.indexing_tasks

        self.batch_index = 0

        if self.index is None:
            self.index = self.alias + '_' + str(str(int(time.time())))

        self.indexes_for_alias = []

    def init_local(self):
        super(ElasticsearchIndexTask, self).init_local()

        es = self.create_elasticsearch_client()

        for index_name, aliases in es.indices.get_aliases(name=self.alias).iteritems():
            if self.alias in aliases.get('aliases', {}) or index_name.startswith(self.alias + '_'):
                self.indexes_for_alias.append(index_name)

        if not self.overwrite:
            if self.index in self.indexes_for_alias:
                raise RuntimeError('Index {0} is currently in use by alias {1}'.format(self.index, self.alias))
            elif len(self.indexes_for_alias) > 1:
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
            retry_on_status=(408, 504),
            retry_on_timeout=True,
            **kwargs
        )

    def mapper(self, line):
        yield (random.randrange(int(self.n_reduce_tasks)), line.rstrip('\r\n'))

    def reducer(self, _key, lines):
        es = self.create_elasticsearch_client()

        document_iterator = self.document_generator(lines)
        while True:
            bulk_actions = []
            num_records = 0
            for raw_data in islice(document_iterator, self.batch_size):
                action, data = elasticsearch.helpers.expand_action(raw_data)
                num_records += 1
                bulk_actions.append(action)
                if data is not None:
                    bulk_actions.append(data)

            if not bulk_actions:
                break

            attempts = 0
            succeeded = False
            while True:
                try:
                    resp = es.bulk(bulk_actions, index=self.index, doc_type=self.doc_type)
                except TransportError as e:
                    if e.status_code != REJECTED_REQUEST_STATUS:
                        raise e
                else:
                    num_errors = 0
                    for raw_data in resp['items']:
                        op_type, item = raw_data.popitem()
                        ok = 200 <= item.get('status', 500) < 300
                        if not ok:
                            log.error('Failed to index: {0}'.format(str(raw_data)))
                            num_errors += 1

                    if num_errors == 0:
                        succeeded = True
                    else:
                        raise RuntimeError('Failed to index {0} records. Aborting.'.format(num_errors))

                attempts += 1
                if not succeeded and attempts < self.max_attempts:
                    sleep_duration = 2**attempts
                    log.warn(
                        'Batch of records rejected. Sleeping for {0} seconds before retrying.'.format(sleep_duration)
                    )
                    time.sleep(sleep_duration)
                else:
                    break

            if succeeded:
                self.incr_counter('Elasticsearch', 'Records Indexed', num_records)
                if self.throttle:
                    time.sleep(self.throttle)
            else:
                raise RuntimeError('Batch of records rejected too many times. Aborting.')

        yield ('', '')

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
            index=self.index,
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
