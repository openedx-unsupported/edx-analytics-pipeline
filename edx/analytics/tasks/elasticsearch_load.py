import random

import datetime
import luigi
import time
import logging

from edx.analytics.tasks.mapreduce import MapReduceJobTask
from edx.analytics.tasks.util.elasticsearch_target import ElasticsearchTarget
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin

try:
    import elasticsearch
    import elasticsearch.helpers
except ImportError:
    elasticsearch = None

try:
    from edx.analytics.tasks.util.boto_connection import BotoHttpConnection
except ImportError:
    BotoHttpConnection = None


log = logging.getLogger(__name__)


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
    throttle = luigi.FloatParameter(default=0.5, significant=False)
    batch_size = luigi.IntParameter(default=1000, significant=False)
    indexing_tasks = luigi.IntParameter(default=None, significant=False)


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
            day_of_year = datetime.datetime.utcnow().timetuple().tm_yday
            self.index = self.alias + '_' + str(day_of_year % 2)

        self.old_index = None

    def init_local(self):
        super(ElasticsearchIndexTask, self).init_local()

        es = self.create_elasticsearch_client()

        indexes_for_alias = es.indices.get_aliases(name=self.alias)
        if self.index in indexes_for_alias:
            raise RuntimeError('Index {0} is currently in use by alias {1}'.format(self.index, self.alias))
        elif len(indexes_for_alias) > 1:
            raise RuntimeError('Invalid state, multiple indexes ({0}) found for alias {1}'.format(', '.join(indexes_for_alias.keys()), self.alias))
        else:
            self.old_index = indexes_for_alias.keys()[0]

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
        self.batch_index = 0

        def wrapped_record_generator():
            for record in self.document_generator(lines):
                record['_type'] = self.doc_type
                yield record
                self.batch_index += 1
                if self.batch_size is not None and self.batch_index >= self.batch_size:
                    self.incr_counter('Elasticsearch', 'Records Indexed', self.batch_index)
                    self.batch_index = 0
                    if self.throttle:
                        time.sleep(self.throttle)

        num_indexed, errors = elasticsearch.helpers.bulk(
            es,
            wrapped_record_generator(),
            index=self.index,
            chunk_size=self.batch_size,
            raise_on_error=False
        )
        self.incr_counter('Elasticsearch', 'Records Indexed', self.batch_index)
        num_errors = len(errors)
        self.incr_counter('Elasticsearch', 'Indexing Errors', num_errors)
        if num_errors > 0:
            log.error('Number of errors: {0}\n'.format(num_errors))
            for error in errors:
                log.error(str(error))
            raise RuntimeError('Unable to index')

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

    def run(self):
        es = self.create_elasticsearch_client()
        super(ElasticsearchIndexTask, self).run()
        es.indices.refresh(index=self.index)
        es.indices.update_aliases(
            {
                "actions": [
                    {"remove": {"index": self.old_index, "alias": self.alias}},
                    {"add": {"index": self.index, "alias": self.alias}}
                ]
            }
        )
        self.output().touch()
