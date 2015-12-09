import random
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


log = logging.getLogger(__name__)


class ElasticsearchIndexTaskMixin(OverwriteOutputMixin):
    host = luigi.Parameter(
        is_list=True,
        config_path={'section': 'elasticsearch', 'name': 'host'}
    )
    timeout = luigi.FloatParameter(
        config_path={'section': 'elasticsearch', 'name': 'timeout'}
    )
    index = luigi.Parameter()
    number_of_shards = luigi.Parameter(default=None)
    throttle = luigi.FloatParameter(default=0.25)
    batch_size = luigi.IntParameter(default=500)
    indexing_tasks = luigi.IntParameter(default=None)


class ElasticsearchIndexTask(ElasticsearchIndexTaskMixin, MapReduceJobTask):

    settings = {}
    properties = {}

    def __init__(self, *args, **kwargs):
        super(ElasticsearchIndexTask, self).__init__(*args, **kwargs)

        self.other_reduce_tasks = self.n_reduce_tasks
        if self.indexing_tasks is not None:
            self.n_reduce_tasks = self.indexing_tasks

        self.batch_index = 0

    def init_local(self):
        super(ElasticsearchIndexTask, self).init_local()

        es = self.create_elasticsearch_client()
        if not es.indices.exists(index=self.index):
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
        return elasticsearch.Elasticsearch(
            hosts=self.host,
            timeout=self.timeout,
            retry_on_status=(408, 504),
            retry_on_timeout=True
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
            host=self.host,
            index=self.index,
            doc_type=self.doc_type,
            update_id=self.update_id()
        )

    def run(self):
        es = self.create_elasticsearch_client()
        super(ElasticsearchIndexTask, self).run()
        es.indices.refresh(index=self.index)
        self.output().touch()
