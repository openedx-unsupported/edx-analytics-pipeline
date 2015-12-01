from mock import patch

from edx.analytics.tasks.elasticsearch_load import ElasticsearchIndexTask, BotoHttpConnection

from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin


class BaseIndexTest(object):

    def setUp(self):
        patcher = patch('edx.analytics.tasks.elasticsearch_load.elasticsearch')
        self.elasticsearch_mock = patcher.start()
        self.addCleanup(patcher.stop)
        self.mock_es = self.elasticsearch_mock.Elasticsearch.return_value

        super(BaseIndexTest, self).setUp()

    def create_task(self, **kwargs):
        self.task = RawIndexTask(
            host='http://localhost:3200',
            alias='foo_alias',
            **kwargs
        )


class ElasticsearchIndexTaskMapTest(BaseIndexTest, MapperTestMixin, unittest.TestCase):

    def test_reduce_task_allocation(self):
        self.create_task(n_reduce_tasks=10)

        self.assertEquals(self.task.n_reduce_tasks, 10)
        self.assertEquals(self.task.other_reduce_tasks, 10)

    def test_reduce_task_allocation_differing(self):
        self.create_task(n_reduce_tasks=10, indexing_tasks=2)

        self.assertEquals(self.task.n_reduce_tasks, 2)
        self.assertEquals(self.task.other_reduce_tasks, 10)

    def test_generated_index_name(self):
        self.assertEqual(self.task.index, 'foo_alias_' + str(hash(self.task.update_id())))

    def test_index_already_in_use(self):
        self.mock_es.indices.get_aliases.return_value = {
            self.task.index: {
                'aliases': {
                    'foo_alias': {}
                }
            }
        }
        with self.assertRaisesRegexp(RuntimeError, r'Index \S+ is currently in use by alias foo_alias'):
            self.task.init_local()

    def test_multiple_aliases(self):
        self.mock_es.indices.get_aliases.return_value = {
            'foo_alias_old': {
                'aliases': {
                    'foo_alias': {}
                }
            },
            'some_other_index': {
                'aliases': {
                    'foo_alias': {}
                }
            }
        }
        with self.assertRaisesRegexp(RuntimeError, r'Invalid state, multiple existing indexes \(.*?\) found for alias foo_alias'):
            self.task.init_local()

    def test_remove_if_exists(self):
        self.create_task(overwrite=True)
        self.mock_es.indices.get_aliases.return_value = {
            self.task.index: {
                'aliases': {
                    'foo_alias': {}
                }
            }
        }
        self.mock_es.indices.exists.return_value = True
        self.task.init_local()
        self.mock_es.indices.delete.assert_called_once_with(index=self.task.index)
        self.assertNotIn(self.task.index, self.task.indexes_for_alias)

    def test_overwrite_multiple_aliases(self):
        self.create_task(overwrite=True)
        self.mock_es.indices.get_aliases.return_value = {
            'foo_alias_old': {
                'aliases': {
                    'foo_alias': {}
                }
            },
            'some_other_index': {
                'aliases': {
                    'foo_alias': {}
                }
            }
        }
        self.task.init_local()
        self.assertItemsEqual(['foo_alias_old', 'some_other_index'], self.task.indexes_for_alias)

    def test_index_already_in_use_overwrite(self):
        self.create_task(overwrite=True)
        self.mock_es.indices.get_aliases.return_value = {
            self.task.index: {
                'aliases': {
                    'foo_alias': {}
                }
            }
        }
        self.task.init_local()
        self.assertItemsEqual([], self.task.indexes_for_alias)

    def test_index_creation_settings(self):
        self.task.init_local()
        self.mock_es.indices.create.assert_called_once_with(
            index=self.task.index,
            body={
                'settings': {
                    'refresh_interval': 10
                },
                'mappings': {
                    'raw_text': {
                        'properties': {
                            'all_text': {'type': 'string'}
                        }
                    }
                }
            }
        )

    def test_index_creation_default_settings(self):
        self.task.settings = {}
        self.task.init_local()
        _args, kwargs = self.mock_es.indices.create.call_args
        self.assertEqual(kwargs['body']['settings'], {'refresh_interval': -1})

    def test_other_settings(self):
        self.task.settings = {
            'foo': 'bar'
        }
        self.task.init_local()
        _args, kwargs = self.mock_es.indices.create.call_args
        self.assertEqual(kwargs['body']['settings'], {'refresh_interval': -1, 'foo': 'bar'})

    def test_boto_connection_type(self):
        self.create_task(connection_type='boto')
        self.task.init_local()
        args, kwargs = self.elasticsearch_mock.Elasticsearch.call_args
        self.assertEqual(kwargs['connection_class'], BotoHttpConnection)

    def test_mapper(self):
        with patch('edx.analytics.tasks.elasticsearch_load.random') as mock_random:
            mock_random.randrange.return_value = 3
            self.assert_single_map_output('foo bar baz\r\n', 3, 'foo bar baz')


class RawIndexTask(ElasticsearchIndexTask):

    properties = {
        'all_text': {'type': 'string'}
    }
    settings = {
        'refresh_interval': 10
    }

    @property
    def doc_type(self):
        return 'raw_text'

    def document_generator(self, lines):
        pass


class ElasticsearchIndexTaskReduceTest(BaseIndexTest, ReducerTestMixin, unittest.TestCase):

    def test_foo(self):
        pass

