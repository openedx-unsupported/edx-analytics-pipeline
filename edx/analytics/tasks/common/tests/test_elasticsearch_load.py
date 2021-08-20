"""Tests for elasticsearch loading."""

import datetime
import unittest

import ddt
import luigi.contrib.hdfs.target
from elasticsearch import TransportError
from freezegun import freeze_time
from mock import MagicMock, call, patch

from edx.analytics.tasks.common.elasticsearch_load import ElasticsearchIndexTask, IndexingError, RequestsHttpConnection
from edx.analytics.tasks.common.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin


class BaseIndexTest(object):
    """A base class for indexing tests."""

    def setUp(self):
        patcher = patch('edx.analytics.tasks.common.elasticsearch_load.elasticsearch.Elasticsearch')
        self.elasticsearch_mock = patcher.start()
        self.addCleanup(patcher.stop)
        self.mock_es = self.elasticsearch_mock.return_value

        sleep_patcher = patch('edx.analytics.tasks.common.elasticsearch_load.time.sleep', return_value=None)
        self.mock_sleep = sleep_patcher.start()
        self.addCleanup(sleep_patcher.stop)

        super(BaseIndexTest, self).setUp()

    def create_task(self, **kwargs):
        """Create a sample indexing task."""
        self.task = RawIndexTask(
            host='http://localhost:3200',
            alias='foo_alias',
            **kwargs
        )


class ElasticsearchIndexTaskMapTest(BaseIndexTest, MapperTestMixin, unittest.TestCase):
    """Test the mapper and initialization logic for elasticsearch indexing tasks."""

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
        self.mock_es.indices.get_alias.return_value = {
            self.task.index: {
                'aliases': {
                    'foo_alias': {}
                }
            }
        }
        with self.assertRaisesRegexp(RuntimeError, r'Index \S+ is currently in use by alias foo_alias'):
            self.task.init_local()

    def test_multiple_aliases(self):
        self.mock_es.indices.get_alias.return_value = {
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
        with self.assertRaisesRegexp(RuntimeError, r'Invalid state, multiple existing indexes \(.*?\) found for alias'
                                                   r' foo_alias'):
            self.task.init_local()

    def test_remove_if_exists(self):
        self.create_task(overwrite=True)
        self.mock_es.indices.get_alias.return_value = {
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
        self.mock_es.indices.get_alias.return_value = {
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
        self.mock_es.indices.get_alias.return_value = {
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
                    'properties': {
                        'all_text': {'type': 'text'}
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
        number_of_shards = 2
        self.task.settings = {
            'foo': 'bar',
            'number_of_shards': number_of_shards,
        }
        self.task.init_local()
        _args, kwargs = self.mock_es.indices.create.call_args
        self.assertEqual(
            kwargs['body']['settings'],
            {'refresh_interval': -1, 'foo': 'bar', 'number_of_shards': number_of_shards}
        )

    def test_boto_connection_type(self):
        self.create_task(connection_type='aws')
        credentials_mock = MagicMock(access_key='', secret_key='secret', token='')
        awsauth_patcher = patch('edx.analytics.tasks.common.elasticsearch_load.boto3.Session.get_credentials', return_value=credentials_mock)
        awsauth_patcher.start()
        self.addCleanup(awsauth_patcher.stop)

        self.task.init_local()
        _args, kwargs = self.elasticsearch_mock.call_args
        self.assertEqual(kwargs['connection_class'], RequestsHttpConnection)

    def test_mapper(self):
        with patch('edx.analytics.tasks.common.elasticsearch_load.random') as mock_random:
            mock_random.randrange.return_value = 3
            self.assert_single_map_output('foo bar baz\r\n', 3, 'foo bar baz')


class RawIndexTask(ElasticsearchIndexTask):
    """A sample elasticsearch indexing class."""

    properties = {
        'all_text': {'type': 'text'}
    }
    settings = {
        'refresh_interval': 10
    }

    def document_generator(self, lines):
        for line in lines:
            yield {'_source': {'all_text': line}}


@ddt.ddt
class ElasticsearchIndexTaskReduceTest(BaseIndexTest, ReducerTestMixin, unittest.TestCase):
    """Test the reducer for the elasticsearch indexing task."""

    reduce_key = 10  # can be any integer

    def test_single_record(self):
        output = self._get_reducer_output(['a'])
        self.assertItemsEqual([('', '')], output)

        self.mock_es.bulk.assert_called_once_with(
            [
                {'index': {}},
                {'all_text': 'a'}
            ],
            index=self.task.index
        )

    def test_multiple_batches(self):
        self.create_task(batch_size=2, throttle=10)

        self.mock_es.bulk.return_value = {'items': []}
        self._get_reducer_output([
            'first batch 0',
            'first batch 1',
            'second batch 0',
        ])

        self.assertItemsEqual(
            self.mock_es.bulk.mock_calls,
            [
                self.bulk_call(
                    [
                        {'index': {}},
                        {'all_text': 'first batch 0'},
                        {'index': {}},
                        {'all_text': 'first batch 1'},
                    ]
                ),
                self.bulk_call(
                    [
                        {'index': {}},
                        {'all_text': 'second batch 0'},
                    ]
                )
            ]
        )
        # ensure that we throttled in between batches
        self.mock_sleep.assert_called_once_with(10)

    def bulk_call(self, actions):
        """A call to the ES bulk API"""
        return call(actions, index=self.task.index)

    def test_transport_error(self):
        self.mock_es.bulk.side_effect = TransportError(404, 'Not found', 'More detail')
        with self.assertRaises(TransportError):
            self._get_reducer_output(['a'])

    def test_non_transport_error(self):
        self.mock_es.bulk.side_effect = RuntimeError()
        with self.assertRaises(RuntimeError):
            self._get_reducer_output(['a'])

    @ddt.data(
        TransportError(503, 'Service Unavailable', ''),
        TransportError(429, 'Rejected bulk request', 'Queue is full')
    )
    def test_rejected_bulk_requests(self, rejected_batch_error):
        self.create_task(max_attempts=10)
        self._rejection_counter = 0
        self.addCleanup(delattr, self, '_rejection_counter')

        def reject_first_3_batches(*_args, **_kwargs):
            """Reject the first 3 batches that are indexed, and then accept the rest."""
            if self._rejection_counter < 3:
                self._rejection_counter += 1
                raise rejected_batch_error
            else:
                return self.get_bulk_api_response(1)

        self.mock_es.bulk.side_effect = reject_first_3_batches

        self._get_reducer_output(['a'])

        # The first three attempts will be rejected and the fourth will succeed, so expect 4 calls
        self.assertItemsEqual(
            self.mock_es.bulk.mock_calls,
            [
                self.bulk_call(
                    [
                        {'index': {}},
                        {'all_text': 'a'},
                    ]
                ),
            ] * 4
        )

        # check the exponential back-off between bulk API requests
        self.assertItemsEqual(
            self.mock_sleep.mock_calls,
            [
                call(2),
                call(4),
                call(8),
            ]
        )

    def get_bulk_api_response(self, num_responses):
        """A common response to a bulk indexing request."""
        return {
            'items': [
                {
                    'index': {
                        '_index': self.task.index,
                        '_type': 'raw_text',
                        '_id': str(i),
                        '_version': i,
                        'status': 200
                    }
                }
                for i in range(num_responses)
            ]
        }

    def test_too_many_rejected_batches(self):
        self.create_task(max_attempts=3)
        self.mock_es.bulk.side_effect = TransportError(429, 'Rejected bulk request', 'Queue is full')

        with self.assertRaisesRegexp(IndexingError, 'Batch of records rejected too many times. Aborting.'):
            self._get_reducer_output(['a'])

        self.assertEqual(len(self.mock_es.bulk.mock_calls), 3)

    def test_indexing_failures(self):
        responses = self.get_bulk_api_response(3)
        responses['items'][1]['index']['status'] = 500
        self.mock_es.bulk.return_value = responses

        with self.assertRaisesRegexp(IndexingError, 'Failed to index 1 records. Aborting.'):
            self._get_reducer_output([
                'first batch 0',
                'first batch 1',
                'first batch 2',
            ])


@freeze_time('2016-03-25')
@patch.object(luigi.contrib.hdfs.target.HdfsTarget, '__del__', return_value=None)
class ElasticsearchIndexTaskCommitTest(BaseIndexTest, ReducerTestMixin, unittest.TestCase):
    """Tests for the commit logic."""

    def test_commit(self, _mock_del):
        self.mock_es.indices.exists.return_value = False
        self.task.commit()

        self.assertEqual(
            self.mock_es.mock_calls,
            [
                call.indices.refresh(index=self.task.index),
                call.indices.update_aliases({'actions': [{'add': {'index': self.task.index, 'alias': 'foo_alias'}}]}),
                call.indices.exists(index='index_updates'),
                call.indices.create(index='index_updates'),
                self.get_expected_index_call(),
                call.indices.flush(index='index_updates')
            ]
        )

    def get_expected_index_call(self):
        """A mock.call object that represents the expected call to index() that touches the marker during commit."""
        return call.__getattr__('index')(
            body={
                'date': datetime.datetime(2016, 3, 25, 0, 0, 0, 0),
                'update_id': self.task.update_id(),
                'target_index': 'foo_alias'
            },
            id=self.task.output().marker_index_document_id(),
            index='index_updates'
        )

    def test_commit_existing_marker_index(self, _mock_del):
        self.mock_es.indices.exists.return_value = True
        self.task.commit()

        self.assertEqual(
            self.mock_es.mock_calls,
            [
                call.indices.refresh(index=self.task.index),
                call.indices.update_aliases({'actions': [{'add': {'index': self.task.index, 'alias': 'foo_alias'}}]}),
                call.indices.exists(index='index_updates'),
                self.get_expected_index_call(),
                call.indices.flush(index='index_updates')
            ]
        )

    def test_commit_with_existing_data(self, _mock_del):
        self.create_task()
        self.mock_es.indices.get_alias.return_value = {
            'foo_alias_old': {
                'aliases': {
                    'foo_alias': {}
                }
            }
        }
        self.mock_es.indices.exists.return_value = False
        self.task.init_local()
        self.mock_es.reset_mock()

        self.mock_es.indices.exists.return_value = True
        self.task.commit()

        self.assertEqual(
            self.mock_es.mock_calls,
            [
                call.indices.refresh(index=self.task.index),
                call.indices.exists(index='foo_alias_old'),
                call.indices.update_aliases(
                    {
                        'actions': [
                            {'remove': {'index': 'foo_alias_old', 'alias': 'foo_alias'}},
                            {'add': {'index': self.task.index, 'alias': 'foo_alias'}}
                        ]
                    }
                ),
                call.indices.exists(index='index_updates'),
                self.get_expected_index_call(),
                call.indices.flush(index='index_updates'),
                call.indices.delete(index='foo_alias_old'),
            ]
        )


@freeze_time('2016-03-25')
@patch.object(luigi.contrib.hdfs.target.HdfsTarget, '__del__', return_value=None)
class ElasticsearchIndexTaskResetTest(BaseIndexTest, ReducerTestMixin, unittest.TestCase):
    """Tests for the rollback logic."""

    def test_reset(self, _mock_del):
        self.mock_es.indices.exists.return_value = False
        self.task.rollback()
        self.assertEqual(
            self.mock_es.mock_calls, [call.indices.delete(index=self.task.index, ignore=[400, 404]), ]
        )
