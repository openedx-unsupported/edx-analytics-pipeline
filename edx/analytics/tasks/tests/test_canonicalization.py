
from cStringIO import StringIO
import gzip
import json
import os

from luigi import date_interval
from mock import patch

from edx.analytics.tasks.canonicalization import CanonicalizationTask

from edx.analytics.tasks.tests import unittest


class CanonicalizationMapperTest(unittest.TestCase):

    def setUp(self):
        self.task = CanonicalizationTask(
            interval=date_interval.Date.parse('2015-01-01')
        )
        increment_patcher = patch.object(self.task, 'increment_counter')
        self.mock_increment = increment_patcher.start()
        self.addCleanup(increment_patcher.stop)

        env_override = {
            'map_input_file': '/tmp/foobar'
        }
        env_patcher = patch.dict(os.environ, env_override, clear=True)
        env_patcher.start()
        self.addCleanup(env_patcher.stop)

    def test_malformed_event(self):
        self.assert_empty(self.task.mapper('foobarbaz'))
        self.mock_increment.assert_called_once_with('analytics.c14n.malformed')

    def assert_empty(self, output):
        self.assertEquals(len(list(output)), 0, 'Mapper emitted events when we expected none')

    def test_missing_event_type(self):
        self.assert_empty(self.call_mapper_with_event({'foo': 'bar'}))

    def call_mapper(self, **kwargs):
        event = self.build_event(**kwargs)
        return self.call_mapper_with_event(event)

    def call_mapper_with_event(self, event):
        self.task.init_local()
        line = json.dumps(event)
        return self.task.mapper(line)

    def test_missing_time(self):
        event = self.build_event()
        del event['time']

        self.assert_empty(
            self.call_mapper_with_event(event)
        )

    def build_event(self, **kwargs):
        default = {
            'event_type': 'foo.bar',
            'time': '2015-01-01T00:00:00+00:00'
        }
        default.update(kwargs)
        return default

    def test_timestamped_time(self):
        output = self.call_mapper(time='2015-01-01T00:00:00+00:00')
        self.assert_event_contains(output, time='2015-01-01T00:00:00.000000Z')

    def assert_event_contains(self, output_gen, **kwargs):
        try:
            key, event_string = output_gen.next()
        except StopIteration:
            self.fail('No events emitted when we expected exactly one.')
        event = json.loads(event_string)
        for key, value in kwargs.iteritems():
            self.assertEquals(event[key], value)

    def test_missing_timezone(self):
        output = self.call_mapper(time='2015-01-01T00:00:00.000000')
        self.assert_event_contains(output, time='2015-01-01T00:00:00.000000Z')

    def test_missing_micros(self):
        output = self.call_mapper(time='2015-01-01T00:00:00+00:00')
        self.assert_event_contains(output, time='2015-01-01T00:00:00.000000Z')

    def test_missing_timezone_and_micros(self):
        output = self.call_mapper(time='2015-01-01T00:00:00')
        self.assert_event_contains(output, time='2015-01-01T00:00:00.000000Z')

    def test_utc_abbrev(self):
        output = self.call_mapper(time='2015-01-01T00:00:00Z')
        self.assert_event_contains(output, time='2015-01-01T00:00:00.000000Z')

    def test_already_completed_date(self):
        with patch.dict(self.task.__dict__, {'complete_dates': set(['2015-01-01'])}):
            self.assert_empty(self.call_mapper(time='2015-01-01T00:00:00'))

    def test_late_event(self):
        self.assert_emitted(
            self.call_mapper(time='2015-01-01T00:00:00+00:00', context={'received_at': '2015-01-02T00:00:01Z'})
        )
        self.mock_increment.assert_called_once_with('analytics.c14n.late_events')

    def assert_emitted(self, output):
        self.assertGreater(len(list(output)), 0, 'Mapper did not emit an event when it was expected to')

    def test_not_late_event(self):
        self.assert_emitted(
            self.call_mapper(context={'received_at': '2015-01-01T01:00:00Z'})
        )
        self.assertFalse(self.mock_increment.called)

    def test_outside_of_date_range(self):
        self.assert_empty(
            self.call_mapper(time='2015-01-02T00:00:00+00:00')
        )

    def test_string_content(self):
        self.assert_event_contains(self.call_mapper(event='{"foo": "bar"}'), event={'foo': 'bar'})

    def test_invalid_json_string_content(self):
        self.assert_event_contains(self.call_mapper(event='{"foo": "ba'), event={})


class CanonicalizationReducerTest(unittest.TestCase):

    def setUp(self):
        self.task = CanonicalizationTask(
            interval=date_interval.Date.parse('2015-01-01')
        )
        self.task.init_local()
        increment_patcher = patch.object(self.task, 'increment_counter')
        self.mock_increment = increment_patcher.start()
        self.addCleanup(increment_patcher.stop)

    def test_output_path_for_key(self):
        pass

    def test_writing_gzip_data(self):
        output_file = StringIO()
        input_values = [
            {'event_type': 'play_video'},
            {'event_type': 'stop_video'}
        ]
        input_values = [json.dumps(e) for e in input_values]
        self.task.multi_output_reducer(('2015-01-01', 1), input_values, output_file)
        output_file.seek(0)
        expected_output = '\n'.join(input_values) + '\n'
        with gzip.GzipFile(mode='rb', fileobj=output_file) as gzip_output_file:
            actual_output = gzip_output_file.read()
        self.assertEqual(expected_output, actual_output)
        self.mock_increment.assert_called_with('analytics.c14n.events', value=2)
