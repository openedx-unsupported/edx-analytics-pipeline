"""Test selection of event log files."""

import datetime

from mock import patch

from luigi.date_interval import Month

from edx.analytics.tasks.pathutil import EventLogSelectionTask
from edx.analytics.tasks.url import UncheckedExternalURL
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.config import with_luigi_config


class EventLogSelectionTaskTest(unittest.TestCase):
    """Test selection of event log files."""

    SOURCE = 's3://collection-bucket/'
    SAMPLE_KEY_PATHS = [
        'FakeOldServerGroup',
        'FakeOldServerGroup/edx.log-20120912.gz',
        'FakeOldServerGroup/.tracking_17438.log.gz.JscfpA',
        'FakeOldServerGroup/edx.log-20120912.gz',
        'FakeOldServerGroup/mnt',
        'FakeOldServerGroup2/nginx/old_logs/error.log.34.gz',
        'FakeOldServerGroup3/tracking_14602.log.gz',
        'processed/FooX/FakeServerGroup/2012-09-24_FooX.log.gz',
        'processed/FooX/FakeServerGroup/2012-09-24_FooX.log.gz.gpg',
        'processed/exclude.txt',
        'processed/testing',
        'FakeEdgeServerGroup',
        'FakeEdgeServerGroup/tracking.log',
        'FakeEdgeServerGroup/tracking.log-20130301.gz',
        'FakeEdgeServerGroup/tracking.log-20130823',
        'FakeEdgeServerGroup/tracking.log-20140324-1395670621.gz',
        'FakeServerGroup2/tracking.log-20130331.gz',
        'FakeServerGroup6',
        'FakeServerGroup/tracking.log-20140227.gz',
        'FakeServerGroup/tracking.log-20140228.gz',
        'FakeServerGroup/tracking.log-20140318.gz',
        'FakeServerGroup/tracking.log-20140318',
        'FakeServerGroup/tracking.log-20140319-1395256622.gz',
        'FakeServerGroup/tracking.log-20140401-1395254574.gz',
        'FakeServerGroup/tracking.log-20140402-1395645654.gz',
        'FakeWorkerServerGroup',
        'FakeWorkerServerGroup/tracking.log',
        'FakeWorkerServerGroup/tracking.log-20131126.gz',
        'FakeWorkerServerGroup/tracking.log-20140416-1397643421.gz',
        'test',
        'test/tracking.log.gz',
        'tmp/FakeServerGroup-mnt-logs.tar.gz',
        'tracking.log'
    ]
    COMPLETE_SOURCE_PATHS = [SOURCE + path for path in SAMPLE_KEY_PATHS]

    @patch('edx.analytics.tasks.pathutil.boto.connect_s3')
    def test_requires(self, connect_s3_mock):
        s3_conn_mock = connect_s3_mock.return_value
        bucket_mock = s3_conn_mock.get_bucket.return_value

        class FakeKey(object):
            """A test double of the structure returned by boto when listing keys in an S3 bucket."""
            def __init__(self, path):
                self.key = path
                self.size = 10

        bucket_mock.list.return_value = [FakeKey(path) for path in self.SAMPLE_KEY_PATHS]

        task = EventLogSelectionTask(
            source=self.SOURCE,
            interval=Month.parse('2014-03'),
            pattern=r'.*?FakeServerGroup/tracking.log-(?P<date>\d{8}).*\.gz',
            expand_interval=datetime.timedelta(0),
        )

        expected_paths = [
            'FakeServerGroup/tracking.log-20140318.gz',
            'FakeServerGroup/tracking.log-20140319-1395256622.gz',
        ]

        self.assertItemsEqual(task.requires(), [UncheckedExternalURL(self.SOURCE + path) for path in expected_paths])

    def test_filtering_of_urls(self):
        task = EventLogSelectionTask(
            source=self.SOURCE,
            interval=Month.parse('2014-03'),
            pattern=r'.*?FakeServerGroup/tracking.log-(?P<date>\d{8}).*\.gz',
            expand_interval=datetime.timedelta(0),
        )

        self.assert_only_matched(task, [
            'FakeServerGroup/tracking.log-20140318.gz',
            'FakeServerGroup/tracking.log-20140319-1395256622.gz',
        ])

    def assert_only_matched(self, task, paths):
        """Assert that the task only includes the given paths in the selected set of files."""
        matched_urls = []
        for url in self.COMPLETE_SOURCE_PATHS:
            if task.should_include_url(url):
                matched_urls.append(url)

        expected_urls = [self.SOURCE + path for path in paths]
        self.assertItemsEqual(matched_urls, expected_urls)

    def test_edge_urls(self):
        task = EventLogSelectionTask(
            source=self.SOURCE,
            interval=Month.parse('2014-03'),
            pattern=r'.*?FakeEdgeServerGroup/tracking.log-(?P<date>\d{8}).*\.gz',
            expand_interval=datetime.timedelta(0),
        )

        self.assert_only_matched(task, [
            'FakeEdgeServerGroup/tracking.log-20140324-1395670621.gz',
        ])

    def test_expanded_interval(self):
        task = EventLogSelectionTask(
            source=self.SOURCE,
            interval=Month.parse('2014-03'),
            pattern=r'.*?FakeServerGroup/tracking.log-(?P<date>\d{8}).*\.gz',
            expand_interval=datetime.timedelta(1),
        )

        self.assert_only_matched(task, [
            'FakeServerGroup/tracking.log-20140228.gz',
            'FakeServerGroup/tracking.log-20140318.gz',
            'FakeServerGroup/tracking.log-20140319-1395256622.gz',
            'FakeServerGroup/tracking.log-20140401-1395254574.gz',
        ])

    @with_luigi_config('event-logs', 'pattern', 'foobar')
    def test_pattern_from_config(self):
        task = EventLogSelectionTask(
            interval=Month.parse('2014-03')
        )
        self.assertEquals(task.pattern, 'foobar')

    @with_luigi_config('event-logs', 'pattern', 'foobar')
    def test_pattern_override(self):
        task = EventLogSelectionTask(
            interval=Month.parse('2014-03'),
            pattern='baz'
        )
        self.assertEquals(task.pattern, 'baz')
