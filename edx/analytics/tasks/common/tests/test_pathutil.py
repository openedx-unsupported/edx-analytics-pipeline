"""Test selection of event log files."""

import datetime
import unittest

from luigi.date_interval import Month
from mock import patch

from edx.analytics.tasks.common.pathutil import PathSelectionByDateIntervalTask
from edx.analytics.tasks.util.tests.config import with_luigi_config
from edx.analytics.tasks.util.url import UncheckedExternalURL


class PathSelectionByDateIntervalTaskTest(unittest.TestCase):
    """Test selection of event log files."""

    SOURCE_1 = 's3://collection-bucket/'
    SAMPLE_KEY_PATHS_1 = [
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
    ]
    SOURCE_2 = 's3://collection-bucket2/'
    SAMPLE_KEY_PATHS_2 = [
        'FakeServerGroup/tracking.log-20140227.gz',
        'FakeServerGroup/tracking.log-20140228.gz',
        'FakeServerGroup/tracking.log-20140318.gz',
        'FakeServerGroup/tracking.log-20140318',
        'FakeServerGroup/tracking.log-20140319-1395256622.gz',
        'FakeServerGroup/tracking.log-20140401-1396379384.gz',
        'FakeServerGroup/tracking.log-20140402-1396465784.gz',
        'FakeWorkerServerGroup',
        'FakeWorkerServerGroup/tracking.log',
        'FakeWorkerServerGroup/tracking.log-20131126.gz',
        'FakeWorkerServerGroup/tracking.log-20140416-1397643421.gz',
        'test',
        'test/tracking.log.gz',
        'tmp/FakeServerGroup-mnt-logs.tar.gz',
        'tracking.log',
    ]
    SAMPLE_KEY_PATHS = SAMPLE_KEY_PATHS_1 + SAMPLE_KEY_PATHS_2
    COMPLETE_SOURCE_PATHS_1 = [SOURCE_1 + path for path in SAMPLE_KEY_PATHS_1]
    COMPLETE_SOURCE_PATHS_2 = [SOURCE_2 + path for path in SAMPLE_KEY_PATHS_2]
    COMPLETE_SOURCE_PATHS = COMPLETE_SOURCE_PATHS_1 + COMPLETE_SOURCE_PATHS_2
    SOURCE = [SOURCE_1, SOURCE_2]

    @patch('luigi.contrib.s3.S3Client.s3')
    def test_requires(self, connect_s3_mock):
        s3_conn_mock = connect_s3_mock
        bucket_mock = s3_conn_mock.get_bucket.return_value

        class FakeKey(object):
            """A test double of the structure returned by boto when listing keys in an S3 bucket."""
            def __init__(self, path):
                self.key = path
                self.size = 10

        bucket_mock.list.return_value = [FakeKey(path) for path in self.SAMPLE_KEY_PATHS]

        task = PathSelectionByDateIntervalTask(
            source=self.SOURCE,
            interval=Month.parse('2014-03'),
            pattern=[r'.*?FakeServerGroup/tracking.log-(?P<date>\d{8}).*\.gz'],
            expand_interval=datetime.timedelta(0),
        )

        expected_paths = [
            'FakeServerGroup/tracking.log-20140318.gz',
            'FakeServerGroup/tracking.log-20140319-1395256622.gz',
        ]

        self.assertItemsEqual(
            task.requires(),
            [UncheckedExternalURL(source + path) for path in expected_paths for source in self.SOURCE]
        )

    def test_default_source(self):
        task = PathSelectionByDateIntervalTask(interval=Month.parse('2014-03'))
        self.assertEquals(task.source, ('s3://fake/input/', 's3://fake/input2/'))

    def test_default_pattern(self):
        task = PathSelectionByDateIntervalTask(interval=Month.parse('2014-03'))
        self.assertEquals(task.pattern, (
            r'.*tracking.log-(?P<date>\\d{8}).*\\.gz',
            r'.*tracking.notalog-(?P<date>\\d{8}).*\\.gz',
        ))

    def test_filtering_of_urls(self):
        task = PathSelectionByDateIntervalTask(
            source=self.SOURCE,
            interval=Month.parse('2014-03'),
            pattern=[r'.*?FakeServerGroup/tracking.log-(?P<date>\d{8}).*\.gz'],
            expand_interval=datetime.timedelta(0),
        )

        self.assert_only_matched(task, [
            'FakeServerGroup/tracking.log-20140318.gz',
            'FakeServerGroup/tracking.log-20140319-1395256622.gz',
        ])

    def test_multiple_filtering_of_urls(self):
        task = PathSelectionByDateIntervalTask(
            source=self.SOURCE,
            interval=Month.parse('2014-03'),
            pattern=[
                r'.*?FakeServerGroup/tracking.log-(?P<date>\d{8}).*\.gz',
                r'.*?FakeEdgeServerGroup/tracking.log-(?P<date>\d{8}).*\.gz',
                r'.*tracking_\d{3,5}\.log\.gz$',
            ],
            expand_interval=datetime.timedelta(0),
        )

        self.assert_only_matched(task, [
            'FakeServerGroup/tracking.log-20140318.gz',
            'FakeServerGroup/tracking.log-20140319-1395256622.gz',
            'FakeEdgeServerGroup/tracking.log-20140324-1395670621.gz',
            'FakeOldServerGroup3/tracking_14602.log.gz',
        ])

    def assert_only_matched(self, task, paths):
        """Assert that the task only includes the given paths in the selected set of files."""
        matched_urls = []
        for url in self.COMPLETE_SOURCE_PATHS:
            if task.should_include_url(url):
                matched_urls.append(url)

        expected_urls = [
            self.SOURCE_1 + path if path in self.SAMPLE_KEY_PATHS_1 else self.SOURCE_2 + path for path in paths
        ]
        self.assertItemsEqual(matched_urls, expected_urls)

    def test_edge_urls(self):
        task = PathSelectionByDateIntervalTask(
            source=self.SOURCE,
            interval=Month.parse('2014-03'),
            pattern=[r'.*?FakeEdgeServerGroup/tracking.log-(?P<date>\d{8}).*\.gz'],
            expand_interval=datetime.timedelta(0),
        )

        self.assert_only_matched(task, [
            'FakeEdgeServerGroup/tracking.log-20140324-1395670621.gz',
        ])

    def test_timestamped_urls(self):
        task = PathSelectionByDateIntervalTask(
            source=self.SOURCE,
            interval=Month.parse('2014-03'),
            pattern=[r'.*?FakeServerGroup/tracking.log-.*-(?P<timestamp>\d{10})\.gz'],
            expand_interval=datetime.timedelta(0),
        )
        self.assert_only_matched(task, [
            'FakeServerGroup/tracking.log-20140319-1395256622.gz',
        ])
        task = PathSelectionByDateIntervalTask(
            source=self.SOURCE,
            interval=Month.parse('2014-03'),
            pattern=[r'.*?FakeServerGroup/tracking.log-.*-(?P<timestamp>\d{10})\.gz'],
            expand_interval=datetime.timedelta(1),
        )
        self.assert_only_matched(task, [
            'FakeServerGroup/tracking.log-20140319-1395256622.gz',
            'FakeServerGroup/tracking.log-20140401-1396379384.gz',
        ])

    def test_expanded_interval(self):
        task = PathSelectionByDateIntervalTask(
            source=self.SOURCE,
            interval=Month.parse('2014-03'),
            pattern=[r'.*?FakeServerGroup/tracking.log-(?P<date>\d{8}).*\.gz'],
            expand_interval=datetime.timedelta(1),
        )

        self.assert_only_matched(task, [
            'FakeServerGroup/tracking.log-20140228.gz',
            'FakeServerGroup/tracking.log-20140318.gz',
            'FakeServerGroup/tracking.log-20140319-1395256622.gz',
            'FakeServerGroup/tracking.log-20140401-1396379384.gz',
        ])

    @with_luigi_config('event-logs', 'pattern', '["foobar"]')
    def test_pattern_from_config(self):
        task = PathSelectionByDateIntervalTask(
            interval=Month.parse('2014-03')
        )
        self.assertEquals(task.pattern, ('foobar',))

    @with_luigi_config('event-logs', 'pattern', '["foobar"]')
    def test_pattern_override(self):
        task = PathSelectionByDateIntervalTask(
            interval=Month.parse('2014-03'),
            pattern=['baz']
        )
        self.assertEquals(task.pattern, ('baz',))
