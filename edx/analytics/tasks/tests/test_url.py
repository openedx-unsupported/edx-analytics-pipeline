"""Tests for URL-related functionality."""
import luigi
import luigi.format
import luigi.hdfs
import luigi.s3
from mock import patch

from edx.analytics.tasks import url
from edx.analytics.tasks.tests import unittest


class TargetFromUrlTestCase(unittest.TestCase):
    """Tests for get_target_from_url()."""

    def test_s3_scheme(self):
        for test_url in ['s3://foo/bar', 's3n://foo/bar']:
            target = url.get_target_from_url(test_url)
            self.assertIsInstance(target, luigi.hdfs.HdfsTarget)
            self.assertEquals(target.path, test_url)

    def test_hdfs_scheme(self):
        target = url.get_target_from_url('hdfs:///foo/bar')
        self.assertIsInstance(target, luigi.hdfs.HdfsTarget)
        self.assertEquals(target.path, '/foo/bar')

    def test_file_scheme(self):
        path = '/foo/bar'
        for test_url in [path, 'file://' + path]:
            target = url.get_target_from_url(test_url)
            self.assertIsInstance(target, luigi.LocalTarget)
            self.assertEquals(target.path, path)

    @patch('luigi.s3.boto')
    def test_s3_https_scheme(self, _mock_boto):
        test_url = 's3+https://foo/bar'
        target = url.get_target_from_url(test_url)
        self.assertIsInstance(target, luigi.s3.S3Target)
        self.assertEquals(target.path, test_url)

    def test_hdfs_directory(self):
        test_url = 's3://foo/bar/'
        target = url.get_target_from_url(test_url)
        self.assertIsInstance(target, luigi.hdfs.HdfsTarget)
        self.assertEquals(target.path, test_url[:-1])
        self.assertEquals(target.format, luigi.hdfs.PlainDir)


class UrlPathJoinTestCase(unittest.TestCase):
    """Tests for url_path_join()."""

    def test_relative(self):
        self.assertEquals(url.url_path_join('s3://foo/bar', 'baz'), 's3://foo/bar/baz')

    def test_absolute(self):
        self.assertEquals(url.url_path_join('s3://foo/bar', '/baz'), 's3://foo/baz')

    def test_attempted_special_elements(self):
        self.assertEquals(url.url_path_join('s3://foo/bar', './baz'), 's3://foo/bar/./baz')
        self.assertEquals(url.url_path_join('s3://foo/bar', '../baz'), 's3://foo/bar/../baz')

    def test_no_path(self):
        self.assertEquals(url.url_path_join('s3://foo', 'baz'), 's3://foo/baz')

    def test_no_netloc(self):
        self.assertEquals(url.url_path_join('file:///foo/bar', 'baz'), 'file:///foo/bar/baz')

    def test_extra_separators(self):
        self.assertEquals(url.url_path_join('s3://foo/bar', '///baz'), 's3://foo///baz')
        self.assertEquals(url.url_path_join('s3://foo/bar', 'baz//bar'), 's3://foo/bar/baz//bar')

    def test_multiple_elements(self):
        self.assertEquals(url.url_path_join('s3://foo', 'bar', 'baz'), 's3://foo/bar/baz')
        self.assertEquals(url.url_path_join('s3://foo', 'bar/bing', 'baz'), 's3://foo/bar/bing/baz')
