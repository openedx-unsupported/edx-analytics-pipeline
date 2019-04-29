"""
Tests for S3-related utility functionality.
"""
from __future__ import print_function

from unittest import TestCase

from ddt import data, ddt, unpack
from mock import MagicMock

from edx.analytics.tasks.util import s3_util


class GenerateS3SourcesTestCase(TestCase):
    """
    Tests for generate_s3_sources().
    """

    def _make_key(self, keyname, size):
        """
        Makes a dummy key object, providing the necessary accessors.
        """
        s3_key = MagicMock()
        s3_key.key = keyname
        s3_key.size = size
        return s3_key

    def _make_s3_generator(self, bucket_name, root, path_info, patterns):
        """
        Generates a list of matching S3 sources using a mock S3 connection.
        """
        s3_conn = MagicMock()
        s3_bucket = MagicMock()
        s3_conn.get_bucket = MagicMock(return_value=s3_bucket)
        target_list = [self._make_key("{root}/{path}".format(root=root, path=path), size)
                       for path, size in path_info.iteritems()]
        s3_bucket.list = MagicMock(return_value=target_list)
        print([(k.key, k.size) for k in target_list])

        s3_bucket.name = bucket_name
        source = "s3://{bucket}/{root}".format(bucket=bucket_name, root=root)
        generator = s3_util.generate_s3_sources(s3_conn, source, patterns)
        output = list(generator)
        return output

    def _run_without_filtering(self, bucket_name, root, path_info):
        """
        Runs generator and checks output.
        """
        patterns = ['*']
        output = self._make_s3_generator(bucket_name, root, path_info, patterns)
        self.assertEquals(len(output), len(path_info))
        expected = [(bucket_name, root, key) for key in path_info]
        self.assertEquals(set(output), set(expected))

    def test_normal_generate(self):
        bucket_name = "bucket_name"
        root = "root1/root2"
        path_info = {
            "subdir1/path1": 1000,
            "path2": 2000,
        }
        self._run_without_filtering(bucket_name, root, path_info)

    def test_generate_with_empty_root(self):
        bucket_name = "bucket_name"
        root = ""
        path_info = {
            "subdir1/path1": 1000,
            "path2": 2000,
        }
        self._run_without_filtering(bucket_name, root, path_info)

    def test_generate_with_pattern_filtering(self):
        bucket_name = "bucket_name"
        root = "root1/root2"
        path_info = {
            "subdir1/path1": 1000,
            "path2": 2000,
        }
        patterns = ['*1']
        output = self._make_s3_generator(bucket_name, root, path_info, patterns)
        self.assertEquals(len(output), 1)
        self.assertEquals(output, [(bucket_name, root, "subdir1/path1")])

    def test_generate_with_size_filtering(self):
        bucket_name = "bucket_name"
        root = "root1/root2"
        path_info = {
            "subdir1/path1": 1000,
            "path2": 0,
        }
        patterns = ['*1']
        output = self._make_s3_generator(bucket_name, root, path_info, patterns)
        self.assertEquals(len(output), 1)
        self.assertEquals(output, [(bucket_name, root, "subdir1/path1")])

    def test_generate_with_trailing_slash(self):
        bucket_name = "bucket_name"
        root = "root1/root2/"
        path_info = {
            "subdir1/path1": 1000,
            "path2": 2000,
        }
        patterns = ['*']
        output = self._make_s3_generator(bucket_name, root, path_info, patterns)
        self.assertEquals(len(output), 2)
        self.assertEquals(set(output), set([
            (bucket_name, root.rstrip('/'), "subdir1/path1"),
            (bucket_name, root.rstrip('/'), "path2")
        ]))


@ddt
class CanonicalizeS3URLTestCase(TestCase):
    """
    Tests for canonicalize_s3_url().
    """

    @data(
        ('s3://hello/world', 's3://hello/world'),
        ('s3+https://hello/world', 's3://hello/world'),
    )
    @unpack
    def test_canonicalize_s3_url(self, original_url, canonicalized_url):
        self.assertEquals(
            s3_util.canonicalize_s3_url(original_url),
            canonicalized_url,
        )
