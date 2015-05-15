"""Tests for S3--related utility functionality."""
from mock import MagicMock, patch

from edx.analytics.tasks import s3_util
from edx.analytics.tasks.tests import unittest


class GenerateS3SourcesTestCase(unittest.TestCase):
    """Tests for generate_s3_sources()."""

    def _make_key(self, keyname, size):
        """Makes a dummy key object, providing the necessary accessors."""
        s3_key = MagicMock()
        s3_key.key = keyname
        s3_key.size = size
        return s3_key

    def _make_s3_generator(self, bucket_name, root, path_info, patterns):
        """Generates a list of matching S3 sources using a mock S3 connection."""
        s3_conn = MagicMock()
        s3_bucket = MagicMock()
        s3_conn.get_bucket = MagicMock(return_value=s3_bucket)
        target_list = [self._make_key("{root}/{path}".format(root=root, path=path), size)
                       for path, size in path_info.iteritems()]
        s3_bucket.list = MagicMock(return_value=target_list)
        print [(k.key, k.size) for k in target_list]

        s3_bucket.name = bucket_name
        source = "s3://{bucket}/{root}".format(bucket=bucket_name, root=root)
        generator = s3_util.generate_s3_sources(s3_conn, source, patterns)
        output = list(generator)
        return output

    def _run_without_filtering(self, bucket_name, root, path_info):
        """Runs generator and checks output."""
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


class ScalableS3ClientTestCase(unittest.TestCase):
    """Tests for ScalableS3Client class."""

    def setUp(self):
        patcher = patch('luigi.s3.boto')
        patcher.start()
        self.addCleanup(patcher.stop)

        self.client = s3_util.ScalableS3Client()

    def _assert_get_chunk_specs(self, source_size_bytes, expected_num_chunks, expected_chunk_size):
        """Asserts that _get_chunk_specs returns the expected values."""
        number_of_chunks, bytes_per_chunk = self.client._get_chunk_specs(source_size_bytes)
        self.assertEquals(number_of_chunks, expected_num_chunks)
        self.assertEquals(bytes_per_chunk, expected_chunk_size)

    def test_get_minimum_chunk_specs(self):
        self._assert_get_chunk_specs(1, 1, s3_util.MINIMUM_BYTES_PER_CHUNK)
        self._assert_get_chunk_specs(s3_util.MINIMUM_BYTES_PER_CHUNK, 1, s3_util.MINIMUM_BYTES_PER_CHUNK)

    def test_get_maximum_chunk_specs(self):
        size = ((s3_util.MULTIPART_UPLOAD_THRESHOLD * s3_util.MULTIPART_UPLOAD_THRESHOLD)
                / s3_util.MINIMUM_BYTES_PER_CHUNK) + 1000
        self._assert_get_chunk_specs(size, 205, s3_util.MULTIPART_UPLOAD_THRESHOLD)

        size *= 2
        self._assert_get_chunk_specs(size, 410, s3_util.MULTIPART_UPLOAD_THRESHOLD)

    def test_generate_even_chunks(self):
        generator = self.client._generate_chunks(1000, 4, 250)
        output = list(generator)
        expected_output = [(1, 0, 250), (2, 250, 250), (3, 500, 250), (4, 750, 250)]
        self.assertEquals(output, expected_output)

    def test_generate_uneven_chunks(self):
        generator = self.client._generate_chunks(900, 4, 250)
        output = list(generator)
        expected_output = [(1, 0, 250), (2, 250, 250), (3, 500, 250), (4, 750, 150)]
        self.assertEquals(output, expected_output)
