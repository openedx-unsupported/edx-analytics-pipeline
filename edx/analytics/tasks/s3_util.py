"""
Utility methods for interacting with S3 via boto.
"""
import os
import math
import logging
import time

from fnmatch import fnmatch
from urlparse import urlparse

from boto.s3.key import Key
from filechunkio import FileChunkIO
from luigi.s3 import S3Client, AtomicS3File
from luigi.hdfs import HdfsTarget, Plain


log = logging.getLogger(__name__)

# S3 does not permit using "put" for files larger than 5 GB, and
# returns a socket error.  There is also a chance that smaller files
# might also fail.  Arbitrarily choose a threshold so that files
# larger than 1GB should use multipart upload instead of a single put.
MULTIPART_UPLOAD_THRESHOLD = 1 * 1024 * 1024 * 1024

# Multipart upload algorithm taken from
# https://gist.github.com/fabiant7t/924094, which
# defines a minimum chunk size for multipart upload.
MINIMUM_BYTES_PER_CHUNK = 5242880

# By default, AWS does not apply an ACL to keys that are put into a
# bucket from another account. Having no ACL at all effectively
# renders the object useless since it cannot be read or anything. The
# only workaround we found was to explicitly set the ACL policy when
# putting the object.  Define here what that policy will be.
DEFAULT_KEY_ACCESS_POLICY = 'bucket-owner-full-control'


def get_file_from_key(s3_client, url, output_path):
    """Downloads a file from a given S3 URL to the output_path."""
    # Files won't appear in S3 instantaneously, wait for the files to appear.
    # TODO: exponential backoff
    for _index in range(30):
        key = s3_client.get_key(url)
        if key is not None:
            break
        else:
            time.sleep(2)

    if key is None:
        log.error("Unable to find expected output file %s", url)
        return None

    downloaded_output_path = os.path.join(output_path, url.split('/')[-1])
    key.get_contents_to_filename(downloaded_output_path)

    return downloaded_output_path


def get_s3_bucket_key_names(url):
    """Extract the bucket and key names from a S3 URL"""
    parts = urlparse(url)
    return (parts.netloc.strip('/'), parts.path.strip('/'))


def join_as_s3_url(bucket, root, path):
    """Combine bucket name, root path and relative path into a S3 URL"""
    return 's3://{0}/{1}/{2}'.format(bucket, root, path)


def get_s3_key(s3_conn, url):
    """Returns an S3 key for use in further boto actions."""
    bucket_name, key_name = get_s3_bucket_key_names(url)
    bucket = s3_conn.get_bucket(bucket_name)
    key = bucket.get_key(key_name)
    return key


def generate_s3_sources(s3_conn, source, patterns=['*'], include_zero_length=False):
    """
    Returns a list of S3 sources that match filters.

    Args:

      s3_conn: a boto connection to S3.
      source:  a url to S3.
      patterns:  a list of strings, each of which defines a pattern to match.

    Yields:

      (bucket, root, path) tuples for each matching file on S3.

      where `bucket` and `root` are derived from the source url,
      and `path` is a matching path relative to the `source`.

    Does not include zero-length files.
    """
    bucket_name, root = get_s3_bucket_key_names(source)

    bucket = s3_conn.get_bucket(bucket_name)

    # Make sure that the listing is done on a "folder" boundary,
    # since list() just looks for matching prefixes.
    root_with_slash = root if len(root) == 0 or root.endswith('/') else root + '/'

    # Skip keys that have zero size.  This allows directories
    # to be skipped, but also skips legitimate files that are
    # also zero-length.
    keys = (s.key for s in bucket.list(root_with_slash) if s.size > 0 or include_zero_length)

    # Make paths relative by removing root
    paths = (k[len(root_with_slash):].lstrip('/') for k in keys)

    # Filter only paths that match the include patterns
    paths = _filter_matches(patterns, paths)

    return ((bucket.name, root, path) for path in paths)


def _filter_matches(patterns, names):
    """Return only key names that match any of the include patterns."""

    def func(name):
        """Check if any pattern matches the name."""
        return any(fnmatch(name, pattern) for pattern in patterns)
    return (n for n in names if func(n))


class ScalableS3Client(S3Client):
    """
    S3 client that adds support for multipart uploads and requires minimal permissions.

    Uses S3 multipart upload API for large files, and regular S3 puts for smaller files.

    This client should only require PutObject and PutObjectAcl permissions in order to write to the target bucket.
    """
    # TODO: Make this behavior configurable and submit this change upstream.

    def put(self, local_path, destination_s3_path):
        """Put an object stored locally to an S3 path."""

        # parse path into bucket and key
        (bucket, key) = self._path_to_bucket_and_key(destination_s3_path)

        # If Boto is passed "validate=True", it will require an
        # additional permission to be present when asked to list all
        # of the keys in the bucket.  We want to minimize the set of
        # required permissions so we get a reference to the bucket
        # without validating that it exists.  It should only require
        # PutObject and PutObjectAcl permissions in order to write to
        # the target bucket.
        s3_bucket = self.s3.get_bucket(bucket, validate=False)

        # Check first if we should be doing a multipart upload.
        source_size_bytes = os.stat(local_path).st_size
        if source_size_bytes < MULTIPART_UPLOAD_THRESHOLD:
            self._upload_single(local_path, s3_bucket, key)
        else:
            log.info("File '%s' has size %d, exceeding threshold %d for using put -- using multipart upload.",
                     destination_s3_path, source_size_bytes, MULTIPART_UPLOAD_THRESHOLD)
            self._upload_multipart(local_path, destination_s3_path, s3_bucket, key, source_size_bytes)

    def _upload_single(self, local_path, s3_bucket, key):
        """
        Write a local file to an S3 key using single PUT.

        This only works for files < 5GB in size.
        """
        s3_key = Key(s3_bucket)
        s3_key.key = key
        # Explicitly set the ACL policy when putting the object, so
        # that it has an ACL when AWS writes to keys from another account.
        s3_key.set_contents_from_filename(local_path, policy=DEFAULT_KEY_ACCESS_POLICY)

    def _upload_multipart(self, local_path, destination_s3_path, s3_bucket, key, source_size_bytes):
        """Upload a large local file to an S3 path, using S3's multipart upload API."""

        # Explicitly set the ACL policy when putting the object, so
        # that it has an ACL when AWS writes to keys from another account.
        multipart = s3_bucket.initiate_multipart_upload(key, policy=DEFAULT_KEY_ACCESS_POLICY)

        number_of_chunks, bytes_per_chunk = self._get_chunk_specs(source_size_bytes)
        log.info("Uploading file '%s' with size %d in %d parts, with chunksize of %d.",
                 destination_s3_path, source_size_bytes, number_of_chunks, bytes_per_chunk)

        chunk_generator = self._generate_chunks(source_size_bytes, number_of_chunks, bytes_per_chunk)
        for part_num, chunk_byte_offset, num_bytes in chunk_generator:
            with FileChunkIO(local_path, 'r', offset=chunk_byte_offset, bytes=num_bytes) as chunk:
                multipart.upload_part_from_file(fp=chunk, part_num=part_num)

        if len(multipart.get_all_parts()) == number_of_chunks:
            multipart.complete_upload()
        else:
            multipart.cancel_upload()

    def _get_chunk_specs(self, source_size_bytes):
        """Returns number of chunks and bytes-per-chunk given a filesize."""
        # Select a chunk size, so that the chunk size grows with the overall size, but
        # more slowly.  (Scale so that it equals the minimum chunk size.)
        bytes_per_chunk = int(math.sqrt(MINIMUM_BYTES_PER_CHUNK) * math.sqrt(source_size_bytes))
        bytes_per_chunk = min(max(bytes_per_chunk, MINIMUM_BYTES_PER_CHUNK), MULTIPART_UPLOAD_THRESHOLD)
        number_of_chunks = int(math.ceil(source_size_bytes / float(bytes_per_chunk)))
        return number_of_chunks, bytes_per_chunk

    def _generate_chunks(self, source_size_bytes, number_of_chunks, bytes_per_chunk):
        """Returns the index, offset, and size of chunks."""
        for chunk_index in range(number_of_chunks):
            chunk_byte_offset = chunk_index * bytes_per_chunk
            remaining_bytes_in_file = source_size_bytes - chunk_byte_offset
            num_bytes = min([bytes_per_chunk, remaining_bytes_in_file])
            # indexing of parts is one-based.
            yield chunk_index + 1, chunk_byte_offset, num_bytes


class S3HdfsTarget(HdfsTarget):
    """HDFS target that supports writing and reading files directly in S3."""

    # Luigi does not support writing to HDFS targets that point to complete URLs like "s3://foo/bar" it only supports
    # HDFS paths that look like standard file paths "/foo/bar".

    # (This class also provides a customized implementation for S3Client.)

    # TODO: Fix the upstream bug in luigi that prevents writing to HDFS files that are specified by complete URLs

    def __init__(self, path=None, format=Plain, is_tmp=False):
        super(S3HdfsTarget, self).__init__(path=path, format=format, is_tmp=is_tmp)

    def open(self, mode='r'):
        if mode not in ('r', 'w'):
            raise ValueError("Unsupported open mode '{mode}'".format(mode=mode))

        if mode == 'r':
            return super(S3HdfsTarget, self).open(mode=mode)
        else:
            safe_path = self.path.replace('s3n://', 's3://')
            if not hasattr(self, 's3_client'):
                self.s3_client = ScalableS3Client()
            return AtomicS3File(safe_path, self.s3_client)
