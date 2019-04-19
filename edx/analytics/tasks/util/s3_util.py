"""
Utility methods for interacting with S3 via boto.
"""
import logging
import os
import time
from fnmatch import fnmatch
from urlparse import urlparse, urlunparse

from luigi.contrib.hdfs.format import Plain
from luigi.contrib.hdfs.target import HdfsTarget
from luigi.contrib.s3 import AtomicS3File, S3Client

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


# TODO: Once we upgrade to boto3 (luigi>=2.7.6), delete this class! In boto3/luigi>=2.7.6, we must
# NOT pass `host` to the s3 client or else it will throw a KeyError.  boto3 will already default to
# s3.amazonaws.com.
class ScalableS3Client(S3Client):
    """
    S3 client that adds support for defaulting host name to s3.amazonaws.com.
    """

    def __init__(self, *args, **kwargs):
        if 'host' not in kwargs:
            kwargs['host'] = self._get_s3_config('host') or 's3.amazonaws.com'

        super(ScalableS3Client, self).__init__(*args, **kwargs)


class S3HdfsTarget(HdfsTarget):
    """HDFS target that supports writing and reading files directly in S3."""

    # Luigi does not support writing to HDFS targets that point to complete URLs like "s3://foo/bar" it only supports
    # HDFS paths that look like standard file paths "/foo/bar".

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
            return AtomicS3File(safe_path, self.s3_client, policy=DEFAULT_KEY_ACCESS_POLICY)


def canonicalize_s3_url(url):
    """
    Convert the given s3 URL into a form which is safe to use with external tools.

    Specifically, URL Schemes such as "s3+https" are unrecognized by gsutil and Snowflake, and must
    be converted to "s3".

    Args:
        url (str): An s3 URL.

    Raises:
        ValueError: if the scheme of the input url is unrecognized as S3 at all.
    """
    parsed_url = urlparse(url)
    if parsed_url.scheme == 's3':
        canonical_url = url  # Simple passthrough, no change needed.
    elif parsed_url.scheme == 's3+https':
        new_url_parts = parsed_url._replace(scheme='s3')
        canonical_url = urlunparse(new_url_parts)
    else:
        raise ValueError(
            'The URL scheme "{}" does not appear to be an S3 URL scheme.'.format(parsed_url.scheme)
        )
    return canonical_url
