"""
Support URLs.  Specifically, we want to be able to refer to data stored in a
variety of locations and formats using a standard URL syntax.

Examples::

    s3://some-bucket/path/to/file
    /path/to/local/file.gz
    hdfs://some/directory/
"""
from __future__ import absolute_import

import os
import urlparse

import luigi
import luigi.configuration
import luigi.format
import luigi.hdfs
import luigi.s3
from luigi.hdfs import HdfsTarget
from luigi.s3 import S3Target

from edx.analytics.tasks.util.s3_util import ScalableS3Client, S3HdfsTarget


class MarkerMixin(object):
    """This mixin handles Targets that cannot accurately be measured by the existence of data files, and instead need
    another positive marker to indicate Task success."""

    def exists(self):  # pragma: no cover
        """Completion of this target is based solely on the existence of the marker file."""
        return self.fs.exists(self.path + "/_SUCCESS")

    def touch_marker(self):  # pragma: no cover
        """Generate the marker file using file system native to the parent Target."""
        marker = self.__class__(path=self.path + "/_SUCCESS")
        marker.open("w").close()


class S3MarkerTarget(MarkerMixin, S3Target):
    """An S3 Target that uses a marker file to indicate success."""


class HdfsMarkerTarget(MarkerMixin, HdfsTarget):
    """An HDFS Target that uses a marker file to indicate success."""


class LocalMarkerTarget(MarkerMixin, luigi.LocalTarget):
    """A Local Target that uses a marker file to indicate success."""


class S3HdfsMarkerTarget(MarkerMixin, S3HdfsTarget):
    """An S3 HDFS Target that uses a marker file to indicate success."""


class ExternalURL(luigi.ExternalTask):
    """Simple Task that returns a target based on its URL"""
    url = luigi.Parameter()

    def output(self):
        return get_target_from_url(self.url)


class UncheckedExternalURL(ExternalURL):
    """A ExternalURL task that does not verify if the source file exists, which can be expensive for S3 URLs."""

    def complete(self):
        return True


class IgnoredTarget(luigi.hdfs.HdfsTarget):
    """Dummy target for use in Hadoop jobs that produce no explicit output file."""
    def __init__(self):
        super(IgnoredTarget, self).__init__(is_tmp=True)

    def exists(self):
        return False

    def open(self, mode='r'):
        return open('/dev/null', mode)


DEFAULT_TARGET_CLASS = luigi.LocalTarget
URL_SCHEME_TO_TARGET_CLASS = {
    'hdfs': luigi.hdfs.HdfsTarget,
    's3': S3HdfsTarget,
    's3n': S3HdfsTarget,
    'file': luigi.LocalTarget,
    's3+https': luigi.s3.S3Target,
}

DEFAULT_MARKER_TARGET_CLASS = LocalMarkerTarget
URL_SCHEME_TO_MARKER_TARGET_CLASS = {
    'hdfs': HdfsMarkerTarget,
    's3': S3HdfsMarkerTarget,
    's3n': S3HdfsMarkerTarget,
    'file': LocalMarkerTarget,
    's3+https': S3MarkerTarget,
}


def get_target_class_from_url(url, marker=False):
    """Returns a luigi target class based on the url scheme"""
    parsed_url = urlparse.urlparse(url)

    if marker:
        target_class = URL_SCHEME_TO_MARKER_TARGET_CLASS.get(parsed_url.scheme, DEFAULT_MARKER_TARGET_CLASS)
    else:
        target_class = URL_SCHEME_TO_TARGET_CLASS.get(parsed_url.scheme, DEFAULT_TARGET_CLASS)

    kwargs = {}
    if issubclass(target_class, luigi.hdfs.HdfsTarget) and url.endswith('/'):
        kwargs['format'] = luigi.hdfs.PlainDir
    if issubclass(target_class, luigi.LocalTarget) or parsed_url.scheme == 'hdfs':
        # LocalTarget and HdfsTarget both expect paths without any scheme, netloc etc, just bare paths. So strip
        # everything else off the url and pass that in to the target.
        url = parsed_url.path
    if issubclass(target_class, luigi.s3.S3Target):
        kwargs['client'] = ScalableS3Client()

    url = url.rstrip('/')
    args = (url,)

    return target_class, args, kwargs


def get_target_from_url(url, marker=False):
    """Returns a luigi target based on the url scheme"""
    cls, args, kwargs = get_target_class_from_url(url, marker)
    return cls(*args, **kwargs)


def url_path_join(url, *extra_path):
    """
    Extend the path component of the given URL.  Relative paths extend the
    existing path, absolute paths replace it.  Special path elements like '.'
    and '..' are not treated any differently than any other path element.

    Examples:

        url=http://foo.com/bar, extra_path=baz -> http://foo.com/bar/baz
        url=http://foo.com/bar, extra_path=/baz -> http://foo.com/baz
        url=http://foo.com/bar, extra_path=../baz -> http://foo.com/bar/../baz

    Args:

        url (str): The URL to modify.
        extra_path (str): The path to join with the current URL path.

    Returns:
        The URL with the path component joined with `extra_path` argument.
    """
    (scheme, netloc, path, params, query, fragment) = urlparse.urlparse(url)
    joined_path = os.path.join(path, *extra_path)
    return urlparse.urlunparse((scheme, netloc, joined_path, params, query, fragment))
