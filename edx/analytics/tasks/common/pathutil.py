"""
Helper classes to specify file dependencies for input and output.

Supports inputs from S3 and local FS.
Supports outputs to HDFS, S3, and local FS.

"""

import datetime
import fnmatch
import logging
import os
import re

import luigi
import luigi.contrib.hdfs
import luigi.contrib.hdfs.format
import luigi.task
from luigi.date_interval import Custom

from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.s3_util import ScalableS3Client, generate_s3_sources, get_s3_bucket_key_names
from edx.analytics.tasks.util.url import ExternalURL, UncheckedExternalURL, get_target_from_url, url_path_join

log = logging.getLogger(__name__)


class PathSetTask(luigi.Task):
    """
    A task to select a subset of files in an S3 bucket or local FS.

    """
    src = luigi.ListParameter(
        config_path={'section': 'event-logs', 'name': 'source'},
        description='A URL pointing to a folder in s3:// or local FS.',
    )
    include = luigi.ListParameter(
        default=('*',),
        description='A list of patterns to use to select.  Multiple patterns are OR\'d.',
    )
    manifest = luigi.Parameter(
        default=None,
        description='A URL pointing to a manifest file location.',
    )
    include_zero_length = luigi.BoolParameter(
        default=False,
        description='If True, include files/directories with size zero.',
    )

    def __init__(self, *args, **kwargs):
        super(PathSetTask, self).__init__(*args, **kwargs)
        self.s3_conn = None

    def generate_file_list(self):
        """Yield each individual path given a source folder and a set of file-matching expressions."""
        for src in self.src:
            if src.startswith('s3'):
                # connect lazily as needed:
                if self.s3_conn is None:
                    self.s3_conn = ScalableS3Client().s3
                for _bucket, _root, path in generate_s3_sources(self.s3_conn, src, self.include, self.include_zero_length):
                    source = url_path_join(src, path)
                    yield ExternalURL(source)
            elif src.startswith('hdfs'):
                for source, size in luigi.contrib.hdfs.listdir(src, recursive=True, include_size=True):
                    if not self.include_zero_length and size == 0:
                        continue
                    elif any(fnmatch.fnmatch(source, include_val) for include_val in self.include):
                        yield ExternalURL(source)
            else:
                # Apply the include patterns to the relative path below the src directory.
                # TODO: implement exclude_zero_length to match S3 case.
                for dirpath, _dirnames, files in os.walk(src):
                    for filename in files:
                        filepath = os.path.join(dirpath, filename)
                        relpath = os.path.relpath(filepath, src)
                        if any(fnmatch.fnmatch(relpath, include_val) for include_val in self.include):
                            yield ExternalURL(filepath)

    def manifest_file_list(self):
        """Write each individual path to a manifest file and yield the path to that file."""
        manifest_target = get_target_from_url(self.manifest)
        if not manifest_target.exists():
            with manifest_target.open('w') as manifest_file:
                for external_url_task in self.generate_file_list():
                    manifest_file.write(external_url_task.url + '\n')

        yield ExternalURL(self.manifest)

    def requires(self):
        if self.manifest is not None:
            return self.manifest_file_list()
        else:
            return self.generate_file_list()

    def complete(self):
        # An optimization: just declare that the task is always
        # complete, by definition, because it is whatever files were
        # requested that match the filter, not a set of files whose
        # existence needs to be checked or generated again.
        return True

    def output(self):
        return [task.output() for task in self.requires()]


class EventLogSelectionDownstreamMixin(object):
    """Defines parameters for passing upstream to tasks that use EventLogSelectionMixin."""

    source = luigi.ListParameter(
        config_path={'section': 'event-logs', 'name': 'source'},
        description='A URL to a path that contains log files that contain the events. (e.g., s3://my_bucket/foo/).',
    )
    interval = luigi.DateIntervalParameter(
        description='The range of dates to export logs for.',
    )
    expand_interval = luigi.TimeDeltaParameter(
        config_path={'section': 'event-logs', 'name': 'expand_interval'},
        description='A time interval to add to the beginning and end of the interval to expand the windows of '
        'files captured.',
    )
    pattern = luigi.ListParameter(
        config_path={'section': 'event-logs', 'name': 'pattern'},
        description='A regex with a named capture group for the date that approximates the date that the events '
        'within were emitted. Note that the search interval is expanded, so events don\'t have to be in exactly '
        'the right file in order for them to be processed.',
    )

    date_pattern = luigi.Parameter(
        default='%Y%m%d',
        description='The format of the date as it appears in the source file name. Note that this correlates with the '
        'named capture group for date in the pattern parameter. This is intended to select relevant event log files '
        'by making sure the date is within the interval.',
    )


class PathSelectionByDateIntervalTask(EventLogSelectionDownstreamMixin, luigi.WrapperTask):
    """
    Select all relevant event log input files from a directory.

    Recursively list all files in the directory which is expected to contain the input files organized in such a way
    that a pattern can be used to find them. Filenames are expected to contain a date which represents an approximation
    of the date found in the events themselves.

    """

    def __init__(self, *args, **kwargs):
        super(PathSelectionByDateIntervalTask, self).__init__(*args, **kwargs)
        self.interval = Custom(
            self.interval.date_a - self.expand_interval,
            self.interval.date_b + self.expand_interval
        )
        self.requirements = None

    def requires(self):
        # This method gets called several times. Avoid making multiple round trips to S3 by caching the first result.
        if self.requirements is None:
            log.debug('No saved requirements found, refreshing requirements list.')
            self.requirements = self._get_requirements()
        else:
            log.debug('Using cached requirements.')
        return self.requirements

    def _get_requirements(self):
        """
        Gather the set of requirements needed to run the task.

        This can be a rather expensive operation that requires usage of the S3 API to list all files in the source
        bucket and select the ones that are applicable to the given date range.
        """
        url_gens = []
        for source in self.source:
            if source.startswith('s3'):
                url_gens.append(self._get_s3_urls(source))
            elif source.startswith('hdfs'):
                url_gens.append(self._get_hdfs_urls(source))
            else:
                url_gens.append(self._get_local_urls(source))

        log.debug('Matching urls using pattern(s)="%s"', self.pattern)
        log.debug(
            'Date interval: %s <= date < %s', self.interval.date_a.isoformat(), self.interval.date_b.isoformat()
        )

        return [UncheckedExternalURL(url) for url_gen in url_gens for url in url_gen if self.should_include_url(url)]

    def _get_s3_urls(self, source):
        """Recursively list all files inside the source URL directory."""
        s3_conn = ScalableS3Client().s3
        bucket_name, root = get_s3_bucket_key_names(source)
        bucket = s3_conn.get_bucket(bucket_name)
        for key_metadata in bucket.list(root):
            if key_metadata.size > 0:
                key_path = key_metadata.key[len(root):].lstrip('/')
                yield url_path_join(source, key_path)

    def _get_hdfs_urls(self, source):
        """Recursively list all files inside the source directory on the hdfs filesystem."""
        if luigi.contrib.hdfs.exists(source):
            # listdir raises an exception if the source doesn't exist.
            for source in luigi.contrib.hdfs.listdir(source, recursive=True):
                yield source

    def _get_local_urls(self, source):
        """Recursively list all files inside the source directory on the local filesystem."""
        for directory_path, _subdir_paths, filenames in os.walk(source):
            for filename in filenames:
                yield os.path.join(directory_path, filename)

    def should_include_url(self, url):
        """
        Determine whether the file pointed to by the URL should be included in the set of files used for analysis.

        Presently filters first on pattern match and then on the datestamp extracted from the file name.
        """
        # Find the first pattern (if any) that matches the URL.
        match = None
        for pattern in self.pattern:
            match = re.match(pattern, url)
            if match:
                break

        if not match:
            return False

        # If the pattern contains a date group, use that to check if within the requested interval.
        # If instead the pattern contains a timestamp group, use that instead to check if within the requested interval.
        # If it doesn't contain either such group, then assume that it should be included.
        should_include = True
        if 'date' in match.groupdict():
            parsed_datetime = datetime.datetime.strptime(match.group('date'), self.date_pattern)
            parsed_date = datetime.date(parsed_datetime.year, parsed_datetime.month, parsed_datetime.day)
            should_include = parsed_date in self.interval
        elif 'timestamp' in match.groupdict():
            timestamp = int(match.group('timestamp'))
            parsed_datetime = datetime.datetime.utcfromtimestamp(timestamp)
            parsed_date = datetime.date(parsed_datetime.year, parsed_datetime.month, parsed_datetime.day)
            should_include = parsed_date in self.interval

        return should_include

    def output(self):
        return [task.output() for task in self.requires()]


class EventLogSelectionMixin(EventLogSelectionDownstreamMixin):
    """
    Extract events corresponding to a specified time interval and outputs them from a mapper.

    """

    def requires(self):
        """Use PathSelectionByDateIntervalTask to define inputs."""
        return PathSelectionByDateIntervalTask(
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            date_pattern=self.date_pattern,
        )

    def init_local(self):
        """Convert intervals to date strings for alpha-numeric comparison."""
        super(EventLogSelectionMixin, self).init_local()
        self.lower_bound_date_string = self.interval.date_a.strftime('%Y-%m-%d')  # pylint: disable=no-member
        self.upper_bound_date_string = self.interval.date_b.strftime('%Y-%m-%d')  # pylint: disable=no-member

    def get_event_and_date_string(self, line):
        """Default mapper implementation, that always outputs the log line, but with a configurable key."""
        event = eventlog.parse_json_event(line)
        if event is None:
            self.incr_counter('Event', 'Discard Unparseable Event', 1)
            return None

        event_time = self.get_event_time(event)
        if not event_time:
            self.incr_counter('Event', 'Discard Missing Time Field', 1)
            return None

        # Don't use strptime to parse the date, it is extremely slow
        # to do so. Instead rely on alphanumeric comparisons.  The
        # timestamp is ISO8601 formatted, so dates will look like
        # %Y-%m-%d.  For example: 2014-05-20.
        date_string = event_time.split("T")[0]

        if date_string < self.lower_bound_date_string or date_string >= self.upper_bound_date_string:
            # Slow: self.incr_counter('Event', 'Discard Outside Date Interval', 1)
            return None

        return event, date_string

    def get_event_time(self, event):
        """Returns time information from event if present, else returns None."""
        try:
            return event['time']
        except KeyError:
            return None

    def get_map_input_file(self):
        """Get the name of the input file from Hadoop."""
        # Hadoop sets an environment variable with the full URL of the input file. This url will be something like:
        # s3://bucket/root/host1/tracking.log.gz. In this example, assume self.source is "s3://bucket/root".
        try:
            return os.environ['mapreduce_map_input_file']
        except KeyError:
            try:
                # Older versions of Hadoop support a deprecated key, so also try that.
                return os.environ['map_input_file']
            except KeyError:
                log.warn('mapreduce_map_input_file not defined in os.environ, unable to determine input file path')
                self.incr_counter('Event', 'Missing map_input_file', 1)
                return ''
