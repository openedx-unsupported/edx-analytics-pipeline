"""
Determine the number of users in each country.
"""
import datetime
import tempfile

import luigi
import pygeoip

import edx.analytics.tasks.util.eventlog as eventlog
from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.pathutil import PathSetTask
from edx.analytics.tasks.url import get_target_from_url, url_path_join, ExternalURL

import logging
log = logging.getLogger(__name__)

UNKNOWN_COUNTRY = "UNKNOWN"
UNKNOWN_CODE = "UNKNOWN"


class GeolocationMixin(object):
    """
    Defines parameters needed for geolocation lookups.

    """
    geolocation_data = luigi.Parameter(
        config_path={'section': 'geolocation', 'name': 'geolocation_data'},
        description='A URL to the location of country-level geolocation data.',
    )

class GeolocationTask(object):

    geoip = None

    def geolocation_data_target(self):
        """Defines target from which geolocation data can be read."""
        raise NotImplementedError

    def init_reducer(self):
        super(GeolocationTask, self).init_reducer()
        # Copy the remote version of the geolocation data file to a local file.
        # This is required by the GeoIP call, which assumes that the data file is located
        # on a local file system.
        self.temporary_data_file = tempfile.NamedTemporaryFile(prefix='geolocation_data')
        with self.geolocation_data_target().open() as geolocation_data_input:
            while True:
                transfer_buffer = geolocation_data_input.read(1024)
                if transfer_buffer:
                    self.temporary_data_file.write(transfer_buffer)
                else:
                    break
        self.temporary_data_file.seek(0)

        self.geoip = pygeoip.GeoIP(self.temporary_data_file.name, pygeoip.STANDARD)

    def final_reducer(self):
        """Clean up after the reducer is done."""
        del self.geoip
        self.temporary_data_file.close()

        return tuple()

class BaseUserLocationTask(GeolocationMixin):
    """
    Defines parameters needed for user location tasks.

    """
    name = luigi.Parameter(
        description='A unique identifier to distinguish one run from another.  It is used in '
        'the construction of output filenames, so each run will have distinct outputs.',
    )
    src = luigi.Parameter(
        is_list=True,
        description='A list of URLs to the root location of input tracking log files.',
    )
    dest = luigi.Parameter(
        description='A URL to the root location to write output file(s).',
    )
    include = luigi.Parameter(
        is_list=True,
        default=('*',),
        description='A list of patterns to be used to match input files, relative to `src` URL. '
        'The default value is [\'*\'].',
    )
    manifest = luigi.Parameter(
        default=None,
        description='A URL to a file location that can store the complete set of input files. '
        'A manifest file is required by hadoop if there are too many input paths. It hits an operating system '
        'limit on the number of arguments passed to the mapper process on the task nodes.',
    )
    end_date = luigi.DateParameter(
        description='Events before or on this date are kept, and after this date are filtered out.',
    )


class BaseGeolocation(GeolocationTask):
    """Base class for performing geolocation map-reduce tasks."""

    def reducer(self, key, values):
        """Outputs country for last ip address associated with a user."""

        # DON'T presort input values (by timestamp).  The data potentially takes up too
        # much memory.  Scan the input values instead.

        # We assume the timestamp values (strings) are in ISO
        # representation, so that they can be compared as strings.
        username = key
        last_ip = None
        last_timestamp = ""
        for timestamp, ip_address in values:
            if timestamp > last_timestamp:
                last_ip = ip_address
                last_timestamp = timestamp

        if not last_ip:
            return

        # This ip address might not provide a country name.
        try:
            country = self.geoip.country_name_by_addr(last_ip)
            code = self.geoip.country_code_by_addr(last_ip)
        except Exception:
            log.exception("Encountered exception getting country:  user '%s', last_ip '%s' on '%s'.",
                          username, last_ip, last_timestamp)
            country = UNKNOWN_COUNTRY
            code = UNKNOWN_CODE

        if country is None or len(country.strip()) <= 0:
            log.error("No country found for user '%s', last_ip '%s' on '%s'.", username, last_ip, last_timestamp)
            # TODO: try earlier IP addresses, if we find this happens much.
            country = UNKNOWN_COUNTRY

        if code is None or len(code.strip()) <= 0:
            log.error("No code found for user '%s', last_ip '%s', country '%s' on '%s'.",
                      username, last_ip, country, last_timestamp)
            # TODO: try earlier IP addresses, if we find this happens much.
            code = UNKNOWN_CODE

        # Add the username for debugging purposes.  (Not needed for counts.)
        yield (country.encode('utf8'), code.encode('utf8')), username.encode('utf8')

    def extra_modules(self):
        """Pygeoip is required by all tasks that load this file."""
        return [pygeoip]


class LastCountryForEachUser(BaseGeolocation, MapReduceJobTask, BaseUserLocationTask):
    """ Identifies the country of the last IP address associated with each user."""
    # TODO: This should be phased out in favor of the
    # LastCountryOfUser task that uses the more standard
    # EventLogSelectionMixin approach.  However, the
    # EventLogSelectionMixin approach currently supports running only
    # on files that contain dates, so it would not get the same
    # results as this task.  Once the new geolocation can either run
    # on the same files or we can accept running on different
    # (i.e. only dated files), then we can get rid of the old
    # geolocation.  Either step would require validation to confirm.

    def __init__(self, *args, **kwargs):
        super(LastCountryForEachUser, self).__init__(*args, **kwargs)

        # end_datetime is midnight of the day after the day to be included.
        end_date_exclusive = self.end_date + datetime.timedelta(1)
        self.end_datetime = datetime.datetime(end_date_exclusive.year, end_date_exclusive.month, end_date_exclusive.day)

    def requires(self):
        results = {
            'events': PathSetTask(self.src, self.include, self.manifest),
            'geoloc_data': ExternalURL(self.geolocation_data),
        }
        return results

    def requires_local(self):
        return self.requires()['geoloc_data']

    def requires_hadoop(self):
        # Only pass the input files on to hadoop, not any data file.
        return self.requires()['events']

    def geolocation_data_target(self):
        return self.input()['geoloc_data']

    def output(self):
        output_name = u'last_country_for_each_user_{name}/'.format(name=self.name)
        return get_target_from_url(url_path_join(self.dest, output_name))

    def mapper(self, line):
        event = eventlog.parse_json_event(line)
        if event is None:
            return

        username = event.get('username')
        if not username:
            return

        stripped_username = username.strip()
        if username != stripped_username:
            log.error("User '%s' has extra whitespace, which is being stripped. Event: %s", username, event)
            username = stripped_username

        timestamp_as_datetime = eventlog.get_event_time(event)
        if timestamp_as_datetime is None:
            return

        if timestamp_as_datetime >= self.end_datetime:
            return

        timestamp = eventlog.datetime_to_timestamp(timestamp_as_datetime)

        ip_address = event.get('ip')
        if not ip_address:
            log.warning("No ip_address found for user '%s' on '%s'.", username, timestamp)
            return

        yield username, (timestamp, ip_address)


class UsersPerCountry(MapReduceJobTask, BaseUserLocationTask):
    """
    Counts number of unique users per country, using a user's last IP address.

    Most parameters are passed through to :py:class:`LastCountryForEachUser`.

    Additional parameter:
        base_input_format: value of input_format to be passed to :py:class:`LastCountryForEachUser`.

    """
    base_input_format = luigi.Parameter(default=None)

    def requires(self):
        return LastCountryForEachUser(
            mapreduce_engine=self.mapreduce_engine,
            lib_jar=self.lib_jar,
            input_format=self.base_input_format,
            n_reduce_tasks=self.n_reduce_tasks,
            src=self.src,
            dest=self.dest,
            include=self.include,
            name=self.name,
            manifest=self.manifest,
            geolocation_data=self.geolocation_data,
            end_date=self.end_date,
        )

    def output(self):
        output_name = u'users_per_country_{name}/'.format(name=self.name)
        return get_target_from_url(url_path_join(self.dest, output_name))

    def mapper(self, line):
        """Replace username with count of 1 for summing."""
        country, code, _username = line.split('\t')
        if len(country) > 0:
            yield (country, code), 1

    def reducer(self, key, values):
        """Sum counts over countries, and append date of current run to each entry."""
        yield key, sum(values), self.end_date

    # The size of mapper outputs can be shrunk by defining the
    # combiner to generate sums for each country coming out of each
    # mapper.  The reducer then only needs to sum the partial sums.
    combiner = reducer

    def extra_modules(self):
        """Pygeoip is required by all tasks that load this file."""
        return [pygeoip]


class UsersPerCountryReport(luigi.Task):
    """
    Calculates TSV file containing number of users per country.

    """
    counts = luigi.Parameter(
        description='Location of counts per country. The format is a Hadoop '
        'TSV file, with fields `country`, `count`, and `date`.',
    )
    report = luigi.Parameter(
        description='Location of the resulting report. The output format is a '
        'Excel CSV file with `country` and `count`.',
    )

    def requires(self):
        return ExternalURL(self.counts)

    def output(self):
        return get_target_from_url(self.report)

    @classmethod
    def create_header(cls, date):
        """Generate a header for CSV output."""
        fields = ['percent', 'count', 'country', 'code', 'date={date}'.format(date=date)]
        return ','.join(fields)

    @classmethod
    def create_csv_entry(cls, percent, count, country, code):
        """Generate a single entry in CSV format."""
        return '{percent:.4f},{count},"{country}",{code}'.format(
            percent=percent, count=count, country=country, code=code
        )

    def run(self):
        # Provide default values for when no counts are available.
        counts = []
        date = "UNKNOWN"
        total = 0
        with self.input().open('r') as input_file:
            for line in input_file.readlines():
                country, code, count, date = line.split('\t')
                counts.append((count, country, code))
                date = date.strip()
                total += int(count)

        # Write out the counts as a CSV, in reverse order of counts.
        with self.output().open('w') as output_file:
            output_file.write(self.create_header(date))
            output_file.write('\n')
            for count, country, code in sorted(counts, reverse=True, key=lambda k: int(k[0])):
                percent = float(count) / float(total)
                output_file.write(self.create_csv_entry(percent, count, country, code))
                output_file.write('\n')

    def extra_modules(self):
        """Pygeoip is required by all tasks that load this file."""
        return [pygeoip]


class UsersPerCountryReportWorkflow(MapReduceJobTaskMixin, UsersPerCountryReport):
    """
    Generates report containing number of users per location (country).

    """
    name = luigi.Parameter(
        description='A unique identifier to distinguish one run from another.  It is used in '
        'the construction of output filenames, so each run will have distinct outputs.',
    )
    src = luigi.Parameter(
        is_list=True,
        description='A list of URLs to the root location of input tracking log files.',
    )
    include = luigi.Parameter(
        is_list=True,
        default=('*',),
        description='A list of patterns to be used to match input files, relative to `src` URL. '
        'The default value is [\'*\'].',
    )
    manifest = luigi.Parameter(
        default=None,
        description='A URL to a file location that can store the complete set of input files.',
    )
    base_input_format = luigi.Parameter(
        default=None,
        description='Events before or on this date are kept, and after this date are filtered out.',
    )
    end_date = luigi.DateParameter(
        description='A URL to the location of country-level geolocation data.',
    )
    geolocation_data = luigi.Parameter()

    def requires(self):
        return UsersPerCountry(
            mapreduce_engine=self.mapreduce_engine,
            lib_jar=self.lib_jar,
            base_input_format=self.base_input_format,
            n_reduce_tasks=self.n_reduce_tasks,
            src=self.src,
            dest=self.counts,
            include=self.include,
            name=self.name,
            manifest=self.manifest,
            geolocation_data=self.geolocation_data,
            end_date=self.end_date,
        )
