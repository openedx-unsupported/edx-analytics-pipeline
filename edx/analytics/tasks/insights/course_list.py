"""
Store course details sourced from the Courses API into a hive table.

See the CourseListApiDataTask and CourseListPartitionTask for details.
"""

import datetime
import json
import logging

import luigi

from edx.analytics.tasks.common.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.util.edx_api_client import EdxApiClient
from edx.analytics.tasks.util.hive import BareHiveTableTask, HivePartitionTask, WarehouseMixin
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.record import DateTimeField, Record, StringField
from edx.analytics.tasks.util.url import get_target_from_url, url_path_join

log = logging.getLogger(__name__)


class CourseRecord(Record):
    """
    Represents a single course's details as fetched from the edX Courses REST API.
    """
    course_id = StringField(nullable=False, length=255, description='Course identifier.')
    name = StringField(nullable=False, length=255, truncate=True, normalize_whitespace=True,
                       description='Course name, truncated to 255 characters.')
    org = StringField(nullable=False, length=255, description='Course organization.')
    number = StringField(nullable=False, length=255, description='Course number.')
    blocks_url = StringField(nullable=False, description='URL of the course\'s blocks')
    short_description = StringField(nullable=True, length=255, truncate=True, normalize_whitespace=True,
                                    description='Short course description, truncated to 255 characters.')
    enrollment_start = DateTimeField(nullable=True, description='Enrollment start date.')
    enrollment_end = DateTimeField(nullable=True, description='Enrollment end date.')
    start_date = DateTimeField(nullable=True, description='Course start date.')
    end_date = DateTimeField(nullable=True, description='Course end date.')
    start_display = StringField(nullable=True, length=255, normalize_whitespace=True,
                                description='Course start date description.')
    start_type = StringField(nullable=True, length=255, normalize_whitespace=True,
                             description='Indicates how start_display was set, e.g. "string", "timestamp", "empty".')
    effort = StringField(nullable=True, length=255, truncate=True, normalize_whitespace=True,
                         description='Description of effort required, truncated to 255 characters.')
    pacing = StringField(nullable=True, length=255, normalize_whitespace=True,
                         description='Description of course pacing strategy.')


class TimestampPartitionMixin(object):
    """
    This mixin provides its task with a formatted datetime partition value property.

    The partition value is the `datetime` parameter, formatted by the `partition_date` parameter.

    It can be used by HivePartitionTasks and tasks which invoke downstream HivePartitionTasks.
    """
    datetime = luigi.DateSecondParameter(
        default=datetime.datetime.utcnow(),
        description='Date/time for the data partition.  Default is UTC now.'
    )
    partition_format = luigi.Parameter(
        config_path={'section': 'course-list', 'name': 'partition_format'},
        default='%Y-%m-%d',
        description='Format string for the course list table partition\'s `datetime` parameter. '
                    'Must result in a filename-safe string, or your partitions will fail to be created.\n'
                    'The default value of "%Y-%m-%d" changes daily, and so causes a new course partition to to be '
                    'created once a day.  For example, use "%Y-%m-%dT%H" to update hourly, though beware of load on '
                    'the edX REST API.  See strftime for options.',
    )

    @property
    def partition_value(self):
        """Partition based on the task's datetime and partition format strings."""
        return unicode(self.datetime.strftime(self.partition_format))


class CourseListDownstreamMixin(TimestampPartitionMixin, WarehouseMixin, OverwriteOutputMixin):
    """Common parameters used by the Course List Data and Partition tasks."""
    pass


class PullCourseListApiData(CourseListDownstreamMixin, luigi.Task):
    """
    This task fetches the courses list from the Courses edX REST API, and
    writes one line of JSON for each course to the output() target.

    See the EdxRestClient to configure the REST API connection parameters.
    """
    api_root_url = luigi.Parameter(
        config_path={'section': 'course-list', 'name': 'api_root_url'},
        description="The base URL for the courses API. This URL should look like"
                    "https://lms.example.com/api/v1/courses/"
    )
    api_page_size = luigi.IntParameter(
        config_path={'section': 'course-list', 'name': 'page_size'},
        significant=False,
        default=100,
        description="The number of records to request from the API in each HTTP request."
    )

    def run(self):
        self.remove_output_on_overwrite()
        client = EdxApiClient()
        params = {
            'page_size': self.api_page_size,
        }

        def _pagination(response):
            """Gets the next URL from the course list API response."""
            return response.get('pagination', {}).get('next')

        counter = 0
        with self.output().open('w') as output_file:
            for response in client.paginated_get(self.api_root_url, params=params, pagination_key=_pagination):
                parsed_response = response.json()
                for course in parsed_response.get('results', []):
                    output_file.write(json.dumps(course))
                    output_file.write('\n')
                    counter += 1

        log.info('Wrote %d records to output file', counter)

    def output(self):
        return get_target_from_url(
            url_path_join(
                self.hive_partition_path('course_list_raw', partition_value=self.partition_value),
                'course_list.json'
            )
        )


class CourseListApiDataTask(CourseListDownstreamMixin, MapReduceJobTask):
    """
    This task processes the data returned by the Course List API into CourseRecord string tuples.

    """
    output_root = luigi.Parameter(
        description='URL where the map reduce data should be stored.',
    )

    # Write the output directly to the final destination and rely on the _SUCCESS file to indicate
    # whether or not it is complete. Note that this is a custom extension to luigi.
    enable_direct_output = True

    def requires(self):
        return PullCourseListApiData(datetime=self.datetime, overwrite=self.overwrite)

    def mapper(self, line):
        """
        Load each line of JSON-formatted Course data, and discard any invalid lines.

        Yields a 2-element tuple for each valid course containing the course's ID and the parsed JSON data as a dict.
        """
        if line is None:
            return

        course = json.loads(line)

        # eucalyptus API uses 'id' instead of 'course_id'
        if 'id' in course:
            course_id = course['id']
            del course['id']
            course['course_id'] = course_id

        else:
            course_id = course.get('course_id')

        if course_id is not None:
            course['course_id'] = course_id
            yield (course_id, course)
        else:
            log.error('Unable to read course data from "%s"', line)

    def reducer(self, _key, values):
        """
        Takes the JSON course data and stores it as a CourseRecord.

        Yields the CourseRecord as a tuple.
        """
        # Note that there should only ever be one record in the values list,
        # since the Course API returns one result per course.
        for course_data in values:
            fields = CourseRecord.get_fields()
            record_data = dict()
            for field_name, field_obj in fields.iteritems():

                # Had to rename these fields, since they're named with hive keywords.
                if field_name in ('end_date', 'start_date'):
                    field_value = course_data.get(field_name.replace('_date', ''))
                else:
                    field_value = course_data.get(field_name)

                if field_value is not None:
                    field_value = field_obj.deserialize_from_string(field_value)
                record_data[field_name] = field_value

            record = CourseRecord(**record_data)
            yield record.to_string_tuple()

    def output(self):
        """Expose the data location target as the output."""
        return get_target_from_url(self.output_root)

    def complete(self):
        """
        The task is complete if the output_root/_SUCCESS file is present.
        """
        return get_target_from_url(url_path_join(self.output_root, '_SUCCESS')).exists()

    def run(self):
        """
        Clear out output if data is incomplete, or if overwrite requested.
        """
        if not self.complete():
            self.remove_output_on_overwrite()

        super(CourseListApiDataTask, self).run()


class CourseListTableTask(BareHiveTableTask):
    """Hive table containing the course data, partitioned by formatted datetime."""

    @property
    def partition_by(self):
        return 'dt'

    @property
    def table(self):
        return 'course_list'

    @property
    def columns(self):
        return CourseRecord.get_hive_schema()

    @property
    def output_root(self):
        """Use the table location path for the output root."""
        return self.table_location


class CourseListPartitionTask(CourseListDownstreamMixin, MapReduceJobTaskMixin, HivePartitionTask):
    """A single hive partition of course data."""

    @property
    def hive_table_task(self):
        return CourseListTableTask(
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )

    @property
    def data_task(self):
        return CourseListApiDataTask(
            datetime=self.datetime,
            output_root=self.output_root,
            overwrite=self.overwrite,
            mapreduce_engine=self.mapreduce_engine,
            input_format=self.input_format,
            lib_jar=self.lib_jar,
            n_reduce_tasks=self.n_reduce_tasks,
            remote_log_level=self.remote_log_level,
        )

    @property
    def output_root(self):
        """Expose the partition location path as the output root."""
        return self.partition_location
