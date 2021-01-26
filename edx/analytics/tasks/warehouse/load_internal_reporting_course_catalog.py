"""
Loads the internal reporting course catalog table into the warehouse through the pipeline.
"""

import datetime
import json
import logging

import luigi
from luigi.contrib.hive import HiveQueryTask

from edx.analytics.tasks.util.edx_api_client import EdxApiClient
from edx.analytics.tasks.util.hive import BareHiveTableTask, HivePartitionTask, WarehouseMixin, hive_database_name
from edx.analytics.tasks.util.opaque_key_util import get_org_id_for_course
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.record import DateField, DateTimeField, FloatField, IntegerField, Record, StringField
from edx.analytics.tasks.util.url import get_target_from_url, url_path_join

# pylint: disable=anomalous-unicode-escape-in-string

log = logging.getLogger(__name__)


class LoadInternalReportingCourseCatalogMixin(WarehouseMixin, OverwriteOutputMixin):
    """
    Mixin to handle parameters common to the tasks involved in loading the internal reporting course catalog table,
    including calling the course catalog API.
    """
    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        description='Default is today, UTC.',
    )
    partner_short_codes = luigi.ListParameter(
        config_path={'section': 'course-catalog-api', 'name': 'partner_short_codes'},
        default=list(),
        description="A list of partner short codes that we should fetch data for."
    )
    partner_api_urls = luigi.ListParameter(
        config_path={'section': 'course-catalog-api', 'name': 'partner_api_urls'},
        default=list(),
        description="A list of API URLs that are associated with the partner_short_codes.  This list must exactly " +
                    "match the ordering of the partner_short_codes list."
    )
    api_root_url = luigi.Parameter(
        config_path={'section': 'course-catalog-api', 'name': 'api_root_url'},
        default=None,
        description="The base URL for the course catalog API. This URL should look like"
                    "https://catalog-service.example.com/api/v1/"
    )
    api_page_size = luigi.IntParameter(
        config_path={'section': 'course-catalog-api', 'name': 'page_size'},
        significant=False,
        default=100,
        description="The number of records to request from the API in each HTTP request."
    )


class PullDiscoveryCoursesAPIData(LoadInternalReportingCourseCatalogMixin, luigi.Task):
    """Call the course catalog API and place the resulting JSON into the output file."""

    def run(self):
        self.remove_output_on_overwrite()
        client = EdxApiClient()
        with self.output().open('w') as output_file:
            short_codes = self.partner_short_codes if self.partner_short_codes else []
            for partner_short_code in short_codes:
                params = {
                    'limit': self.api_page_size,
                    'partner': partner_short_code,
                    'exclude_utm': 1,
                }

                if self.partner_api_urls:
                    url_index = short_codes.index(partner_short_code)

                    if url_index >= self.partner_api_urls.__len__():
                        raise luigi.parameter.MissingParameterException(
                            "Error!  Index of the partner short code from partner_short_codes exceeds the length of "
                            "partner_api_urls.  These lists are not in sync!!!")
                    api_root_url = self.partner_api_urls[url_index]
                elif self.api_root_url:
                    api_root_url = self.api_root_url
                else:
                    raise luigi.parameter.MissingParameterException("Missing either a partner_api_urls or an " +
                                                                    "api_root_url.")

                url = url_path_join(api_root_url, 'courses') + '/'
                for response in client.paginated_get(url, params=params):
                    parsed_response = response.json()
                    counter = 0
                    for course in parsed_response.get('results', []):
                        course['partner_short_code'] = partner_short_code
                        output_file.write(json.dumps(course))
                        output_file.write('\n')
                        counter += 1

                    if counter > 0:
                        log.info('Wrote %d records to output file', counter)

    def output(self):
        return get_target_from_url(
            url_path_join(
                self.hive_partition_path('discovery_api_raw', partition_value=self.date), 'courses.json'
            )
        )


class PullDiscoveryCourseRunsAPIData(LoadInternalReportingCourseCatalogMixin, luigi.Task):
    """Call the course catalog API and place the resulting JSON into the output file."""

    def run(self):
        self.remove_output_on_overwrite()
        client = EdxApiClient()
        with self.output().open('w') as output_file:
            short_codes = self.partner_short_codes if self.partner_short_codes else []
            for partner_short_code in short_codes:
                params = {
                    'limit': self.api_page_size,
                    'partner': partner_short_code,
                    'exclude_utm': 1,
                }

                if self.partner_api_urls:
                    url_index = short_codes.index(partner_short_code)

                    if url_index >= self.partner_api_urls.__len__():
                        raise luigi.parameter.MissingParameterException(
                            "Error!  Index of the partner short code from partner_short_codes exceeds the length of "
                            "partner_api_urls.  These lists are not in sync!!!")
                    api_root_url = self.partner_api_urls[url_index]
                elif self.api_root_url:
                    api_root_url = self.api_root_url
                else:
                    raise luigi.parameter.MissingParameterException("Missing either a partner_api_urls or an " +
                                                                    "api_root_url.")

                url = url_path_join(api_root_url, 'course_runs') + '/'
                for response in client.paginated_get(url, params=params):
                    parsed_response = response.json()
                    counter = 0
                    for course in parsed_response.get('results', []):
                        course['partner_short_code'] = partner_short_code
                        output_file.write(json.dumps(course))
                        output_file.write('\n')
                        counter += 1

                    if counter > 0:
                        log.info('Wrote %d records to output file', counter)

    def output(self):
        return get_target_from_url(
            url_path_join(
                self.hive_partition_path('discovery_api_raw', partition_value=self.date), 'course_runs.json'
            )
        )


class PullDiscoveryProgramsAPIData(LoadInternalReportingCourseCatalogMixin, luigi.Task):
    """Call the course discovery API and place the resulting JSON into the output file."""

    def run(self):
        self.remove_output_on_overwrite()
        client = EdxApiClient()
        with self.output().open('w') as output_file:
            params = {
                'limit': self.api_page_size,
                'exclude_utm': 1,
            }
            if not self.api_root_url:
                raise luigi.parameter.MissingParameterException("Missing api_root_url.")
            url = url_path_join(self.api_root_url, 'programs') + '/'
            for response in client.paginated_get(url, params=params):
                parsed_response = response.json()
                counter = 0
                for program in parsed_response.get('results', []):
                    output_file.write(json.dumps(program))
                    output_file.write('\n')
                    counter += 1

                if counter > 0:
                    log.info('Wrote %d records to output file', counter)

    def output(self):
        return get_target_from_url(
            url_path_join(
                self.hive_partition_path('discovery_api_raw', partition_value=self.date), 'programs.json'
            )
        )


class BaseCourseMetadataTask(LoadInternalReportingCourseCatalogMixin, luigi.Task):
    """Process the information from the API and write it to a tsv."""

    table_name = 'override_me'

    def requires(self):
        return PullDiscoveryCoursesAPIData(
            date=self.date,
            warehouse_path=self.warehouse_path,
            api_root_url=self.api_root_url,
            api_page_size=self.api_page_size,
            overwrite=self.overwrite,
        )

    def run(self):
        self.remove_output_on_overwrite()
        with self.input().open('r') as input_file:
            with self.output().open('w') as output_file:
                for course_str in input_file:
                    course = json.loads(course_str)
                    self.process_course(course, output_file)

    def process_course(self, course_run, output_file):
        """
        Given a course_run, write output to the output file.

        Arguments:
            course_run (dict): A plain-old-python-object that represents the course run.
            output_file (file-like): A file handle that program mappings can be written to. Must implement write(str).
        """
        raise NotImplementedError

    def output(self):
        return get_target_from_url(
            url_path_join(
                self.hive_partition_path(self.table_name, partition_value=self.date), '{0}.tsv'.format(self.table_name)
            )
        )


class BaseCourseRunMetadataTask(LoadInternalReportingCourseCatalogMixin, luigi.Task):
    """Process the information from the API and write it to a tsv."""

    table_name = 'override_me'

    def requires(self):
        return PullDiscoveryCourseRunsAPIData(
            date=self.date,
            warehouse_path=self.warehouse_path,
            api_root_url=self.api_root_url,
            api_page_size=self.api_page_size,
            overwrite=self.overwrite,
        )

    def run(self):
        self.remove_output_on_overwrite()
        with self.input().open('r') as input_file:
            with self.output().open('w') as output_file:
                for course_run_str in input_file:
                    course_run = json.loads(course_run_str)
                    self.process_course_run(course_run, output_file)

    def process_course_run(self, course_run, output_file):
        """
        Given a course_run, write output to the output file.

        Arguments:
            course_run (dict): A plain-old-python-object that represents the course run.
            output_file (file-like): A file handle that program mappings can be written to. Must implement write(str).
        """
        raise NotImplementedError

    def output(self):
        return get_target_from_url(
            url_path_join(
                self.hive_partition_path(self.table_name, partition_value=self.date), '{0}.tsv'.format(self.table_name)
            )
        )


class ProgramCourseOrderDataTask(LoadInternalReportingCourseCatalogMixin, luigi.Task):
    """Process course discovery programs API response and save results to a tsv."""

    def requires(self):
        return PullDiscoveryProgramsAPIData(
            date=self.date,
            warehouse_path=self.warehouse_path,
            api_root_url=self.api_root_url,
            api_page_size=self.api_page_size,
            overwrite=self.overwrite,
        )

    def run(self):
        self.remove_output_on_overwrite()
        with self.input().open('r') as input_file:
            with self.output().open('w') as output_file:
                for program_str in input_file:
                    program = json.loads(program_str)
                    current_slot_number = 1
                    for course in program.get('courses', []):
                        program_id = program['uuid']
                        catalog_course = course['key']
                        slot_number = current_slot_number

                        line = (program_id, catalog_course, str(slot_number))
                        output_file.write('\t'.join([v.encode('utf-8') for v in line]))
                        output_file.write('\n')
                        current_slot_number += 1

    def output(self):
        return get_target_from_url(
            url_path_join(
                self.hive_partition_path('program_course_order', partition_value=self.date),
                '{0}.tsv'.format('program_course_order')
            )
        )


class ProgramCourseOrderTableTask(BareHiveTableTask):
    """Hive table for program_course_order."""

    @property
    def partition_by(self):
        return 'dt'

    @property
    def table(self):
        return 'program_course_order'

    @property
    def columns(self):
        return [
            ('program_id', 'STRING'),
            ('catalog_course', 'STRING'),
            ('program_slot_number', 'INT'),
        ]


class ProgramCourseOrderPartitionTask(LoadInternalReportingCourseCatalogMixin, HivePartitionTask):
    """The hive table partition for the program course catalog data."""

    @property
    def partition_value(self):
        """Use a dynamic partition value based on the date parameter."""
        return self.date.isoformat()  # pylint: disable=no-member

    @property
    def hive_table_task(self):
        return ProgramCourseOrderTableTask(
            warehouse_path=self.warehouse_path,
        )

    @property
    def data_task(self):
        return ProgramCourseOrderDataTask(
            date=self.date,
            warehouse_path=self.warehouse_path,
            api_root_url=self.api_root_url,
            api_page_size=self.api_page_size,
            overwrite=self.overwrite,
        )


class ProgramCourseRecord(Record):
    """Represents a course run within a program."""
    program_id = StringField(nullable=False, length=36)
    program_type = StringField(nullable=False, length=32)
    program_title = StringField(nullable=True, length=255, normalize_whitespace=True)
    catalog_course = StringField(nullable=False, length=255)
    catalog_course_title = StringField(nullable=True, length=255, normalize_whitespace=True)
    course_id = StringField(nullable=False, length=255)
    org_id = StringField(nullable=True, length=255)
    partner_short_code = StringField(nullable=True, length=8)
    program_slot_number = IntegerField(nullable=True)


class ProgramCourseTableTask(BareHiveTableTask):
    """Hive table for program course."""

    @property
    def partition_by(self):
        return 'dt'

    @property
    def table(self):
        return 'program_course'

    @property
    def columns(self):
        return ProgramCourseRecord.get_hive_schema()


class ProgramCoursePartitionTask(LoadInternalReportingCourseCatalogMixin, HivePartitionTask):
    """The hive table partition for the program course catalog data."""

    @property
    def partition_value(self):
        """Use a dynamic partition value based on the date parameter."""
        return self.date.isoformat()  # pylint: disable=no-member

    @property
    def hive_table_task(self):
        return ProgramCourseTableTask(
            warehouse_path=self.warehouse_path,
        )

    @property
    def data_task(self):
        return ProgramCourseDataTask(
            date=self.date,
            warehouse_path=self.warehouse_path,
            api_root_url=self.api_root_url,
            api_page_size=self.api_page_size,
            overwrite=self.overwrite,
        )


class ProgramCourseDataTask(BaseCourseRunMetadataTask):
    """Process the information from the course structure API and write it to a tsv."""

    table_name = 'program_course'

    def process_course_run(self, course_run, output_file):
        for program in course_run.get('programs', []):
            record = ProgramCourseRecord(
                program_id=program['uuid'],
                program_type=program['type'],
                program_title=program.get('title'),
                catalog_course=course_run['course'],
                catalog_course_title=course_run.get('title'),
                course_id=course_run['key'],
                org_id=get_org_id_for_course(course_run['key']),
                partner_short_code=course_run.get('partner_short_code'),
                program_slot_number=None,
            )
            output_file.write(record.to_separated_values(sep=u'\t'))
            output_file.write('\n')


class JoinProgramCourseWithOrderTask(LoadInternalReportingCourseCatalogMixin, HiveQueryTask):
    """Task to join program_course with program_course_order hive tables."""

    def query(self):
        query = """
        USE {database_name};
        DROP TABLE IF EXISTS {table_name};
        CREATE EXTERNAL TABLE {table_name} (
            program_id STRING,
            program_type STRING,
            program_title STRING,
            catalog_course STRING,
            catalog_course_title STRING,
            course_id STRING,
            org_id STRING,
            partner_short_code STRING,
            program_slot_number INT
        )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        LOCATION '{location}';

        INSERT OVERWRITE TABLE {table_name}
        SELECT
            p.program_id,
            p.program_type,
            p.program_title,
            p.catalog_course,
            p.catalog_course_title,
            p.course_id,
            p.org_id,
            p.partner_short_code,
            o.program_slot_number
        FROM program_course p
        LEFT JOIN program_course_order o ON p.program_id = o.program_id AND p.catalog_course = o.catalog_course;
        """.format(
            database_name=hive_database_name(),
            location=self.table_location,
            table_name=self.table,
        )
        log.debug('Executing hive query: %s', query)
        return query

    def run(self):
        self.remove_output_on_overwrite()
        super(JoinProgramCourseWithOrderTask, self).run()

    @property
    def table(self):
        """Provides name of Hive database table."""
        return 'program_course_with_order'

    @property
    def table_location(self):
        """Provides location of Hive database table's data."""
        return self.hive_partition_path(self.table, self.date)

    def output(self):
        return get_target_from_url(self.table_location)

    def requires(self):
        kwargs = {
            'date': self.date,
            'warehouse_path': self.warehouse_path,
            'api_root_url': self.api_root_url,
            'api_page_size': self.api_page_size,
            'overwrite': self.overwrite,
        }
        yield ProgramCoursePartitionTask(**kwargs)
        yield ProgramCourseOrderPartitionTask(**kwargs)


class CourseSeatRecord(Record):
    """Represents a course seat within course run."""
    course_id = StringField(nullable=False, length=255)
    course_seat_type = StringField(nullable=False, length=255)
    course_seat_price = FloatField(nullable=False)
    course_seat_currency = StringField(nullable=False, length=255)
    course_seat_upgrade_deadline = DateTimeField(nullable=True)
    course_seat_credit_provider = StringField(nullable=True, length=255)
    course_seat_credit_hours = IntegerField(nullable=True)


class CourseSeatTask(BaseCourseRunMetadataTask):
    """Process course seat information from the course structure API and write it to a tsv."""

    table_name = 'course_seat'

    def process_course_run(self, course_run, output_file):
        for seat in course_run.get('seats', []):
            record = CourseSeatRecord(
                course_id=course_run['key'],
                course_seat_type=seat['type'],
                course_seat_price=float(seat.get('price', 0)),
                course_seat_currency=seat.get('currency'),
                course_seat_upgrade_deadline=DateTimeField().deserialize_from_string(seat.get('upgrade_deadline')),
                course_seat_credit_provider=seat.get('credit_provider'),
                course_seat_credit_hours=seat.get('credit_hours')
            )
            output_file.write(record.to_separated_values(sep=u'\t'))
            output_file.write('\n')


class CourseRecord(Record):
    """Represents a course."""
    course_id = StringField(nullable=False, length=255)
    catalog_course = StringField(nullable=False, length=255)
    catalog_course_title = StringField(nullable=True, length=255, normalize_whitespace=True)
    start_time = DateTimeField(nullable=True)
    end_time = DateTimeField(nullable=True)
    enrollment_start_time = DateTimeField(nullable=True)
    enrollment_end_time = DateTimeField(nullable=True)
    content_language = StringField(nullable=True, length=50)
    pacing_type = StringField(nullable=True, length=255)
    level_type = StringField(nullable=True, length=255)
    availability = StringField(nullable=True, length=255)
    org_id = StringField(nullable=True, length=255)
    partner_short_code = StringField(nullable=True, length=8)
    marketing_url = StringField(nullable=True, length=1024)
    min_effort = IntegerField(nullable=True)
    max_effort = IntegerField(nullable=True)
    weeks_to_complete = IntegerField(nullable=True)
    announcement_time = DateTimeField(nullable=True)
    reporting_type = StringField(nullable=True, length=20)


class CourseTableTask(BareHiveTableTask):
    """Hive table for course catalog."""

    @property
    def partition_by(self):
        return 'dt'

    @property
    def table(self):
        return 'course_catalog'

    @property
    def columns(self):
        return CourseRecord.get_hive_schema()


class CoursePartitionTask(LoadInternalReportingCourseCatalogMixin, HivePartitionTask):
    """The hive table partition for this course catalog data."""

    @property
    def partition_value(self):
        """Use a dynamic partition value based on the date parameter."""
        return self.date.isoformat()  # pylint: disable=no-member

    @property
    def hive_table_task(self):
        return CourseTableTask(
            warehouse_path=self.warehouse_path,
        )

    @property
    def data_task(self):
        return CourseDataTask(
            date=self.date,
            warehouse_path=self.warehouse_path,
            api_root_url=self.api_root_url,
            api_page_size=self.api_page_size,
            overwrite=self.overwrite,
        )


class CourseDataTask(BaseCourseRunMetadataTask):
    """Process course information from the course structure API and write it to a tsv."""

    table_name = 'course_catalog'

    def process_course_run(self, course_run, output_file):
        record = CourseRecord(
            course_id=course_run['key'],
            catalog_course=course_run['course'],
            catalog_course_title=course_run.get('title'),
            start_time=DateTimeField().deserialize_from_string(course_run.get('start')),
            end_time=DateTimeField().deserialize_from_string(course_run.get('end')),
            enrollment_start_time=DateTimeField().deserialize_from_string(course_run.get('enrollment_start')),
            enrollment_end_time=DateTimeField().deserialize_from_string(course_run.get('enrollment_end')),
            content_language=course_run.get('content_language'),
            pacing_type=course_run.get('pacing_type'),
            level_type=course_run.get('level_type'),
            availability=course_run.get('availability'),
            org_id=get_org_id_for_course(course_run['key']),
            partner_short_code=course_run.get('partner_short_code'),
            marketing_url=course_run.get('marketing_url'),
            min_effort=course_run.get('min_effort'),
            max_effort=course_run.get('max_effort'),
            weeks_to_complete=course_run.get('weeks_to_complete'),
            announcement_time=DateTimeField().deserialize_from_string(course_run.get('announcement')),
            reporting_type=course_run.get('reporting_type'),
        )
        output_file.write(record.to_separated_values(sep=u'\t'))
        output_file.write('\n')


class CourseSubjectRecord(Record):
    """Represents course subject categorizations for a course."""
    course_id = StringField(nullable=False, length=200)
    date = DateField(nullable=True)
    subject_uri = StringField(nullable=True, length=200)
    subject_title = StringField(nullable=True, length=200)
    subject_language = StringField(nullable=True, length=200)


class CourseSubjectTask(BaseCourseMetadataTask):
    """Process course seat information from the course structure API and write it to a tsv."""

    table_name = 'course_subjects'

    def process_course(self, course, output_file):
        # Subject information is stored per-course, but needs to be output per-course-run.
        for course_run in course.get('course_runs', []):
            course_id = course_run['key']
            for subject in course.get('subjects', []):
                uri = None if 'slug' not in subject else '/course/subject/{}'.format(subject.get('slug'))
                record = CourseSubjectRecord(
                    course_id=course_id,
                    date=self.date,
                    subject_uri=uri,
                    subject_title=subject.get('name'),
                    subject_language='en',
                )
                output_file.write(record.to_separated_values(sep=u'\t'))
                output_file.write('\n')
