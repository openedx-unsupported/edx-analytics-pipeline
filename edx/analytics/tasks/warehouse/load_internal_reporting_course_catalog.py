"""
Loads the internal reporting course catalog table into the warehouse through the pipeline.
"""

import json
import datetime
import logging

import luigi

from edx.analytics.tasks.url import get_target_from_url
from edx.analytics.tasks.url import url_path_join
from edx.analytics.tasks.util.edx_api_client import EdxApiClient
from edx.analytics.tasks.util.hive import BareHiveTableTask, HivePartitionTask, WarehouseMixin
from edx.analytics.tasks.util.record import Record, StringField, FloatField, DateTimeField, IntegerField
from edx.analytics.tasks.util.opaque_key_util import get_org_id_for_course
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.vertica_load import VerticaCopyTask, VerticaCopyTaskMixin


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
    partner_short_codes = luigi.Parameter(
        default_from_config={'section': 'course-catalog-api', 'name': 'partner_short_codes'},
        is_list=True,
        default=None,
        description="A list of partner short codes that we should fetch data for."
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


class PullCourseCatalogAPIData(LoadInternalReportingCourseCatalogMixin, luigi.Task):
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
                if not self.api_root_url:
                    raise luigi.parameter.MissingParameterException("Missing api_root_url.")
                url = url_path_join(self.api_root_url, 'course_runs') + '/'
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
                self.hive_partition_path('course_catalog_raw', partition_value=self.date), 'course_catalog.json'
            )
        )


class ProgramCourseRecord(Record):
    """Represents a course run within a program."""
    program_id = StringField(nullable=False, length=36)
    program_type = StringField(nullable=False, length=32)
    program_title = StringField(nullable=True, length=255, normalize_whitespace=True)
    catalog_course = StringField(nullable=False, length=255)
    catalog_course_title = StringField(nullable=True, length=255, normalize_whitespace=True)
    course_id = StringField(nullable=False, length=255)
    org_id = StringField(nullable=False, length=255)
    partner_short_code = StringField(nullable=True, length=8)


class BaseCourseMetadataTask(LoadInternalReportingCourseCatalogMixin, luigi.Task):
    """Process the information from the API and write it to a tsv."""

    table_name = 'override_me'

    def requires(self):
        return PullCourseCatalogAPIData(
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


class ProgramCourseDataTask(BaseCourseMetadataTask):
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
                partner_short_code=course_run.get('partner_short_code')
            )
            output_file.write(record.to_separated_values(sep=u'\t'))
            output_file.write('\n')


class LoadInternalReportingProgramCourseToWarehouse(LoadInternalReportingCourseCatalogMixin, VerticaCopyTask):
    """Load the d_program_course table in the data warehouse."""

    @property
    def insert_source_task(self):
        return ProgramCoursePartitionTask(
            date=self.date,
            warehouse_path=self.warehouse_path,
            api_root_url=self.api_root_url,
            api_page_size=self.api_page_size,
            overwrite=self.overwrite,
        ).data_task

    @property
    def table(self):
        return 'd_program_course'

    @property
    def columns(self):
        return ProgramCourseRecord.get_sql_schema()


class CourseSeatRecord(Record):
    """Represents a course seat within course run."""
    course_id = StringField(nullable=False, length=255)
    course_seat_type = StringField(nullable=False, length=255)
    course_seat_price = FloatField(nullable=False)
    course_seat_currency = StringField(nullable=False, length=255)
    course_seat_upgrade_deadline = DateTimeField(nullable=True)
    course_seat_credit_provider = StringField(nullable=True, length=255)
    course_seat_credit_hours = IntegerField(nullable=True)


class CourseSeatTask(BaseCourseMetadataTask):
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


class LoadInternalReportingCourseSeatToWarehouse(LoadInternalReportingCourseCatalogMixin, VerticaCopyTask):
    """Loads the course seat tsv into the Vertica data warehouse."""

    @property
    def insert_source_task(self):
        return CourseSeatTask(
            date=self.date,
            warehouse_path=self.warehouse_path,
            api_root_url=self.api_root_url,
            api_page_size=self.api_page_size,
            overwrite=self.overwrite,
        )

    @property
    def table(self):
        return 'd_course_seat'

    @property
    def columns(self):
        return CourseSeatRecord.get_sql_schema()


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
    org_id = StringField(nullable=False, length=255)
    partner_short_code = StringField(nullable=True, length=8)
    marketing_url = StringField(nullable=True, length=1024)
    min_effort = IntegerField(nullable=True)
    max_effort = IntegerField(nullable=True)


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


class CourseDataTask(BaseCourseMetadataTask):
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
        )
        output_file.write(record.to_separated_values(sep=u'\t'))
        output_file.write('\n')


class LoadInternalReportingCourseToWarehouse(LoadInternalReportingCourseCatalogMixin, VerticaCopyTask):
    """Loads the course tsv into the Vertica data warehouse."""

    @property
    def insert_source_task(self):
        return CoursePartitionTask(
            date=self.date,
            warehouse_path=self.warehouse_path,
            api_root_url=self.api_root_url,
            api_page_size=self.api_page_size,
            overwrite=self.overwrite,
        ).data_task

    @property
    def default_columns(self):
        return []

    @property
    def table(self):
        return 'd_course'

    @property
    def columns(self):
        return CourseRecord.get_sql_schema()


class LoadInternalReportingCourseCatalogToWarehouse(
        LoadInternalReportingCourseCatalogMixin,
        VerticaCopyTaskMixin,
        luigi.WrapperTask):
    """Workflow for loading course catalog into the data warehouse."""

    def requires(self):
        kwargs = {
            'date': self.date,
            'warehouse_path': self.warehouse_path,
            'api_root_url': self.api_root_url,
            'api_page_size': self.api_page_size,
            'overwrite': self.overwrite,
            'schema': self.schema,
            'credentials': self.credentials,
            'read_timeout': self.read_timeout,
            'marker_schema': self.marker_schema,
        }
        yield LoadInternalReportingCourseToWarehouse(**kwargs)
        yield LoadInternalReportingCourseSeatToWarehouse(**kwargs)
        yield LoadInternalReportingProgramCourseToWarehouse(**kwargs)

    def complete(self):
        # OverwriteOutputMixin changes the complete() method behavior, so we override it.
        return all(r.complete() for r in luigi.task.flatten(self.requires()))
