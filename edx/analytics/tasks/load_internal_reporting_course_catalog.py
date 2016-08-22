"""
Loads the internal reporting course catalog table into the warehouse through the pipeline.
"""

import json
import datetime
import logging

import luigi

from edx.analytics.tasks.util.edx_api_client import EdxApiClient
from edx.analytics.tasks.util.opaque_key_util import get_org_id_for_course
from edx.analytics.tasks.url import get_target_from_url
from edx.analytics.tasks.url import url_path_join
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.vertica_load import VerticaCopyTask
from edx.analytics.tasks.util.hive import WarehouseMixin
from edx.analytics.tasks.util.record import Record, StringField

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
        description="A list of partner short codes that we should fetch data for."
    )
    api_root_url = luigi.Parameter(
        config_path={'section': 'course-catalog-api', 'name': 'api_root_url'},
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
            for partner_short_code in self.partner_short_codes:  # pylint: disable=not-an-iterable
                params = {
                    'limit': self.api_page_size,
                    'partner': partner_short_code
                }
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
    program_title = StringField(nullable=True, length=255)
    catalog_course = StringField(nullable=False, length=255)
    catalog_course_title = StringField(nullable=True, length=255)
    course_id = StringField(nullable=False, length=255)
    org_id = StringField(nullable=False, length=255)
    partner_short_code = StringField(nullable=True, length=8)


class ExtractProgramCourseTask(LoadInternalReportingCourseCatalogMixin, luigi.Task):
    """Process the information from the course structure API and write it to a tsv."""

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
                    self.extract_program_mapping(course_run, output_file)

    @staticmethod
    def extract_program_mapping(course_run, output_file):
        """
        Given a course_run, write program mappings to the output file.

        Arguments:
            course_run (dict): A plain-old-python-object that represents the course run.
            output_file (file-like): A file handle that program mappings can be written to. Must implement write(str).
        """
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

    def output(self):
        return get_target_from_url(
            url_path_join(
                self.hive_partition_path('program_course', partition_value=self.date), 'program_course.tsv'
            )
        )


class LoadInternalReportingProgramCourseToWarehouse(LoadInternalReportingCourseCatalogMixin, VerticaCopyTask):
    """Load the d_program_course table in the data warehouse."""

    @property
    def insert_source_task(self):
        return ExtractProgramCourseTask(
            date=self.date,
            warehouse_path=self.warehouse_path,
            api_root_url=self.api_root_url,
            api_page_size=self.api_page_size,
            overwrite=self.overwrite,
        )

    @property
    def table(self):
        return 'd_program_course'

    @property
    def columns(self):
        return ProgramCourseRecord.get_sql_schema()
