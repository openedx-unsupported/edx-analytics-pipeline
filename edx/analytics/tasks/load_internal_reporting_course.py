"""
Loads the internal reporting course table into the warehouse through the pipeline via Hive.

On the roadmap is to write a task that runs validation queries on the aggregated Hive data pre-load.

"""
import json
import datetime
import luigi
import requests
import ciso8601

from edx.analytics.tasks.url import get_target_from_url
from edx.analytics.tasks.url import url_path_join
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.vertica_load import VerticaCopyTask
from edx.analytics.tasks.database_imports import ImportStudentCourseEnrollmentTask, ImportIntoHiveTableTask
from edx.analytics.tasks.util.hive import HiveTableFromQueryTask, WarehouseMixin, HivePartition

# pylint: disable-msg=anomalous-unicode-escape-in-string
# To ensure the API call pulls all courses, set this to be larger than the number of courses
PAGE_SIZE = 10000


class LoadInternalReportingCourseMixin(WarehouseMixin, OverwriteOutputMixin):
    """
    Mixin to handle parameters common to the tasks involved in loading the internal reporting course table,
    including calling the course structure API.
    """
    run_date = luigi.DateParameter(default=datetime.datetime.utcnow().date())
    api_root_url = luigi.Parameter(
        config_path={'section': 'course-structure', 'name': 'api_root_url'}
    )
    api_access_token = luigi.Parameter(
        config_path={'section': 'course-structure', 'name': 'access_token'}, significant=False
    )


class PullCourseStructureAPIData(LoadInternalReportingCourseMixin, luigi.Task):
    """Call the course structure API and place the resulting JSON on S3."""

    def run(self):
        self.remove_output_on_overwrite()
        headers = {'authorization': ('Bearer ' + self.api_access_token), 'accept': 'application/json'}
        api_url = url_path_join(self.api_root_url, 'api', 'course_structure', 'v0', 'courses',
                                '?page_size={size}'.format(size=PAGE_SIZE))
        response = requests.get(url=api_url, headers=headers)
        if response.status_code != requests.codes.ok:  # pylint: disable=no-member
            msg = "Encountered status {} on request to API for {}".format(response.status_code, self.run_date)
            raise Exception(msg)
        with self.output().open('w') as output_file:
            output_file.write(response.content)

    def output(self):
        """Output is in the form {warehouse_path}/courses_raw/dt={CCYY-MM-DD}/course_structure.json"""
        date_string = self.run_date.strftime('dt=%Y-%m-%d')  # pylint: disable=no-member
        url_with_filename = url_path_join(self.warehouse_path, 'courses_raw', date_string,
                                          "course_structure.json")
        return get_target_from_url(url_with_filename)


class ProcessCourseStructureAPIData(LoadInternalReportingCourseMixin, luigi.Task):
    """Process the information from the course structure API and write it to a tsv."""

    def requires(self):
        kwargs = {
            'run_date': self.run_date,
            'warehouse_path': self.warehouse_path,
            'api_root_url': self.api_root_url,
            'api_access_token': self.api_access_token
        }
        return PullCourseStructureAPIData(**kwargs)

    def run(self):
        self.remove_output_on_overwrite()
        with self.input().open('r') as input_file:
            course_structure = json.load(input_file)
            with self.output().open('w') as output_file:
                courses_list = course_structure.get('results')
                if not courses_list:  # If there are no courses, or 'results' is not a key in the json, output nothing.
                    return
                for course in courses_list:
                    # To maintain robustness, ignore any non-dictionary data that finds its way into the API response.
                    try:
                        start_string = course.get('start')
                        end_string = course.get('end')
                        if start_string is None:
                            cleaned_start_string = '\N'
                        else:
                            cleaned_start_string = ciso8601.parse_datetime(start_string)
                        if end_string is None:
                            cleaned_end_string = '\N'
                        else:
                            cleaned_end_string = ciso8601.parse_datetime(end_string)
                        line = [
                            course.get('id', '\N'),
                            course.get('org', '\N'),
                            course.get('course', '\N'),
                            course.get('run', '\N'),
                            coerce_timestamp_for_hive(cleaned_start_string),
                            coerce_timestamp_for_hive(cleaned_end_string),
                            course.get('name', '\N')
                        ]
                        output_file.write('\t'.join([v.encode('utf-8') for v in line]))
                        output_file.write('\n')
                    except AttributeError:  # If the course is not a dictionary, move on to the next one.
                        continue

    def output(self):
        """
        Output is set up so that it can be read as a Hive table with partitions.

        The form is {warehouse_path}/course_structure/dt={CCYY-mm-dd}/courses.tsv.
        """
        date_string = self.run_date.strftime('%Y-%m-%d')  # pylint: disable=no-member
        partition_path_spec = HivePartition('dt', date_string).path_spec
        url_with_filename = url_path_join(self.warehouse_path, "course_structure",
                                          partition_path_spec, "courses.tsv")
        return get_target_from_url(url_with_filename)


class LoadCourseStructureAPIDataIntoHive(LoadInternalReportingCourseMixin, ImportIntoHiveTableTask):
    """Load the processed course structure API data into Hive."""

    def requires(self):
        kwargs = {
            'run_date': self.run_date,
            'warehouse_path': self.warehouse_path,
            'api_root_url': self.api_root_url,
            'api_access_token': self.api_access_token,
            'overwrite': self.overwrite
        }
        return ProcessCourseStructureAPIData(**kwargs)

    @property
    def table_name(self):
        return 'course_structure'

    @property
    def table_format(self):
        return "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'"

    @property
    def table_location(self):
        return url_path_join(self.warehouse_path, self.table_name)

    @property
    def partition_date(self):
        return self.run_date.isoformat()  # pylint: disable=no-member

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
            ('course_org_id', 'STRING'),
            ('course_number', 'STRING'),
            ('course_run', 'STRING'),
            ('course_start', 'STRING'),
            ('course_end', 'STRING'),
            ('course_name', 'STRING')
        ]


class GetCoursesFromStudentCourseEnrollmentTask(LoadInternalReportingCourseMixin, HiveTableFromQueryTask):
    """
    Finds all courses from the LMS enrollments table to make sure we haven't left out any courses with enrollments
    which, for whatever reason, can't be found in the course structure API.
    """

    n_reduce_tasks = luigi.Parameter()

    def requires(self):
        return ImportStudentCourseEnrollmentTask(import_date=self.run_date, destination=self.warehouse_path)

    @property
    def table(self):
        return 'courses_with_enrollments'

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.run_date.isoformat())  # pylint: disable=no-member

    @property
    def insert_query(self):
        return """
            SELECT DISTINCT course_id
            FROM student_courseenrollment SORT BY course_id ASC
            """


class AggregateInternalReportingCourseTableHive(LoadInternalReportingCourseMixin, HiveTableFromQueryTask):
    """Aggregate the internal reporting course table in Hive."""
    n_reduce_tasks = luigi.Parameter()

    def requires(self):
        """
        This task reads from student_coursenrollment as well as course structure API results, so require that they be
        loaded into Hive (via MySQL loads into Hive or via the pipeline as needed).
        """
        return [GetCoursesFromStudentCourseEnrollmentTask(n_reduce_tasks=self.n_reduce_tasks,
                                                          warehouse_path=self.warehouse_path,
                                                          run_date=self.run_date,
                                                          overwrite=self.overwrite),
                LoadCourseStructureAPIDataIntoHive(run_date=self.run_date,
                                                   warehouse_path=self.warehouse_path,
                                                   api_root_url=self.api_root_url,
                                                   api_access_token=self.api_access_token,
                                                   overwrite=self.overwrite)]

    @property
    def table(self):
        return 'internal_reporting_course'

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
            ('course_org_id', 'STRING'),
            ('course_number', 'STRING'),
            ('course_run', 'STRING'),
            ('course_start', 'STRING'),
            ('course_end', 'STRING'),
            ('course_name', 'STRING'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.run_date.isoformat())  # pylint: disable=no-member

    @property
    def insert_query(self):
        return """
            SELECT
              sce.course_id
            , cs.course_org_id
            , cs.course_number
            , cs.course_run
            , cs.course_start
            , cs.course_end
            , cs.course_name
            FROM courses_with_enrollments sce
            LEFT OUTER JOIN course_structure cs ON sce.course_id = cs.course_id
            SORT BY sce.course_id ASC
            """


class LoadInternalReportingCourseToWarehouse(LoadInternalReportingCourseMixin, VerticaCopyTask):
    """
    Loads the course table from Hive into the Vertica data warehouse.
    """
    n_reduce_tasks = luigi.Parameter()

    @property
    def partition(self):
        """The table is partitioned by date."""
        return HivePartition('dt', self.run_date.isoformat())  # pylint: disable=no-member

    @property
    def insert_source_task(self):
        return (
            # Get the location of the Hive table, so it can be opened and read.
            AggregateInternalReportingCourseTableHive(
                warehouse_path=self.warehouse_path,
                n_reduce_tasks=self.n_reduce_tasks,
                run_date=self.run_date,
                api_root_url=self.api_root_url,
                api_access_token=self.api_access_token,
                overwrite=self.overwrite
            )
        )

    @property
    def table(self):
        return 'd_course'

    @property
    def default_columns(self):
        """List of tuples defining name and definition of automatically-filled columns."""
        return None

    @property
    def auto_primary_key(self):
        """No automatic primary key is needed; course id is a unique identifier."""
        return None

    @property
    def columns(self):
        return [
            ('course_id', 'VARCHAR(256) PRIMARY KEY'),
            ('course_org_id', 'VARCHAR(45)'),
            ('course_number', 'VARCHAR(45)'),
            ('course_run', 'VARCHAR(100)'),
            ('course_start', 'TIMESTAMP'),
            ('course_end', 'TIMESTAMP'),
            ('course_name', 'VARCHAR(200)')
        ]


def coerce_timestamp_for_hive(timestamp):
    """
    Helper method to coerce python None values to the Hive null value, '\N' and datetime.datetime objects to
    iso-formatted strings
    """
    if timestamp is None:
        return '\N'
    elif isinstance(timestamp, datetime.datetime):
        return timestamp.isoformat()
    else:
        return timestamp
