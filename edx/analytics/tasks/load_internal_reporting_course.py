"""
Loads the internal reporting course table into the warehouse through the pipeline via Hive.

On the roadmap is to write a task that runs validation queries on the aggregated Hive data pre-load.

"""
import json
import re
import luigi
import requests

from edx.analytics.tasks.url import get_target_from_url
from edx.analytics.tasks.url import url_path_join
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.vertica_load import VerticaCopyTask
from edx.analytics.tasks.database_imports import ImportStudentCourseEnrollmentTask, ImportIntoHiveTableTask
from edx.analytics.tasks.util.hive import HiveTableFromQueryTask, WarehouseMixin, HivePartition

# pylint: disable-msg=anomalous-unicode-escape-in-string


class LoadDCourseMixin(WarehouseMixin, OverwriteOutputMixin):
    """
    Mixin to handle parameters common to the tasks involved in loading the internal reporting course table,
    including calling the course structure API.
    """
    run_date = luigi.DateParameter()
    api_root = luigi.Parameter(
        config_path={'section': 'course-structure', 'name': 'api_root'}
    )
    api_access_token = luigi.Parameter(
        config_path={'section': 'course-structure', 'name': 'access_token'}, significant=False
    )


class PullCourseStructureAPIData(LoadDCourseMixin, luigi.Task):
    """Call the course structure API and place the resulting JSON on S3."""

    def run(self):
        headers = {'authorization': ('Bearer ' + self.api_access_token), 'accept': 'application/json'}
        api_url = url_path_join(self.api_root, 'api', 'course_structure', 'v0', 'courses', '?page_size=10000')
        response = requests.get(url=api_url, headers=headers)
        if response.status_code != requests.codes.ok:  # pylint: disable=no-member
            msg = "Encountered status {} on request to API for {}".format(response.status_code, self.run_date)
            raise Exception(msg)
        with self.output().open('w') as output_file:
            output_file.write(response.content)

    def output(self):
        """Output is in the form {warehouse_path}/courses_raw/dt={CCYY-MM-DD}/course_structure.json"""
        date_string = "dt=" + self.run_date.strftime('%Y-%m-%d')  # pylint: disable=no-member
        url_with_filename = url_path_join(self.warehouse_path, 'courses_raw', date_string,
                                          "course_structure.json")
        return get_target_from_url(url_with_filename)


class ProcessCourseStructureAPIData(LoadDCourseMixin, luigi.Task):
    """Process the information from the course structure API and write it to a tsv."""
    run_date = luigi.DateParameter()

    def requires(self):
        kwargs = {
            'run_date': self.run_date,
            'warehouse_path': self.warehouse_path,
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
                    if not type(course) == dict:
                        continue
                    timestamp_regex = r'^([^T]*)T([^Z]*)Z'
                    # Coerce the values into strings because sometimes we get a value of None, as in
                    # the end date of self-paced courses.
                    start_string = str(course.get('start', '\N'))
                    end_string = str(course.get('end', '\N'))
                    start_string_search = re.search(timestamp_regex, start_string)
                    end_string_search = re.search(timestamp_regex, end_string)
                    # If we can't make the required matches in the date string, we put in null values.
                    # IndexError and AttributeError each indicate that we do not have a match for a timestamp.
                    try:
                        cleaned_start_string = start_string_search.group(1) + ' ' + start_string_search.group(2)
                    except (IndexError, AttributeError):
                        cleaned_start_string = '\N'
                    try:
                        cleaned_end_string = end_string_search.group(1) + ' ' + end_string_search.group(2)
                    except (IndexError, AttributeError):
                        cleaned_end_string = '\N'
                    line = [
                        course.get('id', '\N'),
                        course.get('org', '\N'),
                        course.get('course', '\N'),
                        course.get('run', '\N'),
                        cleaned_start_string,
                        cleaned_end_string,
                        course.get('name', '\N')
                    ]
                    output_file.write('\t'.join([v.encode('utf-8') for v in line]))
                    output_file.write('\n')

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


class LoadCourseStructureAPIDataIntoHive(LoadDCourseMixin, ImportIntoHiveTableTask):
    """Load the processed course structure API data into Hive."""
    run_date = luigi.Parameter()

    def requires(self):
        kwargs = {
            'run_date': self.run_date,
            'warehouse_path': self.warehouse_path,
            'overwrite': self.overwrite
        }
        return ProcessCourseStructureAPIData(**kwargs)

    @property
    def table_name(self):
        """Provides name of Hive database table."""
        return 'course_structure'

    @property
    def table_format(self):
        """Provides name of Hive database table."""
        return "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"

    @property
    def table_location(self):
        """Provides root location of Hive database table's data."""
        return url_path_join(self.warehouse_path, self.table_name)

    @property
    def partition_date(self):
        """Provides value to use in constructing the partition name of Hive database table."""
        return self.run_date.strftime('%Y-%m-%d')

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
            ('course_org_id', 'STRING'),
            ('course_number', 'STRING'),
            ('course_run', 'STRING'),
            ('course_start_string', 'STRING'),
            ('course_end_string', 'STRING'),
            ('course_name', 'STRING')
        ]


class GetCoursesFromStudentCourseEnrollmentTask(LoadDCourseMixin, HiveTableFromQueryTask):
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


class AggregateInternalReportingCourseTableHive(LoadDCourseMixin, HiveTableFromQueryTask):
    """Aggregate the internal reporting course table in Hive."""
    n_reduce_tasks = luigi.Parameter()

    def requires(self):
        """
        This task reads from student_coursenrollment as well as course structure API results, so require that they be
        loaded into Hive (via MySQL loads into Hive or via the pipeline as needed).
        """
        print str(LoadCourseStructureAPIDataIntoHive(run_date=self.run_date, warehouse_path=self.warehouse_path,
                                                     overwrite=True).output())
        return [GetCoursesFromStudentCourseEnrollmentTask(n_reduce_tasks=self.n_reduce_tasks,
                                                          warehouse_path=self.warehouse_path, run_date=self.run_date),
                LoadCourseStructureAPIDataIntoHive(run_date=self.run_date, warehouse_path=self.warehouse_path,
                                                   overwrite=True)]

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
            ('course_start_string', 'STRING'),
            ('course_end_string', 'STRING'),
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
            , cs.course_start_string
            , cs.course_end_string
            , cs.course_name
            FROM course_structure cs
            RIGHT OUTER JOIN courses_with_enrollments sce ON cs.course_id = sce.course_id
            SORT BY sce.course_id ASC
            """


class LoadInternalReportingCourseToWarehouse(LoadDCourseMixin, VerticaCopyTask):
    """
    Loads the course table from Hive into the Vertica data warehouse.
    """
    n_reduce_tasks = luigi.Parameter()
    overwrite = luigi.BooleanParameter(default=False)

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
                overwrite=self.overwrite,
                n_reduce_tasks=self.n_reduce_tasks,
                run_date=self.run_date
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
            ('course_id', 'VARCHAR(200) PRIMARY KEY'),
            ('course_org_id', 'VARCHAR(45)'),
            ('course_number', 'VARCHAR(45)'),
            ('course_run', 'VARCHAR(100)'),
            ('course_start', 'TIMESTAMP'),
            ('course_end', 'TIMESTAMP'),
            ('course_name', 'VARCHAR(200)')
        ]
