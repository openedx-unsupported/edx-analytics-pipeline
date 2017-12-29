"""
Tasks to split database exports in different groups, per class,
per organization, etc.
"""

import csv
import logging
from collections import namedtuple

import luigi

import edx.analytics.tasks.util.opaque_key_util as opaque_key_util
from edx.analytics.tasks.common.mapreduce import MultiOutputMapReduceJobTask
from edx.analytics.tasks.common.pathutil import PathSetTask
from edx.analytics.tasks.common.sqoop import SqoopImportFromMysql
from edx.analytics.tasks.util import csv_util
from edx.analytics.tasks.util.url import get_target_from_url, url_path_join

log = logging.getLogger(__name__)


# Increase maximum number of characters per field since we have
# entries that easily exceed the default value of 124 KB.

FIELD_SIZE_LIMIT = 4 * 1024 * 1024  # 4 MB
csv.field_size_limit(FIELD_SIZE_LIMIT)

# Helpers for the courseware student module table.

STUDENT_MODULE_FIELDS = [
    'id',
    'module_type',
    'module_id',
    'student_id',
    'state',
    'grade',
    'created',
    'modified',
    'max_grade',
    'done',
    'course_id'
]

StudentModuleRecord = namedtuple('StudentModuleRecord', STUDENT_MODULE_FIELDS)


# Name of marker file to appear in output directory of MultiOutputMapReduceJobTask to indicate success.
MARKER_FILENAME = 'job_success'


class StudentModulePerCourseTask(MultiOutputMapReduceJobTask):
    """
    Separates a raw SQL dump of a courseware_studentmodule table into
    a different tsv file for each course.

    """
    dump_root = luigi.Parameter(
        description='A URL input path pointing to the raw output from the sqoop job.',
    )
    output_suffix = luigi.Parameter(
        default=None,
        description='Added to the filenames for identification.',
    )

    def requires(self):
        return PathSetTask(self.dump_root)

    def output(self):
        return get_target_from_url(url_path_join(self.output_root, MARKER_FILENAME))

    def mapper(self, line):
        """
        Extract course and reformat each line.

        Returns:
            key: course_id
            value: tab separated row data
        """
        values = csv_util.parse_line(line, dialect='mysqldump')
        record = StudentModuleRecord(*values)

        course_id = record.course_id

        # Convert to a tab separated row
        tab_separated_row = csv_util.to_csv_line(record, dialect='mysqlpipe')

        yield course_id, tab_separated_row

    def multi_output_reducer(self, _key, rows, output_file):
        """
        Save one file per course_id.
        """

        header = '\t'.join(STUDENT_MODULE_FIELDS)
        output_file.write(header)
        output_file.write('\n')

        for row in rows:
            output_file.write(row)
            output_file.write('\n')

    def output_path_for_key(self, course_id):
        template = "{course_id}-courseware_studentmodule-{suffix}analytics.sql"

        filename = template.format(
            course_id=opaque_key_util.get_filename_safe_course_id(course_id, '-'),
            suffix=(self.output_suffix + '-') if self.output_suffix else ''
        )

        return url_path_join(self.output_root, filename)


class StudentModulePerCourseAfterImportWorkflow(StudentModulePerCourseTask):
    """
    Generates a raw SQL dump of a courseware_studentmodule table
    and separates it into a different tsv file for each course.

    """
    credentials = luigi.Parameter(
        config_path={'section': 'database-import', 'name': 'credentials'},
        description='Path to the external access credentials file. '
        'The database will be read, not written to.',
    )
    num_mappers = luigi.Parameter(  # TODO: move to config
        default=None,
        significant=False,
        description='The number of map tasks to ask Sqoop to use.',
    )
    where = luigi.Parameter(
        default=None,
        description='A "where" clause to be passed to Sqoop.',
    )
    verbose = luigi.BoolParameter(
        default=False,
        significant=False,
        description='Sqoop prints more information while working.',
    )

    def requires(self):
        table_name = 'courseware_studentmodule'
        return SqoopImportFromMysql(
            credentials=self.credentials,
            destination=url_path_join(self.dump_root, table_name),
            table_name=table_name,
            num_mappers=self.num_mappers,
            where=self.where,
            verbose=self.verbose
        )
