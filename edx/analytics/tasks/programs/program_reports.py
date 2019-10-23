import csv
import datetime
import logging

import luigi

from edx.analytics.tasks.common.mapreduce import MultiOutputMapReduceJobTask
from edx.analytics.tasks.common.vertica_export import ExportVerticaTableToS3Task, get_vertica_table_schema
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join

log = logging.getLogger(__name__)


class BaseProgramReportsTask(OverwriteOutputMixin, MultiOutputMapReduceJobTask):
    """ Generates CSV reports on program enrollment """

    credentials = luigi.Parameter(
        config_path={'section': 'vertica-export', 'name': 'credentials'},
        description='Path to the external access credentials file.',
    )
    output_root = luigi.Parameter(
        description='URL pointing to the location reports should be stored',
    )
    report_name = luigi.Parameter(
        description='Name of report file(s) to output'
    )
    date = luigi.Parameter(
        default=datetime.datetime.utcnow().date(),
        description='Current run date. Used to tag report date'
    )
    schema_name = luigi.Parameter(
        default='programs_reporting',
        description='Vertica schema containing reporting table',
    )
    table_name = luigi.Parameter(
        default='learner_enrollments',
        description='Table containing enrollment rows to report on',
    )
    warehouse_name = luigi.Parameter(
        default='docker',
        description='The Vertica warehouse that houses the report schema.',
    )
    sqoop_null_string = luigi.Parameter(
        default='null',
        description='A string replacement value for any (null) values encountered by Sqoop when exporting from Vertica.',
    )
    sqoop_fields_terminated_by = luigi.Parameter(
        default='\t',
        description='The field delimiter used by Sqoop.',
    )
    sqoop_delimiter_replacement = luigi.Parameter(
        default=' ',
        description='The string replacement value for special characters encountered by Sqoop when exporting from '
                    'Vertica.',
    )
    overwrite = luigi.BoolParameter(
        default=True,
        description='Whether or not to overwrite existing outputs',
    )
    overwrite_export = luigi.BoolParameter(
        default=False,
        description='Whether or not to overwrite existing database export'
    )

    def __init__(self, *args, **kwargs):
        super(BaseProgramReportsTask, self).__init__(*args, **kwargs)
        self.columns = self.get_column_names()

    def requires(self):
        return ExportVerticaTableToS3Task(
            vertica_schema_name=self.schema_name,
            table_name=self.table_name,
            vertica_credentials=self.credentials,
            vertica_warehouse_name=self.warehouse_name,
            sqoop_null_string=self.sqoop_null_string,
            sqoop_fields_terminated_by=self.sqoop_fields_terminated_by,
            sqoop_delimiter_replacement=self.sqoop_delimiter_replacement,
            overwrite=(self.overwrite_export and self.overwrite),
        )

    def get_column_names(self):
        """
        List names of columns as they should appear in the CSV.

        This must match the order they are stored in the exported warehouse table
        """
        return []  # should be implemented by child

    def run(self):
        """
        Clear out output if overwrite requested.
        """
        self.remove_output_on_overwrite()
        super(BaseProgramReportsTask, self).run()

    def mapper(self, line):
        """
        Group input by authoring institution and program
        """
        (org, program_title, program_uuid, content) = line.split('\t', 3)
        yield (org, program_uuid), line

    def multi_output_reducer(self, key, values, output_file):
        """
        Map export values to report output fields and write to csv.  Drops any extra columns
        """
        writer = csv.DictWriter(output_file, self.columns)
        writer.writeheader()

        for content in values:
            fields = content.split('\t')
            row = {field_key: field_value for field_key, field_value in zip(self.columns, fields)}
            writer.writerow(row)

    def output_path_for_key(self, key):
        org_key, program_uuid = key
        filename = u'{}__{}.csv'.format(self.report_name, self.date)
        return url_path_join(self.output_root, org_key, program_uuid, filename)


class BuildLearnerProgramReportTask(BaseProgramReportsTask):

    table_name = luigi.Parameter(
        default='learner_enrollments',
    )
    report_name = luigi.Parameter(
        default='learner_report'
    )

    @staticmethod
    def get_column_names():
        """
        List names of columns as they should appear in the CSV.

        This must match the order they are stored in the exported warehouse table
        """
        return [
            'Authoring Institution',
            'Program Title',
            'Program UUID',
            'Program Type',
            'User ID',
            'Username',
            'Name',
            'User Key',
            'Course Title',
            'Course Run Key',
            'External Course Key',
            'Track',
            'Grade',
            'Letter Grade',
            'Date First Enrolled',
            'Date Last Unenrolled',
            'Currently Enrolled',
            'Date First Upgraded to Verified',
            'Completed',
            'Date Completed'
        ]
