import csv
import datetime
from itertools import islice
import logging

import luigi

from edx.analytics.tasks.common.mapreduce import MultiOutputMapReduceJobTask
from edx.analytics.tasks.common.vertica_export import ExportVerticaTableToS3Task, get_vertica_table_schema
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join

log = logging.getLogger(__name__)

class BuildProgramReportsTask(OverwriteOutputMixin, MultiOutputMapReduceJobTask):
    """ Generates CSV reports on program enrollment """

    credentials = luigi.Parameter(
        config_path={'section': 'vertica-export', 'name': 'credentials'},
        description='Path to the external access credentials file.',
    )
    output_root = luigi.Parameter(
        description='URL pointing to the location reports should be stored',
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
        default=',',
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

    def __init__(self, *args, **kwargs):
        super(BuildProgramReportsTask, self).__init__(*args, **kwargs)
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
            overwrite=False,
        )

    def get_columns(self):
        # TODO: any way to make this callable on __init__()?
        table_schema = get_vertica_table_schema(
            self.credentials,
            self.schema_name,
            self.table_name,
        )
 
        column_list = []
        for field_name, vertica_field_type, _ in table_schema:
            column_list.append(field_name)
        return column_list

    def get_column_names(self):
        return [
            'User ID',
            'Program Title',
            'Program ID',
            'Program UUID',
            'Course ID',
            'Track',
            'Created',
        ]

    def run(self):
        """
        Clear out output if overwrite requested.
        """
        self.remove_output_on_overwrite()

        super(BuildProgramReportsTask, self).run()

    def mapper(self, line):
        """ Group input by program"""
        program_id = line.split(',')[1]
        yield program_id, line

    def multi_output_reducer(self, key, values, output_file):
        writer = csv.DictWriter(output_file, self.columns)
        writer.writerow(dict(
            (k, k) for k in self.columns
        ))

        row_data = []
        for content in values:
            fields = content.split(',')
            row = {field_key: field_value for field_key, field_value in zip(self.columns, fields)}
            row_data.append(row)

        for row_dict in row_data:
            writer.writerow(row_dict)

    def output_path_for_key(self, key):
        filename = u'{}.csv'.format(self.date)
        return url_path_join(self.output_root, key, filename)
