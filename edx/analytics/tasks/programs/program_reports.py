import luigi

from edx.analytics.tasks.common.mapreduce import MapReduceJobTask
from edx.analytics.tasks.common.sqoop import SqoopImportFromVertica
from edx.analytics.tasks.common.vertica_export import ExportVerticaTableToS3Task, get_vertica_table_schema
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin

from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join

class BuildProgramReportsTask(OverwriteOutputMixin, luigi.Task):
    """ Generates CSV reports on program enrollment """

    credentials = luigi.Parameter(
        config_path={'section': 'vertica-export', 'name': 'credentials'},
        description='Path to the external access credentials file.',
    )
    output_root = luigi.Parameter(
        description='URL pointing to the location reports should be stored',
    )
    schema_name = luigi.Parameter(
        default='programs_reporting',
        description='Vertica schema containing reporting table',
    )
    table_name = luigi.Parameter(
        default='z_test_table',
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

    def run(self):

        table_schema = get_vertica_table_schema(
            self.credentials,
            self.schema_name,
            self.table_name,
        )
 
        column_list = []
        for field_name, vertica_field_type, _ in table_schema:
            column_list.append(field_name)

        with self.input().open('r') as input_file:
            lines = input_file.read().splitlines()

        with self.output().open('w') as output_file:
            # print csv rows to console until we have s3 access
            print('--CSV CONTENT--')
            header = ','.join(column_list)
            output_file.write(header + '\n')
            print(header)
            for line in lines:
                output_file.write(line + '\n')
                print(line)

    def output(self):
        return get_target_from_url(url_path_join(self.output_root, 'result.csv'))
