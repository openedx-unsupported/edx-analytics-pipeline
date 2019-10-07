import luigi

from edx.analytics.tasks.common.mapreduce import MapReduceJobTask
from edx.analytics.tasks.common.sqoop import SqoopImportFromVertica
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin

from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join

class BuildProgramReportsTask(luigi.Task):

    credentials = luigi.Parameter(
        config_path={'section': 'vertica-export', 'name': 'credentials'},
        description='Path to the external access credentials file.',
    )
    output_root = luigi.Parameter(
        description='URL pointing to the location reports should be stored'
    )
    # defaults
    warehouse_name = luigi.Parameter(
        default='docker',
        description='The Vertica warehouse that houses the report schema.'
    )
    sqoop_null_string = luigi.Parameter(
        default='null',
        description='A string replacement value for any (null) values encountered by Sqoop when exporting from Vertica.'
    )
    sqoop_fields_terminated_by = luigi.Parameter(
        default=',',
        description='The field delimiter used by Sqoop.'
    )
    sqoop_delimiter_replacement = luigi.Parameter(
        default=' ',
        description='The string replacement value for special characters encountered by Sqoop when exporting from '
                    'Vertica.'
    )
    column_list = luigi.ListParameter(
        default=['*'],
        description='The column names being extracted from this table.'
    )

    def requires(self):
        target_url = url_path_join(self.output_root, '/export')

        return SqoopImportFromVertica(
            schema_name='programs_reporting',
            table_name='learner_enrollments',
            credentials=self.credentials,
            database=self.warehouse_name,
            columns=self.column_list,
            destination=target_url,
            overwrite=True,
            null_string=self.sqoop_null_string,
            fields_terminated_by=self.sqoop_fields_terminated_by,
            delimiter_replacement=self.sqoop_delimiter_replacement,
            verbose=True
        )

    def run(self):

        with self.input().open('r') as input_file:
            lines = input_file.read().splitlines()
            print(lines)

        with self.output().open('w') as output_file:
            output_file.write(','.join(self.column_list) + '\n')
            for line in lines:
                output_file.write(line + '\n')

    def output(self):
        return get_target_from_url(url_path_join(self.output_root, 'result.csv'))
