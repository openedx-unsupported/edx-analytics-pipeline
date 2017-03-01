import logging

import luigi
from luigi import configuration
from luigi.contrib.redshift import S3CopyToTable
from edx.analytics.tasks.insights.enrollments import EnrollmentSummaryRecord
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin

log = logging.getLogger(__name__)


class RedshiftS3CopyToTable(OverwriteOutputMixin, S3CopyToTable):

    path = luigi.Parameter()

    def credentials(self):
        config = configuration.get_config()
        section = 'redshift'
        return {
            'host': config.get(section, 'host'),
            'port': config.get(section, 'port'),
            'database': config.get(section, 'database'),
            'user': config.get(section, 'user'),
            'password': config.get(section, 'password'),
            'aws_account_id': config.get(section, 'account_id'),
            'aws_arn_role_name': config.get(section, 'role_name'),
        }

    def s3_load_path(self):
        return self.path

    @property
    def aws_account_id(self):
        return self.credentials()['aws_account_id']

    @property
    def aws_arn_role_name(self):
        return self.credentials()['aws_arn_role_name']

    @property
    def host(self):
        return self.credentials()['host'] + ':' +  self.credentials()['port']

    @property
    def database(self):
        return self.credentials()['database']

    @property
    def user(self):
        return self.credentials()['user']

    @property
    def password(self):
        return self.credentials()['password']

    @property
    def aws_access_key_id(self):
        return ''

    @property
    def aws_secret_access_key(self):
        return ''

    @property
    def copy_options(self):
        return "DELIMITER '\t'"

    @property
    def table_attributes(self):
        return ''

    @property
    def table_type(self):
        return ''

    def create_table(self, connection):
        if len(self.columns[0]) == 1:
            # only names of columns specified, no types
            raise NotImplementedError("create_table() not implemented "
                                      "for %r and columns types not "
                                      "specified" % self.table)
        elif len(self.columns[0]) == 2:
            # if columns is specified as (name, type) tuples
            coldefs = ','.join(
                '{name} {type}'.format(
                    name=name,
                    type=type) for name, type in self.columns
            )
            query = ("CREATE {type} TABLE IF NOT EXISTS "
                     "{table} ({coldefs}) "
                     "{table_attributes}").format(
                type=self.table_type,
                table=self.table,
                coldefs=coldefs,
                table_attributes=self.table_attributes)

            connection.cursor().execute(query)
        else:
            raise ValueError("create_table() found no columns for %r"
                             % self.table)


    def truncate_table(self, connection):
        query = "truncate %s" % self.table
        cursor = connection.cursor()
        try:
            cursor.execute(query)
        finally:
            cursor.close()

    def init_copy(self, connection):
        self.create_table(connection)

        self.attempted_removal = True
        if self.overwrite:
            self.truncate_table(connection)

    def init_touch(self, connection):
        if self.overwrite:
            marker_table = self.output().marker_table
            query = "DELETE FROM {marker_table} where target_table='{target_table}';".format(
                marker_table=marker_table,
                target_table=self.table,
            )
            log.debug(query)
            connection.cursor().execute(query)

    def run(self):
        if not (self.table):
            raise Exception("table need to be specified")

        path = self.s3_load_path()
        output = self.output()
        connection = output.connect()
        cursor = connection.cursor()

        self.init_copy(connection)
        self.copy(cursor, path)

        self.init_touch(connection)
        output.touch(connection)
        connection.commit()

        # commit and clean up
        connection.close()

    def copy(self, cursor, f):
        cursor.execute("""
         COPY {table} from '{source}'
         IAM_ROLE 'arn:aws:iam::{id}:role/{role}'
         {options}
         ;""".format(
            table=self.table,
            source=f,
            id=self.aws_account_id,
            role=self.aws_arn_role_name,
            options=self.copy_options)
        )

    def update_id(self):
        return str(self)


class LoadUserCourseSummaryToRedshift(RedshiftS3CopyToTable):
    columns = EnrollmentSummaryRecord.get_sql_schema()

    @property
    def table_attributes(self):
        return 'DISTKEY(user_id) SORTKEY(user_id)'

    @property
    def table(self):
        return 'd_user_course'
