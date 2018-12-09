"""
Gather data using Sqoop table dumps run on RDBMS databases.
"""
import datetime
import json
import logging

import luigi
import luigi.hadoop
import luigi.hdfs
import luigi.configuration

from edx.analytics.tasks.url import ExternalURL
from edx.analytics.tasks.url import get_target_from_url
from edx.analytics.tasks.url import url_path_join
from edx.analytics.tasks.util.credentials import CredentialsUrl
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin

log = logging.getLogger(__name__)


def load_sqoop_cmd():
    """Get path to sqoop command from Luigi configuration."""
    return luigi.configuration.get_config().get('sqoop', 'command', 'sqoop')


class SqoopImportTask(OverwriteOutputMixin, luigi.hadoop.BaseHadoopJobTask):
    """
    An abstract task that uses Sqoop to read data out of a database and
    writes it to a file in CSV format.

    In order to protect the database access credentials they are
    loaded from an external file which can be secured appropriately.
    The credentials file is expected to be JSON formatted and contain
    a simple map specifying the host, port, username password and
    database.

    Parameters:
        credentials: Path to the external access credentials file.
        destination: The directory to write the output files to.
        table_name: The name of the table to import.
        num_mappers: The number of map tasks to ask Sqoop to use.
        where:  A 'where' clause to be passed to Sqoop.  Note that
            no spaces should be embedded and special characters should
            be escaped.  For example:  --where "id\<50".
        verbose: Print more information while working.
        columns: A list of column names to be included.  Default is to include all columns.
        null_string:  String to use to represent NULL values in output data.
        fields_terminated_by:  defines the file separator to use on output.
        delimiter_replacement:  defines a character to use as replacement for delimiters
            that appear within data values, for use with Hive.  (Not specified by default.)

    Inherited parameters:
        overwrite:  Overwrite any existing imports.  Default is false.

    Example Credentials File::

        {
            "host": "db.example.com",
            "port": "3306",
            "username": "exampleuser",
            "password": "example password"
        }
    """
    destination = luigi.Parameter(
        default_from_config={'section': 'database-import', 'name': 'destination'}
    )
    credentials = luigi.Parameter(
        default_from_config={'section': 'database-import', 'name': 'credentials'}
    )
    database = luigi.Parameter(
        default_from_config={'section': 'database-import', 'name': 'database'}
    )
    num_mappers = luigi.Parameter(default=None)
    verbose = luigi.BooleanParameter(default=False)
    table_name = luigi.Parameter()
    where = luigi.Parameter(default=None)
    columns = luigi.Parameter(is_list=True, default=[])
    null_string = luigi.Parameter(default=None)
    fields_terminated_by = luigi.Parameter(default=None)
    delimiter_replacement = luigi.Parameter(default=None)

    def requires(self):
        return {
            'credentials': CredentialsUrl(url=self.credentials),
        }

    def output(self):
        return get_target_from_url(self.destination + '/')

    def metadata_output(self):
        """Return target to which metadata about the task execution can be written."""
        return get_target_from_url(url_path_join(self.destination, '.metadata'))

    def job_runner(self):
        """Use simple runner that gets args from the job and passes through."""
        return SqoopImportRunner()

    def get_arglist(self, password_file):
        """Returns list of arguments for running Sqoop."""
        arglist = [load_sqoop_cmd(), 'import']
        # Generic args should be passed to sqoop first, followed by import-specific args.
        arglist.extend(self.generic_args(password_file))
        arglist.extend(self.import_args())
        return arglist

    def generic_args(self, password_target):
        """Returns list of arguments used by all Sqoop commands, using credentials read from file."""
        cred = self._get_credentials()
        url = self.connection_url(cred)
        generic_args = ['--connect', url, '--username', cred.username]

        if self.verbose:
            generic_args.append('--verbose')

        # write password to temp file object, and pass name of file to Sqoop:
        with password_target.open('w') as password_file:
            password_file.write(cred.password)
            password_file.flush()
        generic_args.extend(['--password-file', password_target.path])

        return generic_args

    def import_args(self):
        """Returns list of arguments specific to Sqoop import."""
        arglist = [
            '--table', self.table_name,
            '--target-dir', self.destination,
        ]
        if len(self.columns) > 0:
            arglist.extend(['--columns', ','.join(self.columns)])
        if self.num_mappers is not None:
            arglist.extend(['--num-mappers', str(self.num_mappers)])
        if self.where is not None:
            arglist.extend(['--where', str(self.where)])
        if self.null_string is not None:
            arglist.extend(['--null-string', self.null_string, '--null-non-string', self.null_string])
        if self.fields_terminated_by is not None:
            arglist.extend(['--fields-terminated-by', self.fields_terminated_by])
        if self.delimiter_replacement is not None:
            arglist.extend(['--hive-delims-replacement', self.delimiter_replacement])

        return arglist

    def connection_url(self, _cred):
        """Construct connection URL from provided credentials."""
        raise NotImplementedError  # pragma: no cover

    def _get_credentials(self):
        """
        Gathers the secure connection parameters from an external file
        and uses them to establish a connection to the database
        specified in the secure parameters.

        Returns:
            A dict containing credentials.
        """
        return self.input()['credentials']


class SqoopImportFromMysql(SqoopImportTask):
    """
    An abstract task that uses Sqoop to read data out of a database and writes it to a file in CSV format.

    By default, the output format is defined by meaning of --mysql-delimiters option, which defines defaults used by
    mysqldump tool:

    * fields delimited by comma
    * lines delimited by \n
    * delimiters escaped by backslash
    * delimiters optionally enclosed by single quotes (')

    Parameters:
        direct: use mysqldump's "direct" mode.  Requires that no set of columns be selected. Defaults to True.
        mysql-delimiters:  use standard mysql delimiters (on by default).
    """
    mysql_delimiters = luigi.BooleanParameter(default=True)
    direct = luigi.BooleanParameter(default=True, significant=False)

    def connection_url(self, cred):
        """Construct connection URL from provided credentials."""
        return 'jdbc:mysql://{host}/{database}'.format(host=cred.host, database=self.database)

    def import_args(self):
        """Returns list of arguments specific to Sqoop import from a Mysql database."""
        arglist = super(SqoopImportFromMysql, self).import_args()
        if self.direct:
            arglist.append('--direct')
        if self.mysql_delimiters:
            arglist.append('--mysql-delimiters')
        return arglist


class SqoopPasswordTarget(luigi.hdfs.HdfsTarget):
    """Defines a temp file in HDFS to hold password."""
    def __init__(self):
        super(SqoopPasswordTarget, self).__init__(is_tmp=True)


class SqoopImportRunner(luigi.hadoop.JobRunner):
    """Runs a SqoopImportTask by shelling out to sqoop."""

    def run_job(self, job):
        """Runs a SqoopImportTask by shelling out to sqoop."""
        job.remove_output_on_overwrite()

        metadata = {
            'start_time': datetime.datetime.utcnow().isoformat()
        }
        try:
            # Create a temp file in HDFS to store the password,
            # so it isn't echoed by the hadoop job code.
            # It should be deleted when it goes out of scope
            # (using __del__()), but safer to just make sure.
            password_target = SqoopPasswordTarget()
            arglist = job.get_arglist(password_target)
            luigi.hadoop.run_and_track_hadoop_job(arglist)
        finally:
            password_target.remove()
            metadata['end_time'] = datetime.datetime.utcnow().isoformat()
            try:
                with job.metadata_output().open('w') as metadata_file:
                    json.dump(metadata, metadata_file)
            except Exception:
                log.exception("Unable to dump metadata information.")
                pass
