"""
Gather data using Sqoop table dumps run on RDBMS databases.
"""
import datetime
import json
import logging

import luigi
import luigi.configuration
import luigi.contrib.hadoop
import luigi.contrib.hdfs

from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join

log = logging.getLogger(__name__)

METADATA_FILENAME = '_metadata'


def load_sqoop_cmd():
    """Get path to sqoop command from Luigi configuration."""
    return luigi.configuration.get_config().get('sqoop', 'command', 'sqoop')


class SqoopImportMixin(object):
    """Mixin to expose useful parameters when importing from a database using Sqoop.

    In order to protect the database access credentials they are
    loaded from an external file which can be secured appropriately.
    The credentials file is expected to be JSON formatted and contain
    a simple map specifying the host, port, username password and
    database.

    Example Credentials File::

    {
        "host": "db.example.com",
        "port": "3306",
        "username": "exampleuser",
        "password": "example password"
    }
    """
    destination = luigi.Parameter(
        config_path={'section': 'database-import', 'name': 'destination'},
        description='The directory to write the output files to.',
    )
    credentials = luigi.Parameter(
        config_path={'section': 'database-import', 'name': 'credentials'},
        description='Path to the external access credentials file.',
    )
    database = luigi.Parameter(
        config_path={'section': 'database-import', 'name': 'database'},
    )
    num_mappers = luigi.Parameter(
        default=None,
        significant=False,
        description='The number of map tasks to ask Sqoop to use.',
    )
    verbose = luigi.BoolParameter(
        default=False,
        significant=False,
        description='Print more information while working.',
    )
    where = luigi.Parameter(
        default=None,
        description='A "where" clause to be passed to Sqoop.  Note that '
        'no spaces should be embedded and special characters should '
        'be escaped.  For example:  --where "id\<50". ',
    )


class SqoopImportTask(OverwriteOutputMixin, SqoopImportMixin, luigi.contrib.hadoop.BaseHadoopJobTask):
    """
    An abstract task that uses Sqoop to read data out of a database and
    writes it to a file in CSV format.

    Inherited parameters:
        overwrite:  Overwrite any existing imports.  Default is false.

    """
    table_name = luigi.Parameter(
        description='The name of the table to import.',
    )
    columns = luigi.ListParameter(
        default=[],
        description='A list of column names to be included.  Default is to include all columns.'
    )
    null_string = luigi.Parameter(
        default=None,
        description='String to use to represent NULL values in output data.',
    )
    fields_terminated_by = luigi.Parameter(
        default=None,
        description='Defines the field separator to use on output.',
    )
    delimiter_replacement = luigi.Parameter(
        default=None,
        description='Defines a character to use as replacement for delimiters '
        'that appear within data values, for use with Hive.  Not specified by default.'
    )
    escaped_by = luigi.Parameter(
        default=None,
        description='Defines the character to use on output to escape delimiter values when they appear in field values.',
    )
    enclosed_by = luigi.Parameter(
        default=None,
        description='Defines the character to use on output to enclose field values.',
    )
    optionally_enclosed_by = luigi.Parameter(
        default=None,
        description='Defines the character to use on output to enclose field values when they may contain a delimiter.',
    )
    additional_metadata = luigi.DictParameter(
        default=None,
        significant=False,
        description='Override this to provide the metadata file with additional information about the Sqoop output.',
    )

    def requires(self):
        return {
            'credentials': ExternalURL(url=self.credentials),
        }

    def output(self):
        return get_target_from_url(self.destination + '/')

    def metadata_output(self):
        """Return target to which metadata about the task execution can be written."""
        return get_target_from_url(url_path_join(self.destination, METADATA_FILENAME))

    def marker_output(self):
        """Return target for _SUCCESS marker indicating the task was successfully completed."""
        return get_target_from_url(url_path_join(self.destination, "_SUCCESS"))

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
        generic_args = ['--connect', url, '--username', cred['username']]

        if self.verbose:
            generic_args.append('--verbose')

        # write password to temp file object, and pass name of file to Sqoop:
        with password_target.open('w') as password_file:
            password_file.write(cred['password'])
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
        if self.escaped_by is not None:
            arglist.extend(['--escaped-by', self.escaped_by])
        if self.enclosed_by is not None:
            arglist.extend(['--enclosed-by', self.enclosed_by])
        if self.optionally_enclosed_by is not None:
            arglist.extend(['--optionally-enclosed-by', self.optionally_enclosed_by])
        return arglist

    def source_database_type(self):
        """Metadata about the type of source database that is being dumped."""
        return 'unknown'

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
        cred = {}
        with self.input()['credentials'].open('r') as credentials_file:
            cred = json.load(credentials_file)
        return cred

    def complete(self):
        """
        Wrap Task.complete() to check for metadata and marker file as well as data.
        """
        data_complete = super(SqoopImportTask, self).complete()
        if data_complete and self.marker_output().exists() and self.metadata_output().exists():
            return True
        else:
            return False


class SqoopImportFromMysql(SqoopImportTask):
    """
    An abstract task that uses Sqoop to read data out of a MySQL database and writes it to a file in CSV format.

    By default, the output format is defined by meaning of --mysql-delimiters option, which defines defaults used by
    mysqldump tool:

    * fields delimited by comma
    * lines delimited by \n
    * delimiters escaped by backslash
    * delimiters optionally enclosed by single quotes (')

    """
    mysql_delimiters = luigi.BoolParameter(
        default=True,
        description='Use standard mysql delimiters (on by default).',
    )
    direct = luigi.BoolParameter(
        default=True,
        significant=False,
        description='Use mysqldumpi\'s "direct" mode.  Requires that no set of columns be selected.',
    )

    def connection_url(self, cred):
        """Construct connection URL from provided credentials."""
        return 'jdbc:mysql://{host}/{database}'.format(host=cred['host'], database=self.database)

    def import_args(self):
        """Returns list of arguments specific to Sqoop import from a Mysql database."""
        arglist = super(SqoopImportFromMysql, self).import_args()
        if self.direct:
            arglist.append('--direct')
        if self.mysql_delimiters:
            arglist.append('--mysql-delimiters')
        return arglist

    def source_database_type(self):
        return 'mysql'


class SqoopPasswordTarget(luigi.contrib.hdfs.HdfsTarget):
    """Defines a temp file in HDFS to hold password."""
    def __init__(self):
        super(SqoopPasswordTarget, self).__init__(is_tmp=True)


class SqoopImportRunner(luigi.contrib.hadoop.JobRunner):
    """Runs a SqoopImportTask by shelling out to sqoop."""

    def run_job(self, job):
        """Runs a SqoopImportTask by shelling out to sqoop."""

        # First remove output if the overwrite flag is set.
        job.remove_output_on_overwrite()

        # Sometimes the job will be run by another workflow running at
        # the same time, and so this will already become complete.
        # Sqoop cannot rerun the command line if the output file already
        # exists -- Hadoop returns a FileAlreadyExistsException error from
        # org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.checkOutputSpecs().
        # Just check here first before running, and do nothing if it's already complete.
        if job.complete():
            log.warning("Skipping output of %s -- file already exists!", job.marker_output().path)
            return

        # And so, if it's not complete but there is partial output, it needs to be removed
        # before Sqoop can be run.
        if job.output().exists():
            log.info("Removing existing partial output for task %s", str(job))
            job.output().remove()

        metadata = {
            'start_time': datetime.datetime.utcnow().isoformat(),
            'format': {
                'table_name': job.table_name,
                'columns': job.columns,
                'null_string': job.null_string,
                'fields_terminated_by': job.fields_terminated_by,
                'delimiter_replacement': job.delimiter_replacement,
                'escaped_by': job.escaped_by,
                'optionally_enclosed_by': job.optionally_enclosed_by,
            },
            'source_database_type': job.source_database_type(),
        }

        additional_metadata = job.additional_metadata
        if additional_metadata:
            metadata['additional_metadata'] = dict(additional_metadata)

        try:
            # Create a temp file in HDFS to store the password,
            # so it isn't echoed by the hadoop job code.
            # It should be deleted when it goes out of scope
            # (using __del__()), but safer to just make sure.
            password_target = SqoopPasswordTarget()
            arglist = job.get_arglist(password_target)
            luigi.contrib.hadoop.run_and_track_hadoop_job(arglist)
        finally:
            password_target.remove()
            metadata['end_time'] = datetime.datetime.utcnow().isoformat()
            try:
                with job.metadata_output().open('w') as metadata_file:
                    json.dump(metadata, metadata_file)
            except Exception:
                log.exception("Unable to dump metadata information.")
                pass
