"""
Support for running a SQL script against an BigQuery database.
"""
import datetime
import logging

import luigi
import luigi.configuration

from edx.analytics.tasks.util.url import ExternalURL
from edx.analytics.tasks.util.vertica_target import CredentialFileVerticaTarget

log = logging.getLogger(__name__)


class BaseBigQuerySqlScriptTaskMixin(object):
    """
    Parameters for running a SQL script against an BigQuery database.
    """
    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        description='Default is today, UTC.',
    )
    read_timeout = luigi.IntParameter(
        config_path={'section': 'run-vertica-sql-script', 'name': 'read_timeout'},
        description='Timeout in seconds for reading from a Vertica database connection.'
    )

    def update_id(self):
        """
        Unique string identifying this task run, based on the input parameters.
        """
        return str(self)


class RunBigQuerySqlScriptTaskMixin(BaseBigQuerySqlScriptTaskMixin):
    """
    Parameters required to run a single SQL script against an HP BigQuery database.
    """
    source_script = luigi.Parameter(
        description='Path to the source script to execute.'
    )
    script_name = luigi.Parameter(
        description='Unique identifier for the purposes of tracking whether or not this '
        'script ran successfully i.e. the table created by this script, or the ticket related to it.'
    )
    cli_flags = luigi.Parameter(
        description='Bigquery flags to control if a table is replaced or appended to.'
    )
    destination_table = luigi.Parameter(
        description='Target table that will hold the result of the query.'
    )


class RunBigQuerySqlScriptTask(RunBigQuerySqlScriptTaskMixin, luigi.Task):
    """
    A task for running a SQL script against an HP BigQuery database.
    """
    required_tasks = None
    output_target = None
    depends_on = None

    def add_dependency(self, dependency):
        """
        Adds a custom dependency/requirement for this task.

        Note: this currently *sets* a single, custom dependency.  You cannot add multiple dependencies to this task.
        The last dependency to be added is the only one that will stick.  It will, however, not be the only dependency,
        as this task has a "base" set of dependencies.
        """
        self.depends_on = dependency

    def requires(self):
        if self.required_tasks is None:
            self.required_tasks = {
                'credentials': ExternalURL(url=self.credentials),
                'source_script': ExternalURL(url=self.source_script),
            }

            if self.depends_on is not None:
                self.required_tasks['depends_on'] = self.depends_on

        return self.required_tasks

    def output(self):
        """
        Returns a VerticaTarget representing the inserted dataset.
        """
        if self.output_target is None:
            self.output_target = CredentialFileVerticaTarget(
                credentials_target=self.input()['credentials'],
                table=self.script_name,
                schema=self.schema,
                update_id=self.update_id(),
                read_timeout=self.read_timeout,
                marker_schema=self.marker_schema,
            )

        return self.output_target

    def run(self):
        """
        Runs the given SQL script against the BigQuery target.
        """
        try:
            # TODO this is where I execute the SQL script
            with self.input()['source_script'].open('r') as script_file:
                # Read in our script and execute it.
                script_body = script_file.read()
                command = "bq query --use_legacy_sql=false --{cli_flags} --destination_table={destination_table} '{query}'".format(
                    cli_flags=self.cli_flags, destination_table=self.destination_table, query=script_body
                )


                # If we're here, nothing blew up, so mark as complete.
                log.debug("Committed transaction.")
        except Exception as exc:
            log.exception("Rolled back the transaction; exception raised: %s", str(exc))
            raise
