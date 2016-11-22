"""
Support for running a SQL script against an HP Vertica database.
"""
import datetime
import logging

import luigi
import luigi.configuration
from edx.analytics.tasks.url import ExternalURL
from edx.analytics.tasks.util.vertica_target import VerticaTarget, CredentialFileVerticaTarget

log = logging.getLogger(__name__)

try:
    import vertica_python
    vertica_client_available = True  # pylint: disable-msg=C0103
except ImportError:
    log.warn('Unable to import Vertica client libraries')
    # On hadoop slave nodes we don't have Vertica client libraries installed so it is pointless to ship this package to
    # them, instead just fail noisily if we attempt to use these libraries.
    vertica_client_available = False  # pylint: disable-msg=C0103


class BaseVerticaSqlScriptTaskMixin(object):
    """
    Parameters for running a SQL script against an HP Vertica database.
    """
    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        description='Default is today, UTC.',
    )
    schema = luigi.Parameter(
        config_path={'section': 'run-vertica-sql-script', 'name': 'schema'},
        description='Name of the schema to which to write.',
    )
    credentials = luigi.Parameter(
        config_path={'section': 'run-vertica-sql-script', 'name': 'credentials'},
        description='Path to the external access credentials file.',
    )
    read_timeout = luigi.IntParameter(
        config_path={'section': 'run-vertica-sql-script', 'name': 'read_timeout'},
        description='Timeout in seconds for reading from a Vertica database connection.'
    )
    marker_schema = luigi.Parameter(
        default=None,
        description='Name of the schema to which to write the marker table. marker_schema would '
        'default to the schema value if the value here is None.'
    )

    def update_id(self):
        """
        Unique string identifying this task run, based on the input parameters.
        """
        return str(self)


class RunVerticaSqlScriptTaskMixin(BaseVerticaSqlScriptTaskMixin):
    """
    Parameters required to run a single SQL script against an HP Vertica database.
    """
    source_script = luigi.Parameter(
        description='Path to the source script to execute.'
    )
    script_name = luigi.Parameter(
        description='Unique identifier for the purposes of tracking whether or not this '
        'script ran successfully i.e. the table created by this script, or the ticket related to it.'
    )


class RunVerticaSqlScriptTask(RunVerticaSqlScriptTaskMixin, luigi.Task):
    """
    A task for running a SQL script against an HP Vertica database.
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
        Runs the given SQL script against the Vertica target.
        """
        # Make sure we can connect to Vertica.
        self.check_vertica_availability()
        connection = self.output().connect()

        try:
            # Set up our connection to point to the specified schema so that scripts can have unqualified
            # table references and not necessarily need to know or care about where they're running.
            connection.cursor().execute('SET SEARCH_PATH = {schema};'.format(schema=self.schema))

            with self.input()['source_script'].open('r') as script_file:
                # Read in our script and execute it.
                script_body = script_file.read()
                connection.cursor().execute(script_body)

                # If we're here, nothing blew up, so mark as complete.
                self.output().touch(connection)

                connection.commit()
                log.debug("Committed transaction.")
        except Exception as exc:
            log.exception("Rolled back the transaction; exception raised: %s", str(exc))
            connection.rollback()
            raise
        finally:
            connection.close()

    def check_vertica_availability(self):
        """Call to ensure fast failure if this machine doesn't have the Vertica client library available."""
        if not vertica_client_available:
            raise ImportError('Vertica client library not available')
