"""Task for loading cleaned, gzipped event log data into Vertica."""
import datetime
import luigi
import logging
from edx.analytics.tasks.url import get_target_from_url, url_path_join

from vertica_load import VerticaCopyTask, VerticaCopyTaskMixin
from clean_for_vertica import CleanForVerticaTask

log = logging.getLogger(__name__)


class VerticaEventLoadingTask(VerticaCopyTask):
    """
    A subclass of the Vertica bulk loading task that specifically loads flex tables from gzipped
    json event log files.
    """

    # By default, use flex tables, since we may want to materialize additional columns
    use_flex = luigi.Parameter(default=True)
    run_date = luigi.DateParameter(default=datetime.datetime.utcnow().date())
    remove_implicit = luigi.BooleanParameter(default=True)

    @property
    def insert_source_task(self):
        """The previous task in the workflow is to clean the data for loading into Vertica."""
        return(CleanForVerticaTask(date=self.run_date, remove_implicit=self.remove_implicit))

    @property
    def table(self):
        """We use the table event_logs for this task."""
        return "event_logs"

    def create_table(self, connection):
        """Overriden because we will create a flex table instead of a traditional table."""
        if not self.use_flex and len(self.columns[0]) != 2:
            # only names of columns specified, no types, which is fine for flex tables
            # where there might not be columns at all, but which is bad for columnar tables
            raise NotImplementedError(
                "create_table() not implemented for %r and columns types not specified"
                % self.table
            )

        # Assumes that columns are specified as (name, definition) tuples
        coldefs = self.create_column_definitions()

        type = "FLEX TABLE"
        if not self.use_flex:
            type = "TABLE"

        query = "CREATE {type} IF NOT EXISTS {schema}.{table} ({coldefs})".format(
            type=type, schema=self.schema, table=self.table, coldefs=coldefs
        )
        log.debug(query)
        connection.cursor().execute(query)

    @property
    def columns(self):
        """Overriden with the specific materialized columns we know we want in this (flex|columnar) table."""
        return [
            ('\"agent.type\"', 'VARCHAR(20)'),
            ('\"agent.device_name\"', 'VARCHAR(100)'),
            ('\"agent.os\"', 'VARCHAR(100)'),
            ('\"agent.browser\"', 'VARCHAR(100)'),
            ('\"agent.touch_capable\"', 'BOOLEAN'),
            ('event', 'VARCHAR(65000)'),
            ('event_type', 'VARCHAR(200)'),
            ('event_source', 'VARCHAR(20)'),
            ('host', 'VARCHAR(80)'),
            ('ip', 'VARCHAR(20)'),
            ('page', 'VARCHAR(1000)'),
            ('time', 'TIMESTAMP'),
            ('username', 'VARCHAR(30)'),
            ('\"context.course_id\"', 'VARCHAR(100)'),
            ('\"context.org_id\"', 'VARCHAR(100)'),
            ('\"context.user_id\"', 'INTEGER'),
            ('\"context.path\"', 'VARCHAR(500)')
        ]

        # Old, pre- agent-canonicalization columns

        # return [
        #     ('agent', 'VARCHAR(200)'),
        #     ('event', 'VARCHAR(65000)'),
        #     ('event_type', 'VARCHAR(200)'),
        #     ('event_source', 'VARCHAR(20)'),
        #     ('host', 'VARCHAR(80)'),
        #     ('ip', 'VARCHAR(20)'),
        #     ('page', 'VARCHAR(1000)'),
        #     ('time', 'TIMESTAMP'),
        #     ('username', 'VARCHAR(30)'),
        #     ('\"context.course_id\"', 'VARCHAR(100)'),
        #     ('\"context.org_id\"', 'VARCHAR(100)'),
        #     ('\"context.user_id\"', 'INTEGER'),
        #     ('\"context.path\"', 'VARCHAR(500)')
        # ]

    @property
    def auto_primary_key(self):
        """Overriden because we don't need the auto primary key (because we have __raw__ or similar)."""
        return None

    @property
    def default_columns(self):
        """Overridden since the superclass method includes a time of insertion column we don't want in this table."""
        return None

    # TODO: will the DIRECT flag here work? speed up the query?
    def copy_data_table_from_target(self, cursor):
        """Overriden since we need to use the json parser."""
        with self.input()['insert_source'].open('r') as insert_source_file:
            # If we are using an overwrite, the overwriting commands may cause the queue of messages
            # from the database to have extra information, which will confuse the copy query.
            if self.overwrite:
                cursor.flush_to_query_ready()

            cursor.copy_stream("COPY {schema}.{table} FROM STDIN PARSER fjsonparser() NO COMMIT;"
                               .format(schema=self.schema, table=self.table), insert_source_file)


class VerticaEventLoadingWorkflow(VerticaCopyTaskMixin, luigi.WrapperTask):
    """
    Workflow for encapsulating the Vertica event loading task and passing in parameters.
    Writes in events with the individual days' worth of events as the atomic level; each day is a separate task
    and will succeed or fail separately; query the marker table to determine which ones succeed.
    """
    interval = luigi.DateIntervalParameter()
    remove_implicit = luigi.BooleanParameter(default=True)
    use_flex = luigi.BooleanParameter(default=True)

    def requires(self):
        # Add additional args for VerticaCopyMixin.
        kwargs2 = {
            'schema': self.schema,
            'credentials': self.credentials,
        }
        kwargs2.update(kwargs2)

        for day in luigi.DateIntervalParameter().parse(str(self.interval)):
            task_needed = VerticaEventLoadingTask(run_date=day, remove_implicit=self.remove_implicit,
                                                  use_flex=self.use_flex, **kwargs2)
            yield task_needed
