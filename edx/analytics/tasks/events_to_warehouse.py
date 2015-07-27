"""Task for loading cleaned, gzipped event log data into Vertica."""
import datetime
import os
import luigi
import logging
import gzip
# from edx.analytics.tasks.clean_for_vertica import CleanForVerticaTask
from edx.analytics.tasks.url import get_target_from_url, url_path_join

from vertica_load import VerticaCopyTask, VerticaCopyTaskMixin
from clean_for_vertica import CleanForVerticaTask
import vertica_python

log = logging.getLogger(__name__)

class DummyTarget(luigi.Target):
    def exists(self):
        try:
            open('part-00079.gz', 'rw')
            return True
        except IOError:
            return False

    def open(self, mode):
        return open('part-00079.gz', mode)

class DummyTarget2(luigi.Target):
    def exists(self):
        try:
            open('done', 'rw')
            return True
        except IOError:
            return False

    def open(self, mode):
        return open('done', mode)

class DummyTarget3(luigi.Target):
    def exists(self):
        try:
            open('done_copying', 'rw')
            return True
        except IOError:
            return False

    def open(self, mode):
        return open('done_copying', mode)

class LocalLuigiTestInput(luigi.Task):
    id = luigi.IntParameter()

    def run(self):
        print self.id
        print "HERE WE ARE"

    def output(self):
        return DummyTarget()

class LocalLuigiTestTask(luigi.Task):
    def requires(self):
        return VerticaEventLoadingTask(overwrite=True, schema='experimental', credentials='vertica_creds', interval='2015-07')

    def run(self):
        print "HELLO, WORLD!"
        b = luigi.DateIntervalParameter('2015-07')
        for d in luigi.DateIntervalParameter('2015-07-01/03').parse(b):
            print str(type(d))
            b = luigi.DateParameter(d)
        self.output().open('w').write("DONE!")

    def output(self):
        luigi.DateIntervalParameter('2015-07-01/03').parse('2015-07-01-2015-07-03')
        return DummyTarget2()


class Dummy4(luigi.Task):
    def output(self):
        return get_target_from_url('/Users/jamesrowan/test_loader_folder/part-00024')
        # return luigi.LocalTarget('/Users/jamesrowan/test_loader_folder/')
        # print str(type(get_target_from_url('s3://edx-analytics-data/dev/warehouse/events-vertica/dt=2015-07-20/part-00023')))
        # return get_target_from_url('s3://edx-analytics-data/dev/warehouse/events-vertica/dt=2015-07-20/part-00023')


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
        # return Dummy4()
        return(CleanForVerticaTask(date=self.run_date, remove_implicit=self.remove_implicit))
        # for day in luigi.DateIntervalParameter().parse(self.run_date):
        #     yield CleanForVerticaTask(date=day, remove_implict=self.remove_implict.value)

    def requires(self):
        return super(VerticaEventLoadingTask, self).requires + [LogConcatenator(run_date=self.run_date,
                                                                                remove_implicit=self.remove_implicit)]

    @property
    def table(self):
        """We use the table event_logs for this task."""
        return "event_logs_test"

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
            ('agent', 'VARCHAR(200)'),
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

    @property
    def auto_primary_key(self):
        """Overriden because we don't need the auto primary key (because we have __raw__ or similar)."""
        return None

    @property
    def default_columns(self):
        """Overridden since the superclass method includes a time of insertion column we don't want in this table."""
        return None

    # TODO: we are flattening maps right now, right?
    # TODO: address the vertica copy not doing the problem, maybe?
    def copy_data_table_from_target(self, cursor):
        """Overriden since we copy from gzip files and need to use the json parser."""

        # This one causes 100 Java IOExceptions about "inability to stream" to be thrown, one for each part file
        # with self.input()['insert_source'].open('r') as insert_source_file:
        #     print "HELLO, WE OPENED IT!"
        #     cursor.copy_stream("COPY {schema}.{table} FROM STDIN GZIP PARSER fjsonparser() NO COMMIT;"
        #                        .format(schema=self.schema, table=self.table), insert_source_file)
        #     print "NO ERRORS THROWN!"

        # trying nongzipped locally
        # with self.input()['insert_source'].open('r') as insert_source_file:

        # insert_source_file = self.input()['insert_source'].open('r')

        if hasattr(self.input()['insert_source'], 'format'):
            print self.input()['insert_source'].format

        print self.input()['insert_source'].fs
        print self.input()['insert_source'].fs.listdir()

        with self.input()['insert_source'].open('r') as insert_source_file:
            print "HELLO, WE OPENED IT!"

            # If we are using an overwrite
            if self.overwrite:
                cursor.flush_to_query_ready()

            cursor.copy_stream("COPY {schema}.{table} FROM STDIN GZIP PARSER fjsonparser() NO COMMIT;"
                               .format(schema=self.schema, table=self.table), insert_source_file)

            # while True:
            #     message = cursor.connection.read_message()
            #
            #     print message

            print "NO ERRORS THROWN!"

        print "closed"


        # This one fails because gzip.open expects a string or buffer

        # for place in self.input()['insert_source'].fs.listdir(self.input()['insert_source'].path):
        #     print place
        #     with gzip.open(get_target_from_url(url_path_join(self.input()['insert_source'].path, place), 'r')) as insert_source_file:
        #         cursor.copy_file("COPY {schema}.{table} FROM STDIN PARSER fjsonparser() NO COMMIT;"
        #                          .format(schema=self.schema, table=self.table), insert_source_file, decoder='utf-8')

        # with gzip.open(self.input()['insert_source'], 'r') as insert_source_file:
        #     cursor.copy_file("COPY {schema}.{table} FROM STDIN PARSER fjsonparser() NO COMMIT;"
        #                      .format(schema=self.schema, table=self.table), insert_source_file, decoder='utf-8')


class LogConcatenator(luigi.Task):
    """
    Concatenate the tracking log files from a single day into a single file.
    """
    run_date = luigi.DateParameter(default=datetime.datetime.utcnow().date())
    remove_implicit = luigi.BooleanParameter(default=True)

    def requires(self):
        CleanForVerticaTask(run_date=self.run_date, remove_implicit=self.remove_implicit)

    def run(self):
        print self.input().fs.listdir('.')


class VerticaEventLoadingWorkflow(VerticaCopyTaskMixin, luigi.WrapperTask):
    """Workflow for encapsulating the Vertica event loading task and passing in parameters."""
    interval = luigi.DateIntervalParameter()
    remove_implicit = luigi.BooleanParameter(default=True)

    def requires(self):
        # Add additional args for VerticaCopyMixin.
        kwargs2 = {
            'schema': self.schema,
            'credentials': self.credentials,
        }
        kwargs2.update(kwargs2)

        for day in luigi.DateIntervalParameter().parse(str(self.interval)):
            task_needed = VerticaEventLoadingTask(run_date=day, remove_implicit=self.remove_implicit, **kwargs2)
            print str(task_needed)
            yield task_needed

        # yield (
        #     VerticaEventLoadingTask(**kwargs2),
        # )

    # def run(self):
    #     self.output().open('w').write("DONE!")
    #
    # def output(self):
    #     return DummyTarget2()

if __name__ == '__main__':
    luigi.run(main_task_cls=VerticaEventLoadingWorkflow)
