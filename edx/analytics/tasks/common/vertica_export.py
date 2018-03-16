"""
Supports exporting data from Vertica.
"""
import logging

import luigi

from edx.analytics.tasks.common.sqoop import SqoopImportFromVertica
from edx.analytics.tasks.util.decorators import workflow_entry_point
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.url import ExternalURL, url_path_join

log = logging.getLogger(__name__)


class VerticaSourcedSqoopMixin(OverwriteOutputMixin):
    """A collection of parameters used by the sqoop command."""
    credentials = luigi.Parameter(
        config_path={'section': 'vertica-export', 'name': 'credentials'},
        description='Path to the external access credentials file.',
    )
    database = luigi.Parameter(
        default='warehouse',
        description='The Vertica database that is the source of the sqoop command.'
    )
    schema = luigi.Parameter(
        default=None,
        description='The Vertica schema that is the source of the sqoop command.'
    )
    table = luigi.Parameter(
        default=None,
        description='The Vertica table that is the source of the sqoop command.'
    )
    warehouse_path = luigi.Parameter(
        config_path={'section': 'vertica-export-sqoop', 'name': 'warehouse_path'},
        description='A URL location of the data warehouse.',
    )


class LoadVerticaToS3TableTask(VerticaSourcedSqoopMixin, luigi.Task):
    """
    Sample S3 loader to S3

    A sample loader that reads a table from Vertica and persists the entry to S3.  In order to use
    SqoopImportFromVertica the caller must already know the Vertica schema, table name, and column names. This
    functionality should be added in future development cycles.
    """
    required_tasks = None

    def __init__(self, *args, **kwargs):
        super(LoadVerticaToS3TableTask, self).__init__(*args, **kwargs)

    def requires(self):
        if self.required_tasks is None:
            self.required_tasks = {
                'credentials': ExternalURL(url=self.credentials),
                'insert_source': self.insert_source_task,
            }
        return self.required_tasks

    def complete(self):
        return self.insert_source_task.complete()

    @property
    def insert_source_task(self):
        """The sqoop command that manages the connection to the source datasource."""
        target_url = url_path_join(self.warehouse_path, self.database, self.schema, self.table)

        return SqoopImportFromVertica(
            schema_name=self.schema,
            table_name=self.table,
            credentials=self.credentials,
            database=self.database,
            columns='course_id,course_org_id,course_number,course_run,course_start,course_end,course_name',
            destination=target_url,
            overwrite=self.overwrite,
        )


@workflow_entry_point
class ImportVerticaToS3Workflow(VerticaSourcedSqoopMixin, luigi.WrapperTask):
    """
    A sample workflow to transfer data from Vertica to S3.

    This is a sample workflow used for manual testing and to act as an example of a workflow to copy Vertica data.  In
    the final version the table name should be a list, and should be optional.  Additionally table exclusions should be
    a run time parameter.
    """
    def __init__(self, *args, **kwargs):
        super(ImportVerticaToS3Workflow, self).__init__(*args, **kwargs)

    def requires(self):
        return LoadVerticaToS3TableTask(
            database=self.database,
            schema=self.schema,
            table=self.table,
            credentials=self.credentials,
            overwrite=self.overwrite,
            warehouse_path=self.warehouse_path,
        )
