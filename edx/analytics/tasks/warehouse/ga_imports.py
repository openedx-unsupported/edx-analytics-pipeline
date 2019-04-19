"""
Tasks for loading GA360 data from BigQuery -> Google Cloud Storage -> S3 -> Snowflake.

For a given date, the following tasks will be sequenced:

1. BigQueryExtractGATask
2. CopyGAFromGCSToS3Task
3. SnowflakeLoadGATask

You need only schedule SnowflakeLoadGATask, and luigi will schedule dependent
tasks as necessary.

SnowflakeLoadGAIntervalTask can be used to load multiple days of GA data,
useful when combined with Jenkins to load the "last 3 days" for automatic
recovery from downtime.
"""
import logging

import luigi

from edx.analytics.tasks.common.bigquery_extract import BigQueryExtractShardedTask, CopyGCSToS3Task
from edx.analytics.tasks.common.snowflake_load import SnowflakeLoadJSONTask
from edx.analytics.tasks.util.url import url_path_join

log = logging.getLogger(__name__)


class BigQueryExtractGATask(BigQueryExtractShardedTask):
    """
    Task that extracts GA data for the specified date from BQ into GCS.
    """

    credentials = luigi.Parameter(
        config_path={'section': 'ga-imports', 'name': 'bq_credentials'}
    )
    project = luigi.Parameter(
        config_path={'section': 'ga-imports', 'name': 'bq_project'}
    )
    dataset = luigi.Parameter(
        config_path={'section': 'ga-imports', 'name': 'bq_dataset'}
    )

    @property
    def table(self):
        """
        Name of the table in BQ to extract.
        """
        return 'ga_sessions_'  # This is a sharded table, so the table name needs a trailing underscore.


class CopyGAFromGCSToS3Task(CopyGCSToS3Task):
    """
    Task that copies GA data for the specified date from GCS into S3.
    """

    date = luigi.DateParameter()
    gcs_intermediate_url_prefix = luigi.Parameter(
        config_path={'section': 'ga-imports', 'name': 'gcs_intermediate_url_prefix'}
    )

    @property
    def insert_source_task(self):
        gcs_intermediate_url = url_path_join(
            self.gcs_intermediate_url_prefix,
            self.date.isoformat(),
        )
        return BigQueryExtractGATask(
            date=self.date,
            output_url=gcs_intermediate_url,
        )


class SnowflakeLoadGATask(SnowflakeLoadJSONTask):
    """
    Load GA data for the specified date from s3 into Snowflake.
    """

    s3_intermediate_url_prefix = luigi.Parameter(
        config_path={'section': 'ga-imports', 'name': 's3_intermediate_url_prefix'}
    )
    credentials = luigi.Parameter(
        config_path={'section': 'ga-imports', 'name': 'sf_credentials'}
    )
    role = luigi.Parameter(
        config_path={'section': 'ga-imports', 'name': 'sf_role'}
    )
    database = luigi.Parameter(
        config_path={'section': 'ga-imports', 'name': 'sf_database'}
    )
    schema = luigi.Parameter(
        config_path={'section': 'ga-imports', 'name': 'sf_schema'}
    )
    warehouse = luigi.Parameter(
        config_path={'section': 'ga-imports', 'name': 'sf_warehouse'}
    )

    @property
    def insert_source_task(self):
        s3_intermediate_url = url_path_join(
            self.s3_intermediate_url_prefix,
            self.date.isoformat(),
        )
        return CopyGAFromGCSToS3Task(
            date=self.date,
            output_url=s3_intermediate_url,
        )

    @property
    def table(self):
        """
        Provides the name of the database table.
        """
        return 'ga_sessions'

    @property
    def file_format_name(self):
        """
        Given name for a Snowflake file format definition for the s3 location
        of the GA data.
        """
        return 'ga_sessions_json_format'


class SnowflakeLoadGAIntervalTask(luigi.WrapperTask):
    """
    Fan-out the SnowflakeLoadGATask.  Load GA data for the specified date range.
    """

    interval = luigi.DateIntervalParameter(
        description='The range of received dates for which to load GA records.',
    )

    def requires(self):
        for date in reversed([d for d in self.interval]):  # pylint: disable=not-an-iterable
            yield SnowflakeLoadGATask(date=date)
