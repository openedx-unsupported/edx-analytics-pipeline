"""
Collection of abstract tasks for extracting tables from BigQuery.

BigQuery data can ONLY dump into Google Cloud Storage (GCS), not S3. Therefore, this module also
provides GCSToS3Task.
"""
import errno
import json
import logging
import subprocess
import time

import luigi

from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.s3_util import canonicalize_s3_url
from edx.analytics.tasks.util.url import ExternalURL, GCSMarkerTarget, get_target_from_url, url_path_join

log = logging.getLogger(__name__)

try:
    from google.cloud import bigquery
    from google.oauth2 import service_account
    bigquery_available = True  # pylint: disable=invalid-name
except ImportError:
    log.warn('Unable to import Bigquery libraries')
    # On hadoop slave nodes we don't have bigquery libraries installed,
    # so just fail noisily if we attempt to use these libraries there.
    bigquery_available = False  # pylint: disable=invalid-name


class BigQueryExtractDownstreamMixin(OverwriteOutputMixin):
    """
    Common luigi parameters for all BigQuery extract tasks and wrapper tasks.
    """
    credentials = luigi.Parameter()
    project = luigi.Parameter()
    dataset = luigi.Parameter()


class BigQueryExtractTask(BigQueryExtractDownstreamMixin, luigi.Task):
    """
    Abstract Task to extract one BigQuery table into Google Cloud Storage (GCS).

    This task outputs a GCSMarkerTarget which represents the destination prefix for the dumped table
    which gets broken up (automatically) into several 1GB JSON gzip'd files.
    """

    output_target = None
    required_tasks = None

    # For example, output_url might be:
    #
    #   gs://bucket/table
    #
    # in which case the following output files could be generated:
    #
    #   gs://bucket/table/_SUCCESS
    #   gs://bucket/table/_metadata
    #   gs://bucket/table/0000001.json.gz
    #   gs://bucket/table/0000002.json.gz
    #   gs://bucket/table/0000003.json.gz
    output_url = luigi.Parameter(
        description='The GCS URL prefix of the output files, NOT including the filename pattern or trailing slash.'
    )

    def requires(self):  # pylint: disable=missing-docstring
        if self.required_tasks is None:
            self.required_tasks = {
                'credentials': ExternalURL(url=self.credentials),
            }
        return self.required_tasks

    def output(self):  # pylint: disable=missing-docstring
        if self.output_target is None:
            self.output_target = GCSMarkerTarget(
                self.output_url,
                credentials_file=self.input()['credentials'],
            )
        return self.output_target

    @property
    def table(self):
        """
        Name (str) of the table in BQ to extract.

        If this is a sharded table, do not include the date suffix, e.g. return
        "ga_sessions_" instead of "ga_sessions_20190408".
        """
        raise NotImplementedError

    @property
    def output_compression_type(self):
        """
        The type of compression to use for the output files.

        Return None for no compression.
        """
        return 'GZIP'

    @property
    def output_format(self):
        """
        The type of compression to use for the output files.
        """
        return bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON

    def _get_table_identifier(self):
        """
        Construct a table identifier for the table to be extracted.

        Override this method to include partition or shard suffixes.  By default, this simply
        returns the passed-in table without any suffixes.

        Returns:
            str: The table to extract.
        """
        return self.table

    def _get_destination_url_pattern(self):
        """
        Generate the BQ-compatible GCS destination URL pattern including a single asterisk to
        specify where the table dumps fan-out.

        For example, if self.output_url is:

          gs://bucket/table

        then this function might return:

          gs://bucket/table/*.json.gz

        Returns:
            str: GCS URL pattern in BQ extract_table format.
        """
        if self.output_compression_type == 'GZIP':
            filename_extension = 'json.gz'
        else:
            filename_extension = 'json'

        # This will cause the leaf files themselves to just be named like "0000001.json.gz".
        filename_pattern = '*.{}'.format(filename_extension)

        return url_path_join(
            self.output_url,
            filename_pattern,
        )

    def _get_job_id(self):
        """
        Returns:
            str: Unique identifier to assign to the BQ job.
        """
        return 'extract_{table}_{timestamp}'.format(table=self._get_table_identifier(), timestamp=int(time.time()))

    def _make_bq_client(self):
        """
        Construct a BigQuery client using the credentials file input target.
        """
        with self.input()['credentials'].open('r') as credentials_file:
            json_creds = json.load(credentials_file)
        self.project_id = json_creds['project_id']
        credentials = service_account.Credentials.from_service_account_info(json_creds)
        return bigquery.Client(credentials=credentials, project=self.project_id)

    def run(self):  # pylint: disable=missing-docstring
        self.check_bigquery_availability()

        client = self._make_bq_client()
        dataset_ref = client.dataset(self.dataset, project=self.project)
        table_name = self._get_table_identifier()
        table_ref = dataset_ref.table(table_name)

        job_config = bigquery.job.ExtractJobConfig()
        job_config.destination_format = self.output_format
        if self.output_compression_type:
            job_config.compression = self.output_compression_type
        job = client.extract_table(
            table_ref,
            self._get_destination_url_pattern(),
            job_config=job_config,
            location='US',
            job_id=self._get_job_id(),
        )
        log.info("Starting BigQuery Extract job.")
        job.result()  # Waits for job to complete.

        self.output().touch_marker()

    def check_bigquery_availability(self):
        """
        Call to ensure fast failure if this machine doesn't have the Bigquery libraries available.
        """
        if not bigquery_available:
            raise ImportError('Bigquery library not available.')


class BigQueryExtractShardedTask(BigQueryExtractTask):  # pylint: disable=abstract-method
    """
    Abstract class for extracting BigQuery tables to GCS, specifically for tables sharded by date.

    Be sure to include a trailing underscore when overriding the self.table property, if the table
    name needs one.
    """
    date = luigi.DateParameter()

    def _get_date_suffix(self):
        """
        Generate a date sharding suffix in BQ's typical format (YYYYMMDD).

        Returns:
            str: BQ date suffix.
        """
        iso_date_string = self.date.isoformat()
        return iso_date_string.replace('-', '')

    def _get_table_identifier(self):
        """
        Override super's method to add the shard date suffix to the table identifier.
        """
        return '{}{}'.format(self.table, self._get_date_suffix())


class CopyGCSToS3Task(OverwriteOutputMixin, luigi.Task):
    """
    Abstract Task to copy data from Google Cloud Storage (GCS) to Amazon S3.

    Input to this task is a GCSMarkerTarget, and output is an S3MarkerTarget.
    For example, if input and output URLs were as follows:

      input = gs://bucket/foo
      output = s3+https://bucket/bar

    and the contents of the input were:

      gs://bucket/foo/_SUCCESS
      gs://bucket/foo/_metadata
      gs://bucket/foo/0000001.json.gz
      gs://bucket/foo/0000002.json.gz
      gs://bucket/foo/0000003.json.gz

    then, the following output files would be generated:

      s3://bucket/bar/_SUCCESS
      s3://bucket/bar/_metadata
      s3://bucket/bar/0000001.json.gz
      s3://bucket/bar/0000002.json.gz
      s3://bucket/bar/0000003.json.gz
    """

    output_url = luigi.Parameter(
        description='The S3 URL prefix of the output files, NOT including the filename pattern or extension.'
    )

    output_target = None
    required_tasks = None

    def requires(self):  # pylint: disable=missing-docstring
        if self.required_tasks is None:
            self.required_tasks = {
                'source': self.insert_source_task,
            }
        return self.required_tasks

    def output(self):  # pylint: disable=missing-docstring
        if self.output_target is None:
            self.output_target = get_target_from_url(self.output_url, marker=True)
        return self.output_target

    @property
    def insert_source_task(self):
        """
        Override this to define the source task that outputs a GCSMarkerTarget to copy from.
        """
        raise NotImplementedError

    def _copy_data_from_gcs_to_s3(self, source_path, destination_path):
        """
        Recursively copy a "directory" from GCS to S3.
        """
        # Exclude any luigi marker files which should not be copied.  The pattern is a Python
        # regular expression.
        exclusion_pattern = r'.*_SUCCESS$|.*_metadata$'
        command = [
            'gsutil', '-m', 'rsync', '-x', exclusion_pattern,
            source_path,
            canonicalize_s3_url(destination_path),
        ]
        log.info(
            'Invoking the following command to copy from GCS to S3: %s',
            ' '.join(command),
        )
        try:
            return_code = subprocess.call(command)
        except OSError as err:
            if err.errno == errno.ENOENT:
                log.error('Check that gsutil is installed.')
            raise
        if return_code != 0:
            raise RuntimeError('Error {code} while syncing {source} to {destination}'.format(
                code=return_code,
                source=source_path,
                destination=destination_path,
            ))

    def run(self):  # pylint: disable=missing-docstring
        log.debug("Starting GCS Extract job.")
        self._copy_data_from_gcs_to_s3(
            self.input()['source'].path,
            self.output_url,
        )
        self.output().touch_marker()
