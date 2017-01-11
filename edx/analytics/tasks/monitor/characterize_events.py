import luigi
import os
import logging

from boto.s3.connection import S3Connection

from edx.analytics.tasks.common.mapreduce import MapReduceJobTask
from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join

log = logging.getLogger(__name__)


class ListS3FilesWithDateTask(luigi.Task):

    output_root = luigi.Parameter(
        description='URL of location to write output.',
    )

    bucket = luigi.Parameter()

    key_prefix = luigi.Parameter(default='logs/tracking/')

    def requires(self):
        pass

    def output(self):
        return get_target_from_url(url_path_join(self.output_root, "s3_file_list.tsv"))

    def run(self):
        self.remove_output_on_overwrite()
        conn = S3Connection()
        bucket = conn.get_bucket(self.bucket)
        with self.output().open('w') as output_file:
            for key in bucket.list(prefix=self.key_prefix):
                key_url = url_path_join('s3:////', bucket.name, key.name)
                last_modified = key.last_modified
                output_file.write('\t'.join((key_url, last_modified)))
                output_file.write('\n')
#
#
# class CharacterizeEventsTask(OverwriteOutputMixin, WarehouseMixin, EventLogSelectionMixin, MapReduceJobTask):
#
#     def init_local(self):
#         super(CharacterizeEventsTask, self).init_local()
