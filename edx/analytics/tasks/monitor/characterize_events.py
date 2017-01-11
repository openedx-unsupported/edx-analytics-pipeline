import luigi
import os
import logging
import re

from boto.s3.connection import S3Connection

from edx.analytics.tasks.common.mapreduce import MapReduceJobTask
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join

log = logging.getLogger(__name__)


class ListS3FilesWithDateTask(OverwriteOutputMixin, luigi.Task):

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
                values = [key_url, last_modified]
                metadata = key.metadata

                if metadata:
                    match = re.match('.*?mtime:(?P<mtime>\d{10}).*?ctime:(?P<ctime>\d{10})', metadata.get('s3cmd-attrs', ''))
                    if match:
                        mtime = match.group('mtime')
                        values.append(mtime)
                        ctime = match.group('ctime')
                        values.append(ctime)

                output_file.write('\t'.join(values))
                output_file.write('\n')
#
#
# class CharacterizeEventsTask(OverwriteOutputMixin, WarehouseMixin, EventLogSelectionMixin, MapReduceJobTask):
#
#     def init_local(self):
#         super(CharacterizeEventsTask, self).init_local()
