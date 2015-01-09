"""Group all events into a single file per day."""

import datetime
from hashlib import md5
from operator import attrgetter
import os
import sys
from tempfile import mkdtemp

import boto
import cjson
import luigi
import luigi.configuration
import yaml

from edx.analytics.tasks.mapreduce import MapReduceJobTask
from edx.analytics.tasks.s3_util import get_s3_bucket_key_names
from edx.analytics.tasks.url import UncheckedExternalURL, url_path_join, get_target_from_url
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.hive import WarehouseMixin


class CanonicalizationTask(WarehouseMixin, MapReduceJobTask):
    """
    Group all events into a single file per day.

    Standardize their format so that downstream tasks can make assumptions about their structure.
    """

    source = luigi.Parameter(
        is_list=True,
        default_from_config={'section': 'event-logs', 'name': 'source'}
    )

    VERSION = "1"
    FILES_PER_BATCH = 1000

    def complete(self):
        return False

    def output(self):
        return self.output_target

    def requires(self):
        if hasattr(self, 'requirements'):
            return self.requirements

        self.output_root = url_path_join(self.warehouse_path, 'events')
        self.metadata_path = url_path_join(self.output_root, '_manifest.yml')
        self.current_time = datetime.datetime.utcnow().isoformat()

        tmp_dir = mkdtemp()
        self.output_target = get_target_from_url(tmp_dir)

        metadata_target = get_target_from_url(self.metadata_path)
        self.path_to_batch = {}
        try:
            with metadata_target.open('r') as metadata_file:
                self.manifest = yaml.load(metadata_file)
                max_batch_id = 0
                for batch in self.manifest:
                    self.previously_processed_files[batch['path']] = batch
                    if batch['batch_id'] > max_batch_id:
                        max_batch_id = batch['batch_id']
                self.min_batch_id = max_batch_id + 1
        except Exception:  # pylint: disable=broad-except
            self.min_batch_id = 0

        self.requirements = self._get_requirements()
        for idx, requirement in enumerate(sorted(self.requirements, key=attrgetter('url'))):
            batch_id = self.min_batch_id + (idx / self.FILES_PER_BATCH)
            path = requirement.url
            batch = self.path_to_batch.get(path)
            if not batch:
                self.path_to_batch[path] = {
                    'batch_id': batch_id,
                    'files': [path]
                }
            else:
                batch['files'].append(path)

        return self.requirements

    def requires_hadoop(self):
        return self.requires()

    def _get_requirements(self):
        url_gens = []
        for source in self.source:
            if source.startswith('s3'):
                url_gens.append(self._get_s3_urls(source))
            elif source.startswith('hdfs'):
                url_gens.append(self._get_hdfs_urls(source))
            else:
                url_gens.append(self._get_local_urls(source))

        return [UncheckedExternalURL(url) for url_gen in url_gens for url in url_gen if self.should_include_url(url)]

    def _get_s3_urls(self, source):
        s3_conn = boto.connect_s3()
        bucket_name, root = get_s3_bucket_key_names(source)
        bucket = s3_conn.get_bucket(bucket_name)
        for key_metadata in bucket.list(root):
            if key_metadata.size > 0:
                key_path = key_metadata.key[len(root):].lstrip('/')
                yield url_path_join(source, key_path)

    def _get_hdfs_urls(self, source):
        for source in luigi.hdfs.listdir(source):
            yield source

    def _get_local_urls(self, source):
        for directory_path, _subdir_paths, filenames in os.walk(source):
            for filename in filenames:
                yield os.path.join(directory_path, filename)

    def should_include_url(self, url):
        return url not in self.path_to_batch

    def mapper(self, line):
        event = eventlog.parse_json_event(line)
        if not event:
            return

        if 'event_type' not in event:
            return

        standardized_time = eventlog.get_event_time_string(event)
        if not standardized_time:
            return

        event['time'] = standardized_time
        date_string = standardized_time.split("T")[0]
        event.setdefault('date', date_string)

        metadata = event.setdefault('metadata', {})
        metadata.setdefault('version', self.VERSION)
        metadata['last_modified'] = self.current_time
        if 'id' not in metadata:
            metadata['id'] = self.compute_hash(line)

        map_input_file = os.environ['map_input_file']
        metadata['original_file'] = map_input_file
        batch_id = self.get_batch_id(map_input_file)

        event.setdefault('context', {})
        content = event.get('event')
        if content and isinstance(content, basestring):
            try:
                event['event'] = cjson.decode(content)
            except Exception:
                self.incr_counter('Canonicalization', 'Malformed JSON content string', 1)

        canonical_event = cjson.encode(event)

        yield (date_string, batch_id), canonical_event

    def compute_hash(self, line):
        hasher = md5()
        hasher.update(line)
        return hasher.hexdigest()

    def get_batch_id(self, file_path):
        return self.path_to_batch[file_path]['batch_id']

    def reducer(self, key, values):
        date_string, batch_id = key
        output_path = url_path_join(
            self.output_root,
            'dt=' + date_string,
            'batch_{0}.log'.format(batch_id)
        )
        output_file_target = get_target_from_url(output_path)
        with output_file_target.open('w') as output_file:
            bytes_written = 0
            for idx, value in enumerate(values):
                output_file.write(value.strip())
                output_file.write('\n')
                bytes_written += len(value) + 1

                if idx % 1000 == 0:
                    # WARNING: This line ensures that Hadoop knows that our process is not sitting in an infinite loop.
                    # Do not remove it.
                    self.incr_counter('Canonicalization', 'Raw Bytes Written', bytes_written)
                    bytes_written = 0

            if bytes_written > 0:
                self.incr_counter('Canonicalization', 'Raw Bytes Written', bytes_written)

        # Luigi requires the reducer to return an iterable
        return iter(tuple())

    def run(self):
        try:
            super(CanonicalizationTask, self).run()
        finally:
            if self.output_target.exists():
                self.output_target.remove()
