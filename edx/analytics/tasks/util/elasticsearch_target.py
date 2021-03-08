"""Represents an index in an elasticsearch cluster."""

import datetime
import hashlib
import logging

import luigi
import luigi.configuration
from luigi.contrib.hdfs.target import HdfsTarget

try:
    import elasticsearch
except ImportError:
    elasticsearch = None


log = logging.getLogger(__name__)


class ElasticsearchTarget(HdfsTarget):
    """
    Represents an index in an elasticsearch cluster.

    This derives from HdfsTarget since it is used as the output target for a Hadoop job.

    Arguments:
            client (elasticsearch.Elasticsearch): An elasticsearch client.
            index (str): Name of the index that is populated.
            update_id (str): A unique identifier that is used to determine if an indexing task should be re-run.
    """

    def __init__(self, client, index, update_id):
        super(ElasticsearchTarget, self).__init__(is_tmp=True)

        self.marker_index = luigi.configuration.get_config().get(
            'elasticsearch',
            'marker-index',
            'index_updates'
        )
        self.index = index
        self.update_id = update_id

        self.elasticsearch_client = client

    def marker_index_document_id(self):
        """A concise string that represents a unique ID for this instance of this task."""
        params = '{}:{}'.format(self.index, self.update_id)
        return hashlib.sha1(params.encode('utf-8')).hexdigest()

    def touch(self):
        """Mark the task as having completed successfully."""
        # Ensure the marker index exists
        if not self.elasticsearch_client.indices.exists(index=self.marker_index):
            self.elasticsearch_client.indices.create(index=self.marker_index)

        self.elasticsearch_client.index(
            index=self.marker_index,
            id=self.marker_index_document_id(),
            body={
                'update_id': self.update_id,
                'target_index': self.index,
                'date': datetime.datetime.utcnow()
            }
        )
        self.elasticsearch_client.indices.flush(index=self.marker_index)

    def exists(self):
        """Check if this task has already run successfully in the past."""
        try:
            self.elasticsearch_client.get(
                index=self.marker_index,
                id=self.marker_index_document_id()
            )
            return True
        except elasticsearch.NotFoundError:
            log.debug('Marker document not found.')
        except elasticsearch.ElasticsearchException as err:
            log.warn(err)
        return False
