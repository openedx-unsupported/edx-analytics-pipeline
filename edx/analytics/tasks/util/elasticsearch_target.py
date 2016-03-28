import luigi
import luigi.configuration
import luigi.hdfs
import hashlib
import datetime
import logging


try:
    import elasticsearch
except ImportError:
    elasticsearch = None


log = logging.getLogger(__name__)


class ElasticsearchTarget(luigi.hdfs.HdfsTarget):

    def __init__(self, client, index, doc_type, update_id):
        super(ElasticsearchTarget, self).__init__(is_tmp=True)

        self.marker_index = luigi.configuration.get_config().get(
            'elasticsearch',
            'marker-index',
            'index_updates'
        )
        self.marker_doc_type = luigi.configuration.get_config().get(
            'elasticsearch',
            'marker-doc-type',
            'marker'
        )
        self.index = index
        self.doc_type = doc_type
        self.update_id = update_id

        self.es = client

    def marker_index_document_id(self):
        params = '%s:%s:%s' % (self.index, self.doc_type, self.update_id)
        return hashlib.sha1(params.encode('utf-8')).hexdigest()

    def touch(self):
        self.create_marker_index()
        self.es.index(
            index=self.marker_index,
            doc_type=self.marker_doc_type,
            id=self.marker_index_document_id(),
            body={
                'update_id': self.update_id,
                'target_index': self.index,
                'target_doc_type': self.doc_type,
                'date': datetime.datetime.utcnow()
            }
        )
        self.es.indices.flush(index=self.marker_index)

    def exists(self):
        try:
            self.es.get(index=self.marker_index, doc_type=self.marker_doc_type, id=self.marker_index_document_id())
            return True
        except elasticsearch.NotFoundError:
            log.debug('Marker document not found.')
        except elasticsearch.ElasticsearchException as err:
            log.warn(err)
        return False

    def create_marker_index(self):
        if not self.es.indices.exists(index=self.marker_index):
            self.es.indices.create(index=self.marker_index)
