"""
Tests integration with ES 7.8.0 version
"""
from unittest import TestCase

import edx.analytics.tasks.tests.acceptance.services.elasticsearch_service as es_service


class TestElasticsearchService(TestCase):
    """
    Tests integration with ES for acceptance tests by ElasticsearchService interface.
    """

    def setUp(self):
        self.config = {"elasticsearch_host": "test_elasticsearch_integration"}
        self.index = "test_index"
        self.alias = "test_alias"
        self.es_service = es_service.ElasticsearchService(self.config, self.alias)
        self.es_client = self.es_service.client
        self.es_client.indices.create(index=self.index)
        self.es_client.indices.update_aliases({"actions": [{"add": {"index": self.index, "alias": self.alias}}]})

    def tearDown(self):
        self.es_client.indices.delete(index=self.index, ignore=[400, 404])

    def test_reset(self):
        """
        Tests reset method of the ElasticsearchService class.
        """
        self.assertTrue(self.es_client.indices.exists_alias(name=self.alias, index=self.index))
        self.es_service.reset()
        self.assertFalse(self.es_client.indices.exists(index=self.index))
