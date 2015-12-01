import socket

from edx.analytics.tasks.util.boto_connection import ESConnection, BotoHttpConnection

from edx.analytics.tasks.tests import unittest
from elasticsearch.exceptions import ElasticsearchException
from mock import patch


class ESConnectionTests(unittest.TestCase):

    def test_constructor_params(self):
        connection = ESConnection('mockservice.cc-zone-1.amazonaws.com',
                                  aws_access_key_id='access_key',
                                  aws_secret_access_key='secret',
                                  region='region_123')
        self.assertEqual(connection.auth_region_name, 'region_123')
        self.assertEqual(connection.aws_access_key_id, 'access_key')
        self.assertEqual(connection.aws_secret_access_key, 'secret')

    def test_signing(self):
        connection = ESConnection('mockservice.cc-zone-1.amazonaws.com',
                                  aws_access_key_id='my_access_key',
                                  aws_secret_access_key='secret',
                                  region='region_123')
        # create a request and sign it
        request = connection.build_base_http_request('GET', '/', None)
        request.authorize(connection)

        # confirm the header contains signing method and key id
        auth_header = request.headers['Authorization']
        self.assertTrue('AWS4-HMAC-SHA256' in auth_header)
        self.assertTrue('my_access_key' in auth_header)

    def test_timeout(self):
        def fake_connection(_address):
            raise socket.timeout('fake error')
        socket.create_connection = fake_connection
        connection = ESConnection('mockservice.cc-zone-1.amazonaws.com',
                                  aws_access_key_id='access_key',
                                  aws_secret_access_key='secret',
                                  region='region_123')
        connection.num_retries = 0
        with self.assertRaises(socket.error):
            connection.make_request('GET', 'https://example.com')


class BotoHttpConnectionTests(unittest.TestCase):

    @patch('edx.analytics.tasks.util.boto_connection.ESConnection.make_request')
    def test_perform_request_success(self, mock_response):
        mock_response.return_value.status = 200
        connection = BotoHttpConnection(aws_access_key_id='access_key', aws_secret_access_key='secret')
        with patch('elasticsearch.connection.base.logger.info') as mock_logger:
            status, _header, _data = connection.perform_request('get', 'http://example.com')
            self.assertEqual(status, 200)
            self.assertGreater(mock_logger.call_count, 0)

    @patch('edx.analytics.tasks.util.boto_connection.ESConnection.make_request')
    def test_perform_request_error(self, mock_response):
        mock_response.return_value.status = 500
        connection = BotoHttpConnection(aws_access_key_id='access_key', aws_secret_access_key='secret')
        with self.assertRaises(ElasticsearchException):
            with patch('elasticsearch.connection.base.logger.debug') as mock_logger:
                connection.perform_request('get', 'http://example.com')
                self.assertGreater(mock_logger.call_count, 0)
