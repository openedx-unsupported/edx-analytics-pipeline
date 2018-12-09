"""Test the AWS-specific elasticsearch connection."""
import socket
from unittest import TestCase

from boto.exception import BotoServerError
from elasticsearch.exceptions import ElasticsearchException, TransportError
from mock import patch

from edx.analytics.tasks.util.aws_elasticsearch_connection import AwsElasticsearchConnection, AwsHttpConnection


class AwsElasticsearchConnectionTests(TestCase):
    """Test the generic connection."""

    def test_constructor_params(self):
        connection = AwsElasticsearchConnection('mockservice.cc-zone-1.amazonaws.com',
                                                aws_access_key_id='access_key',
                                                aws_secret_access_key='secret',
                                                region='region_123')
        self.assertEqual(connection.auth_region_name, 'region_123')
        self.assertEqual(connection.aws_access_key_id, 'access_key')
        self.assertEqual(connection.aws_secret_access_key, 'secret')

    def test_signing(self):
        connection = AwsElasticsearchConnection('mockservice.cc-zone-1.amazonaws.com',
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
        def fake_connection(_address, _timeout):
            """Fail immediately with a socket timeout."""
            raise socket.timeout('fake error')
        socket.create_connection = fake_connection
        connection = AwsElasticsearchConnection('mockservice.cc-zone-1.amazonaws.com',
                                                aws_access_key_id='access_key',
                                                aws_secret_access_key='secret',
                                                region='region_123')
        connection.num_retries = 0
        with self.assertRaises(socket.error):
            connection.make_request('GET', 'https://example.com')


class AwsHttpConnectionTests(TestCase):
    """Mock out the request making part and test the connection."""

    @patch('edx.analytics.tasks.util.aws_elasticsearch_connection.AwsElasticsearchConnection.make_request')
    def test_perform_request_success(self, mock_response):
        mock_response.return_value.status = 200
        connection = AwsHttpConnection(aws_access_key_id='access_key', aws_secret_access_key='secret')
        with patch('elasticsearch.connection.base.logger.info') as mock_logger:
            status, _header, _data = connection.perform_request('get', 'http://example.com')
            self.assertEqual(status, 200)
            self.assertGreater(mock_logger.call_count, 0)

    @patch('edx.analytics.tasks.util.aws_elasticsearch_connection.AwsElasticsearchConnection.make_request')
    def test_perform_request_error(self, mock_response):
        mock_response.return_value.status = 500
        connection = AwsHttpConnection(aws_access_key_id='access_key', aws_secret_access_key='secret')
        with self.assertRaises(ElasticsearchException):
            with patch('elasticsearch.connection.base.logger.debug') as mock_logger:
                connection.perform_request('get', 'http://example.com')
                self.assertGreater(mock_logger.call_count, 0)

    @patch('edx.analytics.tasks.util.aws_elasticsearch_connection.AwsElasticsearchConnection.make_request')
    def test_boto_service_unavailable(self, mock_make_request):
        connection = AwsHttpConnection(aws_access_key_id='access_key', aws_secret_access_key='secret')
        mock_make_request.side_effect = BotoServerError(503, 'Service Unavailable')
        try:
            connection.perform_request('get', 'http://example.com')
        except TransportError as transport_error:
            self.assertEqual(transport_error.status_code, 503)
        else:
            self.fail('Expected a transport error to be raised.')
