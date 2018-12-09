"""Test the API client"""

import json
from datetime import datetime, timedelta
from unittest import TestCase

import httpretty
import requests
from ddt import data, ddt, unpack
from mock import patch

from edx.analytics.tasks.util.edx_api_client import EdxApiClient
from edx.analytics.tasks.util.tests.config import with_luigi_config

FAKE_AUTH_URL = 'http://example.com/oauth2/access_token'
FAKE_CLIENT_ID = 'aclientid'
FAKE_CLIENT_SECRET = 'notasecret'
FAKE_ACCESS_TOKEN = 'notasecrettoken'
FAKE_RESOURCE_URL = 'http://example.com/resource/'


@ddt
@httpretty.activate
class EdxApiClientTestCase(TestCase):
    """Test the client"""

    def setUp(self):
        self.current_time = datetime(2016, 1, 1, 0, 0, 0, 0)
        self.time_offset = 0

        class MockDateTime(datetime):
            """
            A fake datetime that allows the test to control the return value from the utcnow() method.

            This allows us to emulate the passage of time, by simply changing `self.time_offset`.
            """

            @classmethod
            def utcnow(cls):
                """Return the time specified by the time offset"""
                return self.current_time + timedelta(seconds=self.time_offset)

        datetime_patcher = patch('edx.analytics.tasks.util.edx_api_client.datetime', MockDateTime)
        datetime_patcher.start()
        self.addCleanup(datetime_patcher.stop)

        # don't actually sleep!
        sleep_patcher = patch('edx.analytics.tasks.util.retry.time.sleep', return_value=None)
        self.mock_sleep = sleep_patcher.start()
        self.addCleanup(sleep_patcher.stop)

        self.client = EdxApiClient(auth_url=FAKE_AUTH_URL, client_id=FAKE_CLIENT_ID, client_secret=FAKE_CLIENT_SECRET)

    def test_fetch_access_token(self):
        self.prepare_for_token_request()

        self.client.ensure_oauth_access_token()

        self.assertEqual(self.client.authenticated_session.auth.token, FAKE_ACCESS_TOKEN)

        self.assert_token_request_body(FAKE_CLIENT_ID, FAKE_CLIENT_SECRET)

    def assert_token_request_body(self, client_id, client_secret, token_type='jwt'):
        """Look at the most recent request and make sure it contained the provided credentials."""
        token_request = httpretty.last_request()
        self.assertEqual(token_request.method, 'POST')
        self.assertEqual(
            token_request.parsed_body,
            {
                'token_type': [token_type],
                'client_secret': [client_secret],
                'client_id': [client_id],
                'grant_type': ['client_credentials']
            }
        )

    def assert_dogwood_token_request_body(self, client_id, client_secret, oauth_username, oauth_password):
        """Look at the most recent request and make sure it contained the provided credentials."""
        token_request = httpretty.last_request()
        self.assertEqual(token_request.method, 'POST')
        self.assertEqual(
            token_request.parsed_body,
            {
                'grant_type': ['password'],
                'token_type': ['jwt'],
                'client_secret': [client_secret],
                'client_id': [client_id],
                'username': [oauth_username],
                'password': [oauth_password],
            }
        )

    @staticmethod
    def prepare_for_token_request(expires_in=60, access_token=FAKE_ACCESS_TOKEN):
        """Setup the request mock so that it returns the specified response."""
        response = {
            'expires_in': expires_in,
            'access_token': access_token
        }
        httpretty.register_uri(httpretty.POST, FAKE_AUTH_URL, body=json.dumps(response))

    @with_luigi_config(
        ('edx-rest-api', 'client_id', 'cidfromcfg'),
        ('edx-rest-api', 'client_secret', 'secfromcfg'),
        ('edx-rest-api', 'auth_url', FAKE_AUTH_URL)
    )
    def test_secrets_from_config(self):
        self.prepare_for_token_request()

        client = EdxApiClient()
        client.ensure_oauth_access_token()

        self.assert_token_request_body('cidfromcfg', 'secfromcfg')
        self.assertEqual(client.authenticated_session.auth.token, FAKE_ACCESS_TOKEN)

    @with_luigi_config(
        ('edx-rest-api', 'client_id', 'cidfromcfg'),
        ('edx-rest-api', 'client_secret', 'secfromcfg'),
        ('edx-rest-api', 'auth_url', FAKE_AUTH_URL)
    )
    def test_token_type(self):
        self.prepare_for_token_request()

        client = EdxApiClient(token_type='bearer')
        client.ensure_oauth_access_token()

        self.assert_token_request_body('cidfromcfg', 'secfromcfg', token_type='bearer')
        self.assertEqual(client.authenticated_session.auth.token, FAKE_ACCESS_TOKEN)

    @with_luigi_config(
        ('edx-rest-api', 'client_id', 'cidfromcfg'),
        ('edx-rest-api', 'client_secret', 'secfromcfg'),
        ('edx-rest-api', 'oauth_username', 'unamefromcfg'),
        ('edx-rest-api', 'oauth_password', 'pwdfromcfg'),
        ('edx-rest-api', 'auth_url', FAKE_AUTH_URL)
    )
    def test_oauth_dogwood_from_config(self):
        self.prepare_for_token_request()

        client = EdxApiClient()
        client.ensure_oauth_access_token()

        self.assert_dogwood_token_request_body('cidfromcfg', 'secfromcfg', 'unamefromcfg', 'pwdfromcfg')
        self.assertEqual(client.authenticated_session.auth.token, FAKE_ACCESS_TOKEN)

    def test_expiration(self):
        expires_in = 10
        self.prepare_for_token_request(expires_in=expires_in, access_token='token1')
        self.client.ensure_oauth_access_token()

        self.assertEqual(self.client.authenticated_session.auth.token, 'token1')

        self.time_offset = 11
        self.prepare_for_token_request(expires_in=10, access_token='token2')
        self.client.ensure_oauth_access_token()

        self.assertEqual(self.client.authenticated_session.auth.token, 'token2')

    def test_get_single_page(self):
        self.prepare_for_token_request()
        response_body = {
            'results': [
                {'a': 1},
                {'a': 2}
            ]
        }
        httpretty.register_uri('GET', FAKE_RESOURCE_URL, body=json.dumps(response_body))

        response = self.client.get(FAKE_RESOURCE_URL)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), response_body)

        responses = list(self.client.paginated_get(FAKE_RESOURCE_URL))
        self.assertEqual(len(responses), 1)
        self.assertEqual(response.status_code, responses[0].status_code)
        self.assertEqual(response.json(), responses[0].json())

    def test_get_internal_server_error(self):
        self.prepare_for_token_request()
        httpretty.register_uri('GET', FAKE_RESOURCE_URL, body='Server Error', status=500)

        with self.assertRaises(requests.HTTPError):
            list(self.client.paginated_get(FAKE_RESOURCE_URL))

    def test_non_fatal_error(self):
        self.prepare_for_token_request()
        httpretty.register_uri('GET', FAKE_RESOURCE_URL,
                               responses=[
                                   httpretty.Response(body='Too many requests!', status=429),
                                   httpretty.Response(body='Unavailable', status=503),
                                   httpretty.Response(body='{}', status=200)
                               ])

        responses = list(self.client.paginated_get(FAKE_RESOURCE_URL))
        self.assertEqual(len(responses), 1)
        response = responses[0]
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {})

        num_auth_token_requests = 1
        num_failed_requests = 2
        num_successful_requests = 1
        total_expected_requests = num_auth_token_requests + num_failed_requests + num_successful_requests
        self.assertEqual(len(httpretty.httpretty.latest_requests), total_expected_requests)

    @data(
        ([
            {
                'next': FAKE_RESOURCE_URL + '?offset=2&limit=2&foo=bar',
                'results': [{'a': 1}, {'a': 2}]
            },
            {
                'next': FAKE_RESOURCE_URL + '?offset=4&limit=2&foo=bar',
                'results': [{'a': 3}, {'a': 4}]
            },
            {
                'next': None,
                'results': [{'a': 5}, {'a': 6}]
            }
        ], 'next'),
        ([
            {
                'pagination': {'next': FAKE_RESOURCE_URL + '?offset=2&limit=2&foo=bar'},
                'results': [{'a': 1}, {'a': 2}]
            },
            {
                'pagination': {'next': FAKE_RESOURCE_URL + '?offset=4&limit=2&foo=bar'},
                'results': [{'a': 3}, {'a': 4}]
            },
            {
                'results': [{'a': 5}, {'a': 6}]
            }
        ], lambda r: r.get('pagination', {}).get('next'))
    )
    @unpack
    def test_paginated_get(self, response_bodies, pagination_key):
        self.prepare_for_token_request()
        httpretty.register_uri('GET', FAKE_RESOURCE_URL,
                               responses=[
                                   httpretty.Response(body=json.dumps(response_bodies[0])),
                                   httpretty.Response(body='Unavailable', status=503),
                                   httpretty.Response(body=json.dumps(response_bodies[1])),
                                   httpretty.Response(body='Too many requests!', status=429),
                                   httpretty.Response(body=json.dumps(response_bodies[2])),
                               ])

        responses = list(self.client.paginated_get(FAKE_RESOURCE_URL, params={'limit': 2, 'foo': 'bar'},
                                                   pagination_key=pagination_key))
        self.assertEqual([response.json() for response in responses], response_bodies)

        self.assertEqual(len(httpretty.httpretty.latest_requests), 6)
        self.assertEquals(
            httpretty.httpretty.latest_requests[1].querystring, {'limit': ['2'], 'foo': ['bar']}
        )
        self.assertEquals(
            httpretty.httpretty.latest_requests[3].querystring, {'limit': ['2'], 'foo': ['bar'], 'offset': ['2']}
        )
        self.assertEquals(
            httpretty.httpretty.latest_requests[5].querystring, {'limit': ['2'], 'foo': ['bar'], 'offset': ['4']}
        )
