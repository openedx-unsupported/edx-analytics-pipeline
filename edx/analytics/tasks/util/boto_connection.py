import json
import time

from boto.connection import AWSAuthConnection
from elasticsearch import Connection


class BotoHttpConnection(Connection):

    def __init__(self, host='localhost', port=443, **kwargs):
        super(BotoHttpConnection, self).__init__(host=host, port=port, **kwargs)
        self.connection = ESConnection(host=host, port=port)

    def perform_request(self, method, url, params=None, body=None, timeout=None, ignore=()):
        if not isinstance(body, basestring):
            body = json.dumps(body)

        start = time.time()
        response = self.connection.make_request(method, url, params=params, data=body)
        duration = time.time() - start
        raw_data = response.read()

        # raise errors based on http status codes, let the client handle those if needed
        if not (200 <= response.status < 300) and response.status not in ignore:
            self.log_request_fail(method, url, body, duration, response.status)
            self._raise_error(response.status, raw_data)

        self.log_request_success(method, url, url, body, response.status, raw_data, duration)

        return response.status, dict(response.getheaders()), raw_data


class ESConnection(AWSAuthConnection):

    def __init__(self, *args, **kwargs):
        region = kwargs.pop('region', 'us-east-1')
        kwargs.setdefault('is_secure', True)
        super(ESConnection, self).__init__(*args, **kwargs)
        self.auth_region_name = region
        self.auth_service_name = "es"

    def _required_auth_capability(self):
        return ['hmac-v4']
