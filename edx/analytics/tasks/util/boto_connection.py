import json
import time

from boto.connection import AWSAuthConnection
from elasticsearch import Connection


class BotoHttpConnection(Connection):
    """
    Uses AWS configured connection to sign requests before they're sent to
    elasticsearch nodes.
    """

    connection = None

    def __init__(self, host='localhost', port=443, aws_access_key_id=None, aws_secret_access_key=None,
                 region=None, **kwargs):
        super(BotoHttpConnection, self).__init__(host=host, port=port, **kwargs)
        connection_params = {'host': host, 'port': port}

        # If not provided, boto will attempt to use default environment variables to fill
        # the access credentials.
        connection_params['aws_access_key_id'] = aws_access_key_id
        connection_params['aws_secret_access_key'] = aws_secret_access_key
        connection_params['region'] = region
        # Remove 'None' values so that we don't overwrite defaults
        connection_params = {key: val for key, val in connection_params.items() if val is not None}
        self.connection = ESConnection(**connection_params)

    # pylint: disable=unused-argument
    def perform_request(self, method, url, params=None, body=None, timeout=None, ignore=()):
        """
        Called when making requests elasticsearch.  Requests are signed and
        http status, headers, and response is returned.

        Note: the "timeout" kwarg is ignored in this case.  Boto manages the timeout
        and the default is 70 seconds.
        See: https://github.com/boto/boto/blob/develop/boto/connection.py#L533
        """
        if not isinstance(body, basestring):
            body = json.dumps(body)
        start = time.time()
        response = self.connection.make_request(method, url, params=params, data=body)
        duration = time.time() - start
        raw_data = response.read()

        # raise errors based on http status codes and let the client handle them
        if not (200 <= response.status < 300) and response.status not in ignore:
            self.log_request_fail(method, url, body, duration, response.status)
            self._raise_error(response.status, raw_data)

        self.log_request_success(method, url, url, body, response.status, raw_data, duration)

        return response.status, dict(response.getheaders()), raw_data


class ESConnection(AWSAuthConnection):
    """
    Use to sign requests for an AWS hosted elasticsearch cluster.
    """

    def __init__(self, *args, **kwargs):
        region = kwargs.pop('region', None)
        kwargs.setdefault('is_secure', True)
        super(ESConnection, self).__init__(*args, **kwargs)
        self.auth_region_name = region
        self.auth_service_name = 'es'

    def _required_auth_capability(self):
        """
        Supplies the capabilities of the auth handler and signs the responses to
        AWS using HMAC-4.
        """
        return ['hmac-v4']
