"""A simple client for authenticated access to Open edX REST APIs."""

from datetime import datetime, timedelta
import logging

from luigi import configuration
import requests
from requests.auth import AuthBase

from edx.analytics.tasks.util.retry import retry


log = logging.getLogger(__name__)

DEFAULT_RETRY_STATUS_CODES = (
    requests.codes.request_timeout,         # HTTP Status Code 408
    requests.codes.too_many_requests,       # HTTP Status Code 429
    requests.codes.service_unavailable,     # HTTP Status Code 503
    requests.codes.gateway_timeout          # HTTP Status Code 504
)
DEFAULT_TIMEOUT_SECONDS = 7200


class EdxApiClient(object):
    """
    Simplifies authentication and pagination logic when communicating with Open edX REST APIs.

    Note that we deliberately chose *not* to use the edx-rest-api-client here since slumber was mostly just getting in
    the way. There is a small amount of code duplication between this client and that one.

    Sample configuration that can be included in the configuration file:

    [edx-rest-api]
    auth_url = https://example.com/oauth2/v1/access_token
    client_id = 1234567890
    client_secret = this-should-be-a-secret

    Arguments:
        auth_url (str): The full URL of the authentication endpoint. It should look like
            https://example.com/oauth2/v1/access_token. If this is not provided it is read from the "auth_url" field
            of the "edx-rest-api" section of the configuration file.
        client_id (str): This is the OAuth2 client_id for the application. If it is not provided it is read from the
            "client_id" field of the "edx-rest-api" section of the configuration file.
        client_secret (str): This is the OAuth2 client_secret for the application. If it is not provided it is read from
            the "client_secret" field of the "edx-rest-api" section of the configuration file.
    """

    def __init__(self, auth_url=None, client_id=None, client_secret=None):
        self._expires_at = None
        self._session = requests.Session()
        self._session.hooks = {
            'response': log_response_hook
        }

        config = configuration.get_config()
        self.client_id = client_id or config.get('edx-rest-api', 'client_id')
        self.client_secret = client_secret or config.get('edx-rest-api', 'client_secret')
        self.auth_url = auth_url or config.get('edx-rest-api', 'auth_url')

    @property
    def authenticated_session(self):
        """A session that has a valid access token associated with it and can make authenticated requests."""
        self.ensure_oauth_access_token()
        return self._session

    def ensure_oauth_access_token(self):
        """Retrieves OAuth 2.0 access token using the client credentials grant and stores it in the request session."""
        now = datetime.utcnow()
        if self._expires_at is None or now >= self._expires_at:
            log.info('Token is expired or missing, requesting a new one.')
            response = requests.post(
                self.auth_url,
                data={
                    'grant_type': 'client_credentials',
                    'client_id': self.client_id,
                    'client_secret': self.client_secret,
                    'token_type': 'jwt',
                },
                hooks={
                    'response': log_response_hook
                }
            )
            data = response.json()
            self._session.auth = SuppliedJwtAuth(data['access_token'])
            self._expires_at = now + timedelta(seconds=data['expires_in'])
            log.info('Acquired a token that expires at %s', self._expires_at.isoformat())

    def paginated_get(self, url, params=None, timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
                      retry_on=DEFAULT_RETRY_STATUS_CODES, pagination_key='next'):
        """
        Fetches a paginated resource.

        Arguments:
            url (str): The URL of the resource.
            params (dict): This is a dictionary of key-value pairs that are URL encoded and injected into the query
                string when making the request.
            timeout_seconds (float): When requesting a page, keep retrying unless this much time has elapsed. This timer
                resets after every successful fetch of a page. Raise an error if it takes longer than this amount of
                time to fetch an individual page.
            retry_on (iterable): This is a set of HTTP status codes that should trigger a retry of the request if they
                are received from the server in the response. If one is received the system implements an exponential
                back-off and repeatedly requests the page until either the timeout expires, a fatal exception occurs, or
                an OK response is received.
            pagination_key (str): The response is parsed as JSON and this is the name of the field that should be in the
                root of the JSON document that provides the URL for the next page of data. This field should be omitted,
                set to null, or an empty string in the final response to prevent attempts to gather more pages of data.

        Yields: A single requests.Response object for each page of data received from the server.
        """

        def should_retry(error):
            """Retry the request if the response status code is in the set of status codes that are retryable."""
            error_response = getattr(error, 'response', None)
            if error_response is None:
                return False

            return error_response.status_code in retry_on

        @retry(should_retry=should_retry, timeout=timeout_seconds)
        def get_resource_with_retry(next_url=None):
            """
            Attempt to get the resource, using an exponetial back-off to retry recoverable, failed requests.

            Arguments:
                next_url (str): The url of the next page to fetch. If this is `None` this is the first page, so we
                    should use the provided `url` and `params` passed into the outer function to fetch this first page.
                    The next url is provided in the first response with all of the appropriate parameters needed to
                    fetch the next page of data. We don't want to accidentally override existing parameters so we omit
                    the `params` kwarg from the call.
            """
            if next_url is None:
                raw_response = self.authenticated_session.get(url, params=params)
            else:
                raw_response = self.authenticated_session.get(next_url)

            raw_response.raise_for_status()
            return raw_response

        response = get_resource_with_retry()
        yield response
        if not pagination_key:
            return

        next_url_from_response = response.json().get(pagination_key)
        while next_url_from_response:
            response = get_resource_with_retry(next_url_from_response)
            yield response
            next_url_from_response = response.json().get(pagination_key)


class SuppliedJwtAuth(AuthBase):
    """Attaches a supplied JWT to the given Request object."""

    def __init__(self, token):
        """Instantiate the auth class."""
        self.token = token

    def __call__(self, r):
        """Update the request headers."""
        r.headers['Authorization'] = 'JWT {jwt}'.format(jwt=self.token)
        return r


def log_response_hook(response, *args, **kwargs):  # pylint: disable=unused-argument
    """Log summary information about every request made."""
    log.info('[%s] [%d] [%f] %s',
             response.request.method, response.status_code, response.elapsed.total_seconds(), response.url)
