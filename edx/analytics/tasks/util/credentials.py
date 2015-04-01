
import json
import urlparse

from edx.analytics.tasks.url import ExternalURL


class CredentialsUrl(ExternalURL):

    def __init__(self, *args, **kwargs):
        super(CredentialsUrl, self).__init__(*args, **kwargs)
        self.parsed_url = urlparse.urlparse(self.url)

    def complete(self):
        if self.is_external_file:
            return super(CredentialsUrl, self).complete()
        else:
            return True

    @property
    def is_external_file(self):
        return self.parsed_url.scheme != 'mysql'

    @property
    def credentials(self):
        if not hasattr(self, '_credentials'):
            if self.is_external_file:
                with self.open('r') as credentials_file:
                    self._credentials = json.load(credentials_file)
            else:
                split_netloc = self.parsed_url.netloc.split('@')
                username = split_netloc[0]
                password = None
                if ':' in username:
                    username, password = username.split(':')
                host = split_netloc[1]
                port = None
                if ':' in host:
                    host, port = host.split(':')
                    port = int(port)

                self._credentials = {
                    'username': username,
                    'host': host
                }
                if port:
                    self._credentials['port'] = port
                if password:
                    self._credentials['password'] = password

        return self._credentials

    @property
    def username(self):
        return self.credentials['username']

    @property
    def host(self):
        return self.credentials['host']

    @property
    def port(self):
        return self.credentials.get('port', 3306)

    @property
    def password(self):
        return self.credentials['password']
