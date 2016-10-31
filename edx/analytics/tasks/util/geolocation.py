"""Utility classes for providing geolocation functionality."""

import tempfile

import luigi
import pygeoip

from edx.analytics.tasks.url import ExternalURL

UNKNOWN_COUNTRY = "UNKNOWN"
UNKNOWN_CODE = "UNKNOWN"


class GeolocationDownstreamMixin(object):
    """
    Defines parameters needed for geolocation lookups.

    """
    geolocation_data = luigi.Parameter(
        config_path={'section': 'geolocation', 'name': 'geolocation_data'},
        description='A URL to the location of country-level geolocation data.',
    )


class GeolocationMixin(GeolocationDownstreamMixin):
    """Provides support for initializing a geolocation object."""

    geoip = None

    def requires_local(self):
        """Adds geolocation_data as a local requirement."""
        result = super(GeolocationMixin, self).requires_local()
        # Default is an empty list, but assume that any real data added is done
        # so as a dict.
        if not result:
            result = {}
        result['geolocation_data'] = ExternalURL(self.geolocation_data)
        return result

    def geolocation_data_target(self):
        """Defines target from which geolocation data can be read."""
        return self.input_local()['geolocation_data']

    def init_reducer(self):
        """Initialize the geolocation object for use by a reducer."""
        super(GeolocationMixin, self).init_reducer()
        # Copy the remote version of the geolocation data file to a local file.
        # This is required by the GeoIP call, which assumes that the data file is located
        # on a local file system.
        self.temporary_data_file = tempfile.NamedTemporaryFile(prefix='geolocation_data')
        with self.geolocation_data_target().open() as geolocation_data_input:
            while True:
                transfer_buffer = geolocation_data_input.read(1024)
                if transfer_buffer:
                    self.temporary_data_file.write(transfer_buffer)
                else:
                    break
        self.temporary_data_file.seek(0)

        self.geoip = pygeoip.GeoIP(self.temporary_data_file.name, pygeoip.STANDARD)

    def final_reducer(self):
        """Clean up after the reducer is done."""
        del self.geoip
        self.temporary_data_file.close()

        return tuple()

    def extra_modules(self):
        """Pygeoip is required by all tasks that perform geolocation."""
        modules = super(GeolocationMixin, self).extra_modules()
        if not modules:
            return [pygeoip]
        else:
            return modules.append(pygeoip)
