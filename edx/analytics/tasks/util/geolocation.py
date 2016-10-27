"""Utility classes for providing geolocation functionality."""

import pygeoip
import tempfile

import luigi

UNKNOWN_COUNTRY = "UNKNOWN"
UNKNOWN_CODE = "UNKNOWN"


class GeolocationMixin(object):
    """
    Defines parameters needed for geolocation lookups.

    """
    geolocation_data = luigi.Parameter(
        config_path={'section': 'geolocation', 'name': 'geolocation_data'},
        description='A URL to the location of country-level geolocation data.',
    )


class GeolocationTask(object):
    """Provides support for initializing a geolocation object."""

    geoip = None

    def geolocation_data_target(self):
        """Defines target from which geolocation data can be read."""
        raise NotImplementedError

    def init_reducer(self):
        super(GeolocationTask, self).init_reducer()
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
