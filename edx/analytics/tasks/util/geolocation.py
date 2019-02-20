"""Utility classes for providing geolocation functionality."""

import logging
import os.path
import tempfile

import luigi

from edx.analytics.tasks.util.url import ExternalURL

try:
    import pygeoip
except ImportError:
    # The module will be imported on slave nodes even though they don't actually have the package installed.
    # The module is hopefully exported for tasks that actually use the module.
    pygeoip = NotImplemented

try:
    import geoip2.database
    import maxminddb
except ImportError:
    # The module will be imported on slave nodes even though they don't actually have the package installed.
    # The module is hopefully exported for tasks that actually use the module.
    geoip2 = NotImplemented


UNKNOWN_COUNTRY = "UNKNOWN"
UNKNOWN_CODE = "UNKNOWN"

log = logging.getLogger(__name__)


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
    geoip2_reader = None #  = geoip2.database.Reader(NEW_GEOIP_PATH)
    geoip_v6 = None # pygeoip.GeoIP(GEOIPV6_PATH)
    geoip_v4 = None # pygeoip.GeoIP(GEOIP_PATH)

    def requires_local(self):
        """Adds geolocation_data as a local requirement."""
        result = super(GeolocationMixin, self).requires_local()
        # Default is an empty list, but assume that any real data added is done
        # so as a dict.
        if not result:
            result = {}
        result['geolocation_data'] = ExternalURL(self.geolocation_data)
        # Hardcode other files relative to the path to the main one.
        dirpath, filename = os.path.split(self.geolocation_data)
        result['geolocation_v4_data'] = ExternalURL(os.path.join(dirpath, "GeoIP_v4_test.dat"))
        result['geolocation_v6_data'] = ExternalURL(os.path.join(dirpath, "GeoIP_v6_test.dat"))
        result['geolocation_new_data'] = ExternalURL(os.path.join(dirpath, "GeoIP2_test.dat"))

        return result

    def geolocation_data_target(self, key):
        """Defines target from which geolocation data can be read."""
        return self.input_local()[key]

    def init_reducer(self):
        """Initialize the geolocation object for use by a reducer."""
        super(GeolocationMixin, self).init_reducer()
        # Copy the remote version of the geolocation data file to a local file.
        # This is required by the GeoIP call, which assumes that the data file is located
        # on a local file system.
        self.temporary_data_file = tempfile.NamedTemporaryFile(prefix='geolocation_data')
        with self.geolocation_data_target('geolocation_data').open() as geolocation_data_input:
            while True:
                transfer_buffer = geolocation_data_input.read(1024)
                if transfer_buffer:
                    self.temporary_data_file.write(transfer_buffer)
                else:
                    break
        self.temporary_data_file.seek(0)
        self.geoip = pygeoip.GeoIP(self.temporary_data_file.name, pygeoip.STANDARD)

        self.temporary_v4_data_file = tempfile.NamedTemporaryFile(prefix='geolocation_v4_data')
        with self.geolocation_data_target('geolocation_v4_data').open() as geolocation_data_input:
            while True:
                transfer_buffer = geolocation_data_input.read(1024)
                if transfer_buffer:
                    self.temporary_v4_data_file.write(transfer_buffer)
                else:
                    break
        self.temporary_v4_data_file.seek(0)
        self.geoip_v4 = pygeoip.GeoIP(self.temporary_v4_data_file.name)

        self.temporary_v6_data_file = tempfile.NamedTemporaryFile(prefix='geolocation_v6_data')
        with self.geolocation_data_target('geolocation_v6_data').open() as geolocation_data_input:
            while True:
                transfer_buffer = geolocation_data_input.read(1024)
                if transfer_buffer:
                    self.temporary_v6_data_file.write(transfer_buffer)
                else:
                    break
        self.temporary_v6_data_file.seek(0)
        self.geoip_v6 = pygeoip.GeoIP(self.temporary_v6_data_file.name)

        self.temporary_new_data_file = tempfile.NamedTemporaryFile(prefix='geolocation_new_data')
        with self.geolocation_data_target('geolocation_new_data').open() as geolocation_data_input:
            while True:
                transfer_buffer = geolocation_data_input.read(1024)
                if transfer_buffer:
                    self.temporary_new_data_file.write(transfer_buffer)
                else:
                    break
        self.temporary_new_data_file.seek(0)
        self.geoip2_reader = geoip2.database.Reader(self.temporary_new_data_file.name)

    def final_reducer(self):
        """Clean up after the reducer is done."""
        del self.geoip
        del self.geoip2_reader
        del self.geoip_v6
        del self.geoip_v4
        self.temporary_data_file.close()
        self.temporary_v4_data_file.close()
        self.temporary_v6_data_file.close()
        self.temporary_new_data_file.close()

        return tuple()

    def extra_modules(self):
        """Pygeoip is required by all tasks that perform geolocation."""
        modules = super(GeolocationMixin, self).extra_modules()
        # need more libs for geoip2:
        # requests, chardet, certifi, urllib3, idna, should already be included.
        # Add maxminddb and geoip2.
        if not modules:
            return [pygeoip, maxminddb, geoip2]
        else:
            modules.append(pygeoip)
            modules.append(maxminddb)
            modules.append(geoip2)
            return modules

    def get_country_name(self, ip_address, debug_message=None):
        """
        Find country name for a given IP address.

        The ip address might not provide a country name, so return
        UNKNOWN_COUNTRY in those cases.

        """
        try:
            name = self.geoip.country_name_by_addr(ip_address)
        except Exception:   # pylint:  disable=broad-except
            if debug_message:
                log.exception("Encountered exception getting country name for ip_address '%s': %s.",
                              ip_address, debug_message)
            name = UNKNOWN_COUNTRY

        if name is None or len(name.strip()) <= 0:
            if debug_message:
                log.error("No country name found for ip_address '%s': %s.", ip_address, debug_message)
            name = UNKNOWN_COUNTRY

        return name

    def get_country_code(self, ip_address, debug_message=None):
        """
        Find country code for a given IP address.

        The ip address might not provide a country code, so return
        UNKNOWN_CODE in those cases.

        """
        try:
            code = self.geoip.country_code_by_addr(ip_address)
        except Exception:   # pylint:  disable=broad-except
            if debug_message:
                log.exception("Encountered exception getting country code for ip_address '%s': %s.",
                              ip_address, debug_message)
            code = UNKNOWN_CODE

        if code is None or len(code.strip()) <= 0:
            if debug_message:
                log.error("No country code found for ip_address '%s': %s.", ip_address, debug_message)
            code = UNKNOWN_CODE

        return code

    def get_country_code_for_geoip(self, geoip, ip_address, debug_message=None):
        """
        Find country codes for a given IP address.

        The ip address might not provide a country code, so return
        UNKNOWN_CODE in those cases.

        """
        try:
            code = geoip.country_code_by_addr(ip_address)
        except Exception:   # pylint:  disable=broad-except
            if debug_message:
                log.exception("Encountered exception getting country code for ip_address '%s': %s.",
                              ip_address, debug_message)
            code = UNKNOWN_CODE

        if code is None or len(code.strip()) <= 0:
            if debug_message:
                log.error("No country code found for ip_address '%s': %s.", ip_address, debug_message)
            code = UNKNOWN_CODE

        return code

    def get_country_code_for_geoip2(self, geoip2_reader, ip_address, debug_message=None):
        """
        Find country codes for a given IP address.

        The ip address might not provide a country code, so return
        UNKNOWN_CODE in those cases.

        """
        try:
            response = geoip2_reader.country(ip_address)
            code = response.country.iso_code
        except Exception:   # pylint:  disable=broad-except
            if debug_message:
                log.exception("Encountered exception getting country code for ip_address '%s': %s.",
                              ip_address, debug_message)
            code = UNKNOWN_CODE

        if code is None or len(code.strip()) <= 0:
            if debug_message:
                log.error("No country code found for ip_address '%s': %s.", ip_address, debug_message)
            code = UNKNOWN_CODE

        return code

    def get_country_codes(self, ip_address, debug_message=None):
        """
        Find country codes for a given IP address.

        The ip address might not provide a country code, so return
        UNKNOWN_CODE in those cases.

        """
        code = get_country_code_for_geoip(self.geoip, ip_address, debug_message)
        code_v4 = get_country_code_for_geoip(self.geoip_v4, ip_address, debug_message)
        code_v6 = get_country_code_for_geoip(self.geoip_v6, ip_address, debug_message)
        code_new = get_country_code_for_geoip2(self.geoip2_reader, ip_address, debug_message)
        return (code, code_v4, code_v6, code_new)
