"""
Tests and test object for geolocation tests.
"""
from unittest import TestCase

from mock import Mock

from edx.analytics.tasks.util.geolocation import UNKNOWN_CODE, UNKNOWN_COUNTRY, GeolocationMixin


class FakeGeoLocation(object):
    """Fake version of pygeoip.GeoIp() instance for use in testing."""

    ip_address_1 = "123.45.67.89"
    ip_address_2 = "98.76.54.123"
    country_name_1 = "COUNTRY NAME 1"
    country_code_1 = "COUNTRY CODE 1"
    country_name_2 = "COUNTRY NAME 2"
    country_code_2 = "COUNTRY CODE 2"

    def country_name_by_addr(self, ip_address):
        """Generates a country name if ip address is recognized."""
        country_name_map = {
            self.ip_address_1: self.country_name_1,
            self.ip_address_2: self.country_name_2,
        }
        return country_name_map.get(ip_address)

    def country_code_by_addr(self, ip_address):
        """Generates a country code if ip address is recognized."""
        country_code_map = {
            self.ip_address_1: self.country_code_1,
            self.ip_address_2: self.country_code_2,
        }
        return country_code_map.get(ip_address)


class TestGeolocationTaskTestCase(TestCase):
    """Test GeolocationMixin.get_country_xxx() methods."""

    def setUp(self):
        self.task = GeolocationMixin()
        self.task.geoip = FakeGeoLocation()

    def test_country_name(self):
        name = self.task.get_country_name(FakeGeoLocation.ip_address_1)
        self.assertEquals(name, FakeGeoLocation.country_name_1)

    def test_country_code(self):
        code = self.task.get_country_code(FakeGeoLocation.ip_address_1)
        self.assertEquals(code, FakeGeoLocation.country_code_1)

    def test_country_name_exception(self):
        self.task.geoip.country_name_by_addr = Mock(side_effect=Exception)
        name = self.task.get_country_name(FakeGeoLocation.ip_address_1)
        self.assertEquals(name, UNKNOWN_COUNTRY)

    def test_country_code_exception(self):
        self.task.geoip.country_code_by_addr = Mock(side_effect=Exception)
        code = self.task.get_country_code(FakeGeoLocation.ip_address_1)
        self.assertEquals(code, UNKNOWN_CODE)

    def test_missing_country_name(self):
        self.task.geoip.country_name_by_addr = Mock(return_value=None)
        name = self.task.get_country_name(FakeGeoLocation.ip_address_1)
        self.assertEquals(name, UNKNOWN_COUNTRY)

    def test_missing_country_code(self):
        self.task.geoip.country_code_by_addr = Mock(return_value=None)
        code = self.task.get_country_code(FakeGeoLocation.ip_address_1)
        self.assertEquals(code, UNKNOWN_CODE)

    def test_empty_country_name(self):
        self.task.geoip.country_name_by_addr = Mock(return_value="  ")
        name = self.task.get_country_name(FakeGeoLocation.ip_address_1)
        self.assertEquals(name, UNKNOWN_COUNTRY)

    def test_empty_country_code(self):
        self.task.geoip.country_code_by_addr = Mock(return_value="  ")
        code = self.task.get_country_code(FakeGeoLocation.ip_address_1)
        self.assertEquals(code, UNKNOWN_CODE)
