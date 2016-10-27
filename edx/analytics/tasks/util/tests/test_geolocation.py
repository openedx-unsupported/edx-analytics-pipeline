"""
Test object for geolocation tests.
"""


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
