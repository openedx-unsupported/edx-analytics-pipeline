import tempfile
import pygeoip


class Geolocation(object):

    def __init__(self, geolocation_data_target):
        self._temporary_data_file = tempfile.NamedTemporaryFile(prefix='geolocation_data')
        with geolocation_data_target().open() as geolocation_data_input:
            while True:
                transfer_buffer = geolocation_data_input.read(1024)
                if transfer_buffer:
                    self._temporary_data_file.write(transfer_buffer)
                else:
                    break
        self._temporary_data_file.seek(0)

        self._geoip = pygeoip.GeoIP(self.temporary_data_file.name, pygeoip.STANDARD)

    @property
    def geoip(self):
        return self._geoip

    @property
    def temp_data_file(self):
        return self._temporary_data_file
