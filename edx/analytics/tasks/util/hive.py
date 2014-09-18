
import luigi
from luigi.configuration import get_config


def hive_database_name():
    return get_config().get('hive', 'database', 'default')


class WarehouseMixin(object):

    warehouse_path = luigi.Parameter(
        default_from_config={'section': 'hive', 'name': 'warehouse_path'}
    )
