
from luigi.configuration import get_config


def hive_database_name():
    return get_config().get('hive', 'database', 'default')
