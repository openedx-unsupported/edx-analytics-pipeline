"""
Store the basic demographic information about each user into the Analytics MySQL DB.
"""
import datetime
import luigi
from edx.analytics.tasks.database_imports import ImportAuthUserTask, ImportAuthUserProfileTask
from edx.analytics.tasks.util.hive import HivePartition, HiveQueryToMysqlTask


class InsertToMysqlUserProfilesTask(HiveQueryToMysqlTask):
    """
    Select from the 'auth_user' and 'auth_userprofile' tables we already have in Hive and save
    them to MySQL.
    """
    auto_primary_key = None
    import_date = luigi.DateParameter(default=None)
    hive_overwrite = True

    def __init__(self, *args, **kwargs):
        super(InsertToMysqlUserProfilesTask, self).__init__(*args, **kwargs)

        if not self.import_date:
            self.import_date = datetime.datetime.utcnow().date()


    @property
    def table(self):
        return "user_profile"

    @property
    def query(self):
        return """
            SELECT
                au.id,
                au.username,
                au.last_login,
                au.date_joined,
                au.is_staff,
                au.email,
                p.name,
                IF(p.gender != '', p.gender, NULL),
                p.year_of_birth,
                IF(p.level_of_education != '', p.level_of_education, NULL)
            FROM auth_user au
            LEFT OUTER JOIN auth_userprofile p ON au.id = p.user_id
            WHERE au.is_active
        """

    @property
    def columns(self):
        return [
            ('id', 'INT PRIMARY KEY NOT NULL'),
            ('username', 'VARCHAR(30) NOT NULL'),
            ('last_login', 'DATETIME'),
            ('date_joined', 'DATETIME NOT NULL'),
            ('is_staff', 'TINYINT(1) NOT NULL'),
            ('email', 'VARCHAR(75) NOT NULL'),
            ('name', 'VARCHAR(255)'),
            ('gender', 'VARCHAR(6)'),
            ('year_of_birth', 'INT'),
            ('level_of_education', 'VARCHAR(6)'),
        ]

    @property
    def required_table_tasks(self):
        kwargs_for_db_import = {
            'overwrite': False,
            'import_date': self.import_date,
            # Are other parameters needed?
        }
        yield (
            ImportAuthUserTask(**kwargs_for_db_import),
            ImportAuthUserProfileTask(**kwargs_for_db_import)
        )

    @property
    def indexes(self):
        return [
            ('id', ),
            ('username', ),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.import_date)
