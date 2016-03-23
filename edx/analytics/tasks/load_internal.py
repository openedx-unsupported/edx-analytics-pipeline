
import json
import logging

import luigi
import vertica_python

from edx.analytics.tasks.load_internal_reporting_user_course import LoadInternalReportingUserCourseToWarehouse
from edx.analytics.tasks.url import get_target_from_url, IgnoredTarget
from edx.analytics.tasks.util.hive import WarehouseMixin
from edx.analytics.tasks.vertica_load import VerticaCopyTaskMixin


log = logging.getLogger(__name__)


class LoadInternalReports(WarehouseMixin, VerticaCopyTaskMixin, luigi.Task):

    date = luigi.DateParameter()

    def __init__(self, *args, **kwargs):
        super(LoadInternalReports, self).__init__(*args, **kwargs)
        credentials_target = get_target_from_url(self.credentials)
        with credentials_target.open('r') as credentials_file:
            cred = json.load(credentials_file)

        self.host = cred.get('host')
        self.port = int(cred.get('port', 5433))
        self.user = cred.get('username')
        self.password = cred.get('password')

    @property
    def loading_schema_name(self):
        return self.schema + '_loading'

    def requires(self):
        yield LoadInternalReportingUserCourseToWarehouse(
            date=self.date,
            warehouse_path=self.warehouse_path,
            schema=self.loading_schema_name,
            credentials=self.credentials,
            read_timeout=self.read_timeout,
            overwrite=self.overwrite,
            recreate_schema=True,
        )

    def output(self):
        return IgnoredTarget()

    def run(self):
        connection = vertica_python.connect(user=self.user, password=self.password, host=self.host, port=self.port,
                                            database="", autocommit=False, read_timeout=self.read_timeout)
        connection.cursor().execute(
            'ALTER SCHEMA {loading} RENAME TO {current}'.format(
                loading=self.loading_schema_name,
                current=self.schema,
            )
        )
