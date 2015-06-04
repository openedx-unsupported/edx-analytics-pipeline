"""Collect the course catalog from Drupal for further downstream processing of things like course subjects or types"""

import datetime
import requests
import luigi

from edx.analytics.tasks.url import get_target_from_url
from edx.analytics.tasks.url import url_path_join
from edx.analytics.tasks.util.hive import WarehouseMixin
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin

#TODO: general template code: https://github.com/edx/edx-analytics-pipeline/blob/brian/cybersource/edx/analytics/tasks/reports/payments.py

class DailyPullCatalogFromDrupalTask(OverwriteOutputMixin, WarehouseMixin, luigi.task):
    """
    A task that reads the course catalog out of Drupal and writes the result json blob to a file.

    Pulls are made daily to keep a full historical record
    """

    catalog_url = "https://www.edx.org/api/catalog/v2/courses"
    catalog_key = "items"
    run_date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        pass

    def run(self):
        self.remove_output_on_overwrite()
        #TODO: bring up need to refactor these API calls maybe?
        response = requests.get(self.catalog_url)
        if response.status_code != requests.codes.ok: #pylint: disable=no-member #TODO: why is this here?
            msg = "Encountered status {} on request to Drupal for {}".format(response.status_code, self.run_date)
            raise Exception(msg)

        with self.output().open('w') as output_file:
            output_file.write(response.content)

    def output(self):
        """Output is in the form {warehouse_path}/catalog/{CCYY-mm-dd}/catalog.json"""
        date_string = self.run_date.strftime('%Y%m%d')
        filename = "catalog"
        url_with_filename = url_path_join(self.warehouse_path, "catalog", date_string, filename)

        return get_target_from_url(url_with_filename)

