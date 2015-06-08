"""Collect the course catalog from Drupal for processing of course metadata like subjects or types"""

import datetime
import requests
import json
import luigi

from edx.analytics.tasks.url import get_target_from_url
from edx.analytics.tasks.url import url_path_join
from edx.analytics.tasks.util.hive import WarehouseMixin, HivePartition
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin

class PullFromDrupalMixin(OverwriteOutputMixin, WarehouseMixin):
    """Define common parameters for Drupal course catalog pull and downstream tasks."""

    host = luigi.Parameter(
        default_from_config={'section': 'drupal', 'name': 'host'}
    )

class DailyPullCatalogFromDrupalTask(PullFromDrupalMixin, luigi.task):
    """
    A task that reads the course catalog out of Drupal and writes the result json blob to a file.

    Pulls are made daily to keep a full historical record.
    """

    CATALOG_URL = 'https://www.edx.org/api/catalog/v2/courses'
    CATALOG_FORMAT = 'json'
    run_date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        pass

    def run(self):
        self.remove_output_on_overwrite()
        response = requests.get(self.CATALOG_URL)
        if response.status_code != requests.codes.ok:  # pylint: disable=no-member
            msg = "Encountered status {} on request to Drupal for {}".format(response.status_code, self.run_date)
            raise Exception(msg)

        with self.output().open('w') as output_file:
            output_file.write(response.content)

    def output(self):
        """Output is in the form {warehouse_path}/catalog/{CCYY-mm-dd}/catalog.json"""
        date_string = self.run_date.strftime('%Y%m%d')
        filename = "catalog.{}".format(self.CATALOG_FORMAT)
        url_with_filename = url_path_join(self.warehouse_path, "catalog", date_string, filename, self.CATALOG_FORMAT)

        return get_target_from_url(url_with_filename)

class DailyProcessFromCatalogSubjectTask(PullFromDrupalMixin, luigi.task):
    """
    A task that reads a local file generated from a daily catalog pull, and writes the course id and subject to a csv.

    The output file should be readable by Hive.
    """

    run_date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        kwargs = {
            'run_date': self.run_date,
            'host': self.host,
            'warehouse_path': self.warehouse_path,
            'overwrite': self.overwrite
        }
        return DailyPullCatalogFromDrupalTask(**kwargs)

    def run(self):
        # Read the catalog and select just the subjects data for output
        self.remove_output_on_overwrite()
        with self.input().open('r') as input_file:
            # Since the course catalog is of fairly manageable size, we can read it all into memory at once.
            # If this needs to change, we should be able to parse the catalog course by course.
            catalog = json.loads(input_file.read())['items']
            with self.output().open('w') as output_file:
                for course in catalog:
                    course_id = course['course_id']
                    # This will be a list of dictionaries with keys 'title', 'uri', and 'language'.
                    subjects = course['subjects']
                    for subject in subjects:
                        line = [
                            course_id,
                            self.run_date,
                            subject['uri'],
                            subject['title'],
                            subject['language']
                        ]
                        output_file.write('\t'.join(line))
                        output_file.write('\n')

    def output(self):
        """
        Output is set up so that it can be read as a Hive table with partitions,

        The form is {warehouse_path}/subjects/dt={CCYY-mm-dd}/subjects.tsv.
        """
        date_string = self.run_date.strftime('%Y-%m-%d')  # pylint: disable=no-member
        partition_path_spec = HivePartition('dt', date_string).path_spec
        filename = "subjects.tsv"
        url_with_filename = url_path_join(self.warehouse_path, "subjects", partition_path_spec, filename)
        return get_target_from_url(url_with_filename)