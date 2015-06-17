"""Collect the course catalog from Drupal for processing of course metadata like subjects or types"""
from requests import get as get_request
from requests import codes as request_codes
import datetime

import json
import luigi

from edx.analytics.tasks.url import get_target_from_url
from edx.analytics.tasks.url import url_path_join
from edx.analytics.tasks.util.hive import WarehouseMixin, HivePartition
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.vertica_load import VerticaCopyTask, VerticaCopyTaskMixin

import urllib3.contrib.pyopenssl
urllib3.contrib.pyopenssl.inject_into_urllib3()


class PullFromDrupalMixin(OverwriteOutputMixin, WarehouseMixin):
    """Define common parameters for Drupal course catalog pull and downstream tasks."""

    run_date = luigi.DateParameter(default=datetime.date.today())


class DailyPullCatalogFromDrupalTask(PullFromDrupalMixin, luigi.Task):
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
        response = get_request(self.CATALOG_URL)
        if response.status_code != request_codes.ok:  # pylint: disable=no-member
            msg = "Encountered status {} on request to Drupal for {}".format(response.status_code, self.run_date)
            raise Exception(msg)
        with self.output().open('w') as output_file:
            output_file.write(response.content)

    def output(self):
        """Output is in the form {warehouse_path}/catalog/{CCYY-mm-dd}/catalog.json"""
        date_string = self.run_date.strftime('%Y%m%d')
        filename = "catalog.{}".format(self.CATALOG_FORMAT)
        url_with_filename = url_path_join(self.warehouse_path, "catalog", date_string, filename)
        return get_target_from_url(url_with_filename)


class DailyProcessFromCatalogSubjectTask(PullFromDrupalMixin, luigi.Task):
    """
    A task that reads a local file generated from a daily catalog pull, and writes the course id and subject to a csv.


    The output file should be readable by Hive.
    """

    run_date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        kwargs = {
            'run_date': self.run_date,
            # 'host': self.host,
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
                    # Encode in utf-8 as in general the subjects could be given in other languages.
                    subjects = course.get('subjects', None)
                    # It's possible no subjects are given for the course, in which case we record the lack of subjects.
                    if subjects is None or len(subjects) == 0:
                        line = [
                            course_id.encode('utf-8'),
                            self.run_date,
                            '\N'.encode('utf-8'),  # pylint: disable-msg=W1402
                            '\N'.encode('utf-8'),  # pylint: disable-msg=W1402
                            '\N'.encode('utf-8')  # pylint: disable-msg=W1402
                        ]
                        output_file.write('\t'.join(line))
                        output_file.write('\n')
                    else:
                        for subject in subjects:
                            line = [
                                course_id.encode('utf-8'),
                                str(self.run_date),
                                subject.get('uri', '\N').encode('utf-8'),  # pylint: disable-msg=W1402
                                subject.get('title', '\N').encode('utf-8'),  # pylint: disable-msg=W1402
                                subject.get('language', '\N').encode('utf-8')  # pylint: disable-msg=W1402
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


class DailyLoadSubjectsToVerticaTask(VerticaCopyTask):
    """Does the bulk loading of the subjects data into Vertica."""

    run_date = luigi.DateParameter(default=datetime.date.today())

    @property
    def insert_source_task(self):
        return(DailyProcessFromCatalogSubjectTask(run_date=self.run_date))

    @property
    def table(self):
        return "experimental.d_course_subjects"

    @property
    def auto_primary_key(self):
        return None

    @property
    def default_columns(self):
        """List of tuples defining name and definition of automatically-filled columns."""
        return None

    @property
    def columns(self):
        return [
            ('row_number', 'AUTO_INCREMENT PRIMARY KEY'),
            ('course_id', 'VARCHAR(200)'),
            ('date', 'DATE'),
            ('subject_uri', 'VARCHAR(200)'),
            ('subject_title', 'VARCHAR(200)'),
            ('subject_language', 'VARCHAR(200)')
        ]


class CourseCatalogWorkflow(PullFromDrupalMixin, VerticaCopyTaskMixin, luigi.WrapperTask):
    """Upload the course catalog to the data warehouse."""

    def requires(self):
        # Add additional args for VerticaCopyMixin.
        kwargs2 = {
            'database': self.database,
            'credentials': self.credentials,
            'run_date': self.run_date
        }
        kwargs2.update(kwargs2)

        yield (
            DailyLoadSubjectsToVerticaTask(**kwargs2),
        )
