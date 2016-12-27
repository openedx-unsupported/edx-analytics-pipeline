"""Collect the course catalog from the course catalog API for processing of course metadata like subjects or types."""
import requests
import datetime

import json
import luigi

from edx.analytics.tasks.url import get_target_from_url
from edx.analytics.tasks.url import url_path_join
from edx.analytics.tasks.util.hive import WarehouseMixin, HivePartition
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.vertica_load import VerticaCopyTask, VerticaCopyTaskMixin

# Tell urllib3 to switch the ssl backend to PyOpenSSL.
# See https://urllib3.readthedocs.org/en/latest/security.html#pyopenssl.
import urllib3.contrib.pyopenssl
urllib3.contrib.pyopenssl.inject_into_urllib3()
# Since we block-encode the offending strings anyways, ignore complaints about unicode escapes in '\N' appearances.
# pylint: disable-msg=anomalous-unicode-escape-in-string


class PullCatalogMixin(OverwriteOutputMixin, WarehouseMixin):
    """Define common parameters for the course catalog API pull and downstream tasks."""

    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        description='Default is today, UTC.',
    )
    catalog_path = luigi.Parameter(
        config_path={'section': 'course-catalog', 'name': 'catalog_path'},
        description='Base URL for the drupal catalog API, e.g. https://www.edx.org/api/catalog/v2/courses',
    )


class DailyPullCatalogTask(PullCatalogMixin, luigi.Task):
    """
    A task that reads the course catalog off the API and writes the result json blob to a file.

    Pulls are made daily to keep a full historical record.
    """

    def requires(self):
        pass

    def run(self):
        self.remove_output_on_overwrite()
        response = requests.get(self.catalog_path)
        if response.status_code != requests.codes.ok:  # pylint: disable=no-member
            msg = "Encountered status {} on request to API for {}".format(response.status_code, self.date)
            raise Exception(msg)
        with self.output().open('w') as output_file:
            output_file.write(response.content)

    def output(self):
        """Output is in the form {warehouse_path}/course_catalog_api/catalog/dt={CCYY-MM-DD}/catalog.json"""
        date_string = "dt=" + self.date.strftime('%Y-%m-%d')  # pylint: disable=no-member
        url_with_filename = url_path_join(self.warehouse_path, "course_catalog", "catalog", date_string,
                                          "catalog.json")
        return get_target_from_url(url_with_filename)


class DailyProcessFromCatalogSubjectTask(PullCatalogMixin, luigi.Task):
    """
    A task that reads a local file generated from a daily catalog pull, and writes the course id and subject to a tsv.

    The output file should be readable by Hive.
    """

    def requires(self):
        kwargs = {
            'date': self.date,
            'catalog_path': self.catalog_path,
            'warehouse_path': self.warehouse_path,
            'overwrite': self.overwrite,
        }
        return DailyPullCatalogTask(**kwargs)

    def run(self):
        # Read the catalog and select just the subjects data for output
        self.remove_output_on_overwrite()
        with self.input().open('r') as input_file:
            # Since the course catalog is of fairly manageable size, we can read it all into memory at once.
            # If this needs to change, we should be able to parse the catalog course by course.
            catalog = json.loads(input_file.read()).get('items')
            with self.output().open('w') as output_file:
                if catalog is None:
                    return
                for course in catalog:
                    # The course catalog occasionally is buggy and has malformed courses, so just skip those.
                    if not type(course) == dict:
                        continue
                    course_id = course.get('course_id')
                    if course_id is None:
                        continue
                    # This will be a list of dictionaries with keys 'title', 'uri', and 'language'.
                    # Encode in utf-8 as in general the subjects could be given in other languages.
                    subjects = course.get('subjects')
                    # It's possible no subjects are given for the course, in which case we record the lack of subjects.
                    if subjects is None or len(subjects) == 0:
                        line = [
                            course_id,
                            self.date.strftime('%Y-%m-%d'),  # pylint: disable=no-member,
                            '\N',
                            '\N',
                            '\N'
                        ]
                        output_file.write('\t'.join([v.encode('utf-8') for v in line]))
                        output_file.write('\n')
                    else:
                        for subject in subjects:
                            line = [
                                course_id,
                                self.date.strftime('%Y-%m-%d'),  # pylint: disable=no-member,
                                subject.get('uri', '\N'),
                                subject.get('title', '\N'),
                                subject.get('language', '\N')
                            ]
                            output_file.write('\t'.join([v.encode('utf-8') for v in line]))
                            output_file.write('\n')

    def output(self):
        """
        Output is set up so that it can be read as a Hive table with partitions,

        The form is {warehouse_path}/course_catalog_api/subjects/dt={CCYY-mm-dd}/subjects.tsv.
        """
        date_string = self.date.strftime('%Y-%m-%d')  # pylint: disable=no-member
        partition_path_spec = HivePartition('dt', date_string).path_spec
        url_with_filename = url_path_join(self.warehouse_path, "course_catalog", "subjects",
                                          partition_path_spec, "subjects.tsv")
        return get_target_from_url(url_with_filename)


class DailyLoadSubjectsToVerticaTask(PullCatalogMixin, VerticaCopyTask):
    """Does the bulk loading of the subjects data into Vertica."""

    @property
    def insert_source_task(self):
        # Note: don't pass overwrite down from here.  Use it only for overwriting when copying to Vertica.
        return DailyProcessFromCatalogSubjectTask(date=self.date, catalog_path=self.catalog_path)

    @property
    def table(self):
        return "d_course_subjects"

    @property
    def auto_primary_key(self):
        """Overridden since the database schema specifies a different name for the auto incrementing primary key."""
        return ('row_number', 'AUTO_INCREMENT')

    @property
    def default_columns(self):
        """Overridden since the superclass method includes a time of insertion column we don't want in this table."""
        return None

    @property
    def columns(self):
        return [
            ('course_id', 'VARCHAR(200)'),
            ('date', 'DATE'),
            ('subject_uri', 'VARCHAR(200)'),
            ('subject_title', 'VARCHAR(200)'),
            ('subject_language', 'VARCHAR(200)')
        ]


class CourseCatalogWorkflow(PullCatalogMixin, VerticaCopyTaskMixin, luigi.WrapperTask):
    """Upload the course catalog to the data warehouse."""

    def requires(self):
        # Add additional args for VerticaCopyMixin.
        kwargs2 = {
            'schema': self.schema,
            'credentials': self.credentials,
            'date': self.date,
            'catalog_path': self.catalog_path,
            'overwrite': self.overwrite,
        }
        kwargs2.update(kwargs2)

        yield (
            DailyLoadSubjectsToVerticaTask(**kwargs2),
        )
