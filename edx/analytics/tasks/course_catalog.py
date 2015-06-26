"""Collect the course catalog from the course catalog API for processing of course metadata like subjects or types"""
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
# pylint: disable-msg=W1402


class PullFromCatalogAPIMixin(OverwriteOutputMixin, WarehouseMixin):
    """Define common parameters for the course catalog API pull and downstream tasks."""

    run_date = luigi.DateParameter(default=datetime.datetime.utcnow().date())
    catalog_url = luigi.Parameter(default_from_config={'section': 'course-catalog', 'name': 'catalog_path'})


class DailyPullCatalogFromCatalogAPITask(PullFromCatalogAPIMixin, luigi.Task):
    """
    A task that reads the course catalog off the API and writes the result json blob to a file.


    Pulls are made daily to keep a full historical record.
    """

    def requires(self):
        pass

    def run(self):
        date_string = "dt=" + self.run_date.strftime('%Y-%m-%d')  # pylint: disable=no-member
        url_with_filename = url_path_join(self.warehouse_path, "course_catalog_api/catalog",
                                          date_string, "catalog.json")
        print "URL_WITH_FILENAME: ", str(url_with_filename)
        self.remove_output_on_overwrite()
        print "CATALOG: ", self.catalog_url
        response = requests.get(self.catalog_url)
        if response.status_code != requests.codes.ok:  # pylint: disable=no-member
            msg = "Encountered status {} on request to API for {}".format(response.status_code, self.run_date)
            raise Exception(msg)
        with self.output().open('w') as output_file:
            output_file.write(response.content)

    def output(self):
        """Output is in the form {warehouse_path}/course_catalog_api/catalog/{CCYY-mm-dd}/catalog.json"""
        date_string = "dt=" + self.run_date.strftime('%Y-%m-%d')  # pylint: disable=no-member
        url_with_filename = url_path_join(self.warehouse_path, "course_catalog_api/catalog",
                                          date_string, "catalog.json")
        return get_target_from_url(url_with_filename)


class DailyProcessFromCatalogSubjectTask(PullFromCatalogAPIMixin, luigi.Task):
    """
    A task that reads a local file generated from a daily catalog pull, and writes the course id and subject to a tsv.


    The output file should be readable by Hive.
    """

    def requires(self):
        kwargs = {
            'run_date': self.run_date,
            'catalog_url': self.catalog_url,
            'warehouse_path': self.warehouse_path,
            'overwrite': self.overwrite
        }
        return DailyPullCatalogFromCatalogAPITask(**kwargs)

    def run(self):
        # Read the catalog and select just the subjects data for output
        self.remove_output_on_overwrite()
        with self.input().open('r') as input_file:
            # Since the course catalog is of fairly manageable size, we can read it all into memory at once.
            # If this needs to change, we should be able to parse the catalog course by course.
            catalog = json.loads(input_file.read()).get('items', None)
            with self.output().open('w') as output_file:
                if catalog is None:
                    return
                for course in catalog:
                    # The course catalog occasionally is buggy and has malformed courses, so just skip those.
                    if not type(course) == dict:
                        continue
                    course_id = course.get('course_id', None)
                    if course_id is None:
                        continue
                    # This will be a list of dictionaries with keys 'title', 'uri', and 'language'.
                    # Encode in utf-8 as in general the subjects could be given in other languages.
                    subjects = course.get('subjects', None)
                    # It's possible no subjects are given for the course, in which case we record the lack of subjects.
                    if subjects is None or len(subjects) == 0:
                        line = [
                            course_id,
                            self.run_date.strftime('%Y-%m-%d'),  # pylint: disable=no-member,
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
                                self.run_date.strftime('%Y-%m-%d'),  # pylint: disable=no-member,
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
        date_string = self.run_date.strftime('%Y-%m-%d')  # pylint: disable=no-member
        partition_path_spec = HivePartition('dt', date_string).path_spec
        filename = "subjects.tsv"
        url_with_filename = url_path_join(self.warehouse_path, "course_catalog_api/subjects",
                                          partition_path_spec, filename)
        return get_target_from_url(url_with_filename)


class DailyLoadSubjectsToVerticaTask(PullFromCatalogAPIMixin, VerticaCopyTask):
    """Does the bulk loading of the subjects data into Vertica."""

    @property
    def insert_source_task(self):
        return(DailyProcessFromCatalogSubjectTask(run_date=self.run_date, catalog_url=self.catalog_url))

    @property
    def table(self):
        return "d_course_subjects"

    @property
    def auto_primary_key(self):
        """Overridden since the database schema specifies a different name for the auto incrementing primary key."""
        return None

    @property
    def default_columns(self):
        """Overridden since the superclass method includes a time of insertion column we don't want in this table."""
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


class CourseCatalogWorkflow(PullFromCatalogAPIMixin, VerticaCopyTaskMixin, luigi.WrapperTask):
    """Upload the course catalog to the data warehouse."""

    def requires(self):
        # Add additional args for VerticaCopyMixin.
        kwargs2 = {
            'schema': self.schema,
            'credentials': self.credentials,
            'run_date': self.run_date,
            'catalog_url': self.catalog_url
        }
        kwargs2.update(kwargs2)

        yield (
            DailyLoadSubjectsToVerticaTask(**kwargs2),
        )


class CourseCatalogWorkflowFixed(PullFromCatalogAPIMixin, VerticaCopyTaskMixin, luigi.WrapperTask):
    """Upload the course catalog to the data warehouse."""

    def requires(self):
        # Add additional args for VerticaCopyMixin.
        kwargs2 = {
            'schema': self.schema,
            'credentials': self.credentials,
            'run_date': self.run_date,
            'catalog_url': self.catalog_url
        }
        kwargs2.update(kwargs2)

        yield (
            DailyLoadSubjectsToVerticaTask(**kwargs2),
        )
