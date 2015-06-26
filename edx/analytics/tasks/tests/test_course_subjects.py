"""
Test the course subjects task which processes from the course catalog.

Testing strategy:
    Empty catalog (expect empty output)
    Catalog with one course listed which has no subjects listed (expect output with null values?)
    Catalog with one course listed which has one subject
    Catalog with one course listed which has multiple subjects
    Catalog with multiple courses listed
    Catalog that is an actual day's catalog, checking that the size of the produced .tsv is good.
"""

import tempfile
import os
import shutil
import datetime
import pandas
from edx.analytics.tasks.course_catalog import DailyProcessFromCatalogSubjectTask

from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.target import FakeTarget

from mock import MagicMock


class TestCourseSubjects(unittest.TestCase):
    """Tests for the task parsing the catalog into rows of course ids and subjects"""

    def setUp(self):
        self.temp_rootdir = tempfile.mkdtemp()
        self.input_dir = os.path.join(self.temp_rootdir, "input")
        os.mkdir(self.input_dir)
        self.input_file = os.path.join(self.input_dir, "catalog", "20150625", "catalog.json")
        self.addCleanup(self.cleanup, self.temp_rootdir)

    def cleanup(self, dirname):
        """Remove the temp directory only if it exists."""
        if os.path.exists(dirname):
            shutil.rmtree(dirname)

    def run_task(self, source):
        """Helper utility for running task under test"""

        self.input_file = "catalog_test.json"
        with open(self.input_file, 'w') as fle:
            # fle.write(source)
            fle.write(source.encode('utf-8'))

        fake_warehouse_path = self.input_dir

        task = DailyProcessFromCatalogSubjectTask(warehouse_path=fake_warehouse_path,
                                                  run_date=datetime.date(2015, 06, 25), catalog_url='')

        output_target = FakeTarget()
        task.output = MagicMock(return_value=output_target)

        class DummyInput(object):
            """A dummy input object to imitate the input to a luigi task."""
            def __init__(self, filename):
                self.filename = filename

            def open(self, mode):
                """Opens the file this object is mocking a past task as having output."""
                return open(self.filename, mode)
        input_dummy = DummyInput(self.input_file)
        task.input = MagicMock(return_value=input_dummy)
        task.run()
        results = pandas.read_table(output_target.buffer, sep='\t', header=None,
                                    names=['course_id', 'date', 'subject_uri', 'subject_title', 'subject_language'])
        return results

    def check_subject_entry(self, data, row_num, expected):
        """
        Checks if the entries in a row of the data are what we expected, and returns whether this is true.


        Doesn't do asserts directly to help other methods be order-agnostic.


        Args:
            data is a pandas data frame representing the output of a run of the task.
            row_num is the row number to check.
            expected is a dictionary of values we expect to see.
        """
        working = True
        self.assertGreater(data.shape[0], row_num)
        row = data.iloc[row_num]
        for expected_key in expected.keys():
            self.assertIn(expected_key, row)
            if not row[expected_key] == expected[expected_key]:
                working = False
        return working

    def test_empty_catalog(self):
        """With an empty catalog, we expect no rows of data."""

        data = self.run_task("{}")
        self.assertEquals(data.shape[0], 0)

    def test_course_no_sbujects(self):
        """With a course with no subjects, we expect a row with NULLs."""

        data = self.run_task("{\"items\":[{\"course_id\":\"foo\", \"subjects\":[{}]}]}")
        # We expect an entry in the list of courses, since there is a course in the catalog.
        self.assertEquals(data.shape[0], 1)
        # We expect nulls for the three subject data columns.
        expected = {'course_id': 'foo',
                    'subject_uri': '\N', 'subject_title': '\N', 'subject_language': '\N'}  # pylint: disable-msg=W1402
        self.assertTrue(self.check_subject_entry(data, 0, expected))

    def test_course_with_one_subject(self):
        """With a course with one subject, we expect to see that subject"""
        input_data = """{\"items\":
                     [{\"course_id\":\"foo\", \"subjects\":
                        [{\"uri\":\"testing/uri\",\"title\":\"Testing\",\"language\":\"py\"}]}]}"""

        data = self.run_task(input_data)
        # We expect to see this course with the mock_subject information.
        self.assertEquals(data.shape[0], 1)
        goal = {'course_id': 'foo', 'subject_uri': 'testing/uri', 'subject_title': 'Testing', 'subject_language': 'py'}
        self.assertTrue(self.check_subject_entry(data, 0, goal))

    def test_course_with_two_subjects(self):
        """With a course with two subjects, we expect to see both of those subjects"""
        input_data = """{\"items\":
                     [{\"course_id\":\"foo\", \"subjects\":
                        [{\"uri\":\"testing/uri\",\"title\":\"Testing\",\"language\":\"py\"},
                        {\"uri\":\"bar/uri\",\"title\":\"Bar\",\"language\":\"py\"}]}]}"""

        data = self.run_task(input_data)
        # We expect to see this course with two subjects of information.
        self.assertEquals(data.shape[0], 2)
        subj1 = {'course_id': 'foo', 'subject_uri': 'testing/uri', 'subject_title': 'Testing', 'subject_language': 'py'}
        subj2 = {'course_id': 'foo', 'subject_uri': 'bar/uri', 'subject_title': 'Bar', 'subject_language': 'py'}

        self.assertTrue(self.check_subject_entry(data, 0, subj1) or self.check_subject_entry(data, 1, subj1))
        self.assertTrue(self.check_subject_entry(data, 0, subj2) or self.check_subject_entry(data, 1, subj2))

    def test_multiple_courses(self):
        input_data = """{\"items\":
                        [{\"course_id\":\"foo\", \"subjects\":
                            [{\"uri\":\"testing/uri\",\"title\":\"Testing\",\"language\":\"py\"}]},
                        {\"course_id\":\"bar\", \"subjects\":
                            [{\"uri\":\"testing/uri\",\"title\":\"Testing\",\"language\":\"py\"}]}]}"""
        data = self.run_task(input_data)
        # We expect to see two courses.
        self.assertEquals(data.shape[0], 2)
        subj1 = {'course_id': 'foo', 'subject_uri': 'testing/uri', 'subject_title': 'Testing', 'subject_language': 'py'}
        subj2 = {'course_id': 'bar', 'subject_uri': 'testing/uri', 'subject_title': 'Testing', 'subject_language': 'py'}
        self.assertTrue(self.check_subject_entry(data, 0, subj1) or self.check_subject_entry(data, 1, subj1))
        self.assertTrue(self.check_subject_entry(data, 0, subj2) or self.check_subject_entry(data, 1, subj2))
