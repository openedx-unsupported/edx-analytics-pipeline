"""
Test the course structure processing task which extracts the information needed for the internal reporting course
warehouse table.

Testing strategy:
    Empty course structure json (expect empty output)
    Course structure json with one course listed which is missing some fields (expect Hive nulls for those fields)
    Course structure json with one course listed which has all the fields needed
    Course structure json with multiple courses listed
    Course structure json with a malformed course (like a list) inside
    Course structure json with a course with a unicode-containing name inside
"""

import tempfile
import os
import shutil
import datetime
import pandas
import json
from edx.analytics.tasks.load_internal_reporting_course import ProcessCourseStructureAPIData
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.target import FakeTarget

from mock import MagicMock

# pylint: disable-msg=anomalous-unicode-escape-in-string


class TestCourseInformation(unittest.TestCase):
    """Tests for the task parsing the course structure information for use in the internal reporting course table."""

    TEST_DATE = '2015-08-25'

    def setUp(self):
        self.temp_rootdir = tempfile.mkdtemp()
        self.input_dir = os.path.join(self.temp_rootdir, "input")
        os.mkdir(self.input_dir)
        self.input_file = os.path.join(self.input_dir, "courses_raw", "dt=" + self.TEST_DATE, "course_structure.json")
        self.addCleanup(self.cleanup, self.temp_rootdir)

    def cleanup(self, dirname):
        """Remove the temp directory only if it exists."""
        if os.path.exists(dirname):
            shutil.rmtree(dirname)

    def run_task(self, source):
        """Helper utility for running task under test"""

        self.input_file = "course_structure.json"
        with open(self.input_file, 'w') as fle:
            fle.write(source.encode('utf-8'))

        fake_warehouse_path = self.input_dir

        task = ProcessCourseStructureAPIData(warehouse_path=fake_warehouse_path, run_date=datetime.date(2015, 8, 25))

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
                                    names=['course_id', 'course_org_id', 'course_number', 'course_run',
                                           'course_start', 'course_end', 'course_name'])
        return results

    def check_structure_entry(self, data, row_num, expected):
        """
        Checks if the entries in a row of the data are what we expected, and returns whether this is true.

        Args:
            data is a pandas data frame representing the output of a run of the task.
            row_num is the row number to check, starting from 0.
            expected is a dictionary of values we expect to see.
        """
        self.assertGreater(data.shape[0], row_num)
        row = data.iloc[row_num]
        self.assertEqual(dict(row), expected)

    def test_empty_structure(self):
        """With an empty structure information json, we expect no rows of data."""
        data = self.run_task("{}")
        self.assertEquals(data.shape[0], 0)

    def test_course_missing_data(self):
        """With a course with some data missing, we expect a row with null values in some columns."""
        course_with_missing_data = {"results": [{"id": "foo", "org": "bar"}]}
        data = self.run_task(json.dumps(course_with_missing_data))
        # We expect an entry in the list of courses, since there is a course in the list.
        self.assertEquals(data.shape[0], 1)
        # We expect nulls for the columns aside from course id and course org id.
        expected = {
            'course_id': 'foo',
            'course_org_id': 'bar',
            'course_number': '\N',
            'course_run': '\N',
            'course_start': '\N',
            'course_end': '\N',
            'course_name': '\N'
        }
        self.check_structure_entry(data, 0, expected)

    def test_single_course(self):
        """With a course with one all the necessary information, we expect to see that course."""
        input_data = {
            "results": [
                {
                    "id": "foo",
                    "name": "Foo",
                    "org": "bar",
                    "course": "Baz",
                    "run": "2T2015",
                    "start": "2015-08-24T00:00:00Z",
                    "end": "2016-08-25T00:00:00Z"
                }
            ]
        }

        data = self.run_task(json.dumps(input_data))
        # We expect to see this course with the mock structure information.
        self.assertEquals(data.shape[0], 1)
        expected = {
            'course_id': 'foo',
            'course_name': 'Foo',
            'course_org_id': 'bar',
            'course_number': 'Baz',
            'course_run': '2T2015',
            'course_start': '2015-08-24T00:00:00+00:00',
            'course_end': '2016-08-25T00:00:00+00:00'
        }
        self.check_structure_entry(data, 0, expected)

    def test_multiple_courses(self):
        """With two courses, we expect to see both of them."""
        input_data = {
            "results": [
                {
                    "id": "foo",
                    "name": "Foo",
                    "org": "bar",
                    "course": "Baz",
                    "run": "2T2015",
                    "start": "2015-08-24T00:00:00Z",
                    "end": "2016-08-25T00:00:00Z"
                },
                {
                    "id": "foo2",
                    "name": "Foo2",
                    "org": "bar2",
                    "course": "Baz",
                    "run": "2T2015",
                    "start": "2015-08-24T00:00:00Z"
                }
            ]
        }

        data = self.run_task(json.dumps(input_data))
        # We expect to see two courses.
        self.assertEquals(data.shape[0], 2)
        course1 = {
            'course_id': 'foo',
            'course_name': 'Foo',
            'course_org_id': 'bar',
            'course_number': 'Baz',
            'course_run': '2T2015',
            'course_start': '2015-08-24T00:00:00+00:00',
            'course_end': '2016-08-25T00:00:00+00:00'
        }
        course2 = {
            'course_id': 'foo2',
            'course_name': 'Foo2',
            'course_org_id': 'bar2',
            'course_number': 'Baz',
            'course_run': '2T2015',
            'course_start': '2015-08-24T00:00:00+00:00',
            'course_end': '\N'
        }

        self.check_structure_entry(data, 0, course1)
        self.check_structure_entry(data, 1, course2)

    def test_malformed_course(self):
        """
        If a single course in the API response is malformed, we want to skip over the malformed course without
        throwing an error and load the rest of the courses.
        """
        input_data = {
            "results": [
                [],
                {
                    "id": "foo2",
                    "name": "Foo2",
                    "org": "bar2",
                    "course": "Baz",
                    "run": "2T2015",
                    "start": "2015-08-24T00:00:00Z"
                }
            ]
        }
        data = self.run_task(json.dumps(input_data))
        # We expect to see the second course, which is well-formed, but nothing from the first.
        self.assertEquals(data.shape[0], 1)
        expected = {
            'course_id': 'foo2',
            'course_name': 'Foo2',
            'course_org_id': 'bar2',
            'course_number': 'Baz',
            'course_run': '2T2015',
            'course_start': '2015-08-24T00:00:00+00:00',
            'course_end': '\N'
        }
        self.check_structure_entry(data, 0, expected)

    def test_unicode_course_name(self):
        """Unicode course names should be handled properly, so that they appear correctly in the database."""
        input_data = {
            "results": [
                {
                    "id": "foo",
                    "name": u"Fo\u263a",
                    "org": "bar",
                    "course": "Baz",
                    "run": "2T2015",
                    "start": "2015-08-24T00:00:00Z",
                    "end": "2016-08-25T00:00:00Z"
                }
            ]
        }
        data = self.run_task(json.dumps(input_data))
        # We expect to see this course with the mock structure information.
        # NB: if the test fails, you may get an error from nose about "'ascii' codec can't decode byte 0xe2";
        # this arises from the fact that nose is trying to print the difference between the result data and expected
        # but can't handle the unicode strings.  This "error" really indicates a test failure.
        self.assertEquals(data.shape[0], 1)
        expected = {
            'course_id': 'foo',
            'course_name': 'Fo\xe2\x98\xba',
            'course_org_id': 'bar',
            'course_number': 'Baz',
            'course_run': '2T2015',
            'course_start': '2015-08-24T00:00:00+00:00',
            'course_end': '2016-08-25T00:00:00+00:00'
        }
        self.check_structure_entry(data, 0, expected)
