# -*- coding: utf-8 -*-
"""
Grades: Ingest Grades into analytics to supplement various reports.

Grades are imported from the OfflineComputedGrades models
(the courseware_offlinecomputedgrade table).

We assume that some scheduled process is in place that computes offline grades
(e.g. daily) before this pipeline is run.

This workflow contains two parallel tasks, one which computes aggregate grade stats for each
course, preserving historical data, and another which records the current grade of each student
and does not explicitly preserve historical data.

                   RawOfflineGradesImportTask
                               ▼
                               ▼
                   PerStudentGradeBreakdownTask
                       ▼              ▼
                       ▼              ▼
   LetterGradeBreakdownTask           ▼
                       ▼              ▼
                       ▼              ▼
LetterGradeBreakdownTableTask       PerStudentGradeBreakdownTableTask
                       ▼              ▼
                       ▼              ▼
LetterGradeBreakdownToMysqlTask     PerStudentGradeBreakdownToMysqlTask
                       ▼              ▼
                       ▼              ▼
                      GradesPipelineTask
"""
from collections import defaultdict
import datetime
from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.mysql_dump import MysqlSelectTask, mysql_datetime
from edx.analytics.tasks.mysql_load import MysqlInsertTask
from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.util.hive import WarehouseMixin, HivePartition, HiveTableTask
import json
import logging
import luigi

log = logging.getLogger(__name__)


class GradesParametersMixin(WarehouseMixin, MapReduceJobTaskMixin):
    """
    Parameters common to the grade tasks:
        today: The current date. Should always be left at the default since we cannot query
               historical data from the LMS.
        max_age_days: Only use offline computed grades if they are less than this many days old.
    """
    today = luigi.DateParameter(default=datetime.datetime.utcnow().date())
    max_age_days = luigi.IntParameter(default=2)

    @property
    def partition(self):
        return HivePartition('dt', self.today.isoformat())


class RawOfflineGradesImportTask(GradesParametersMixin, MysqlSelectTask):
    """
    Import "recent" offline computed grades from the LMS and store them in a .TSV file.
    """

    @property
    def query(self):
        return (
            "SELECT user_id, course_id, updated, gradeset FROM `courseware_offlinecomputedgrade` "
            "WHERE `updated` >= %s"
        )

    @property
    def query_parameters(self):
        oldest_update_accepted = self.today - datetime.timedelta(days=self.max_age_days)
        return (
            mysql_datetime(oldest_update_accepted),
        )

    @property
    def filename(self):
        return 'offline_grades_{0}.tsv'.format(self.today.isoformat())


class PerStudentGradeBreakdownTask(GradesParametersMixin, MapReduceJobTask):
    """
    Parse the raw JSON-encoded grade data coming from the LMS's offline grades table.
    """

    def requires(self):
        return RawOfflineGradesImportTask(
            today=self.today,
            max_age_days=self.max_age_days,
        )

    def mapper(self, line):
        """Group the input data by course."""
        (
            user_id,
            course_id,
            updated,
            gradeset,
        ) = line.split('\t')
        yield ((course_id, ), (user_id, updated, gradeset))

    def reducer(self, key, entries):
        """
        Parse the JSON data and separate out the fields we care about, then format them to be
        loaded as a Hive table.
        """
        course_id = key[0]

        for user_id, _updated, gradeset_str in entries:
            try:
                gradeset = json.loads(gradeset_str)
            except ValueError:
                log.error('Unable to parse gradeset for user %s in course %s', user_id, course_id)
                continue

            letter_grade = gradeset['grade']
            percent_grade = gradeset['percent'] * 100.
            if letter_grade is None:
                # This user has not done any graded work yet or is not passing.
                letter_grade = '\\N'  # Hive representation of NULL
            is_passing = letter_grade not in ('\\N', 'Fail', 'F')
            yield (
                course_id,
                user_id,
                letter_grade,
                percent_grade,
                int(is_passing),
            )

    @property
    def output_root(self):
        """
        The output directory, ready to become a Hive table
        Not a parameter because two different tasks require() this one.
        """
        # Note this must end in a slash for Hadoop
        return url_path_join(
            self.warehouse_path,
            PerStudentGradeBreakdownTableTask.table,
            self.partition.path_spec
        ) + '/'

    def output(self):
        return get_target_from_url(self.output_root)


class LetterGradeBreakdownTask(GradesParametersMixin, MapReduceJobTask):
    """
    Count the # of students with each possible letter grade.
    """
    output_root = luigi.Parameter()

    def requires(self):
        return PerStudentGradeBreakdownTask(
            today=self.today,
            max_age_days=self.max_age_days,
        )

    def mapper(self, line):
        """Group the input data by course."""
        (
            course_id,
            user_id,
            letter_grade,
            percent_grade,
            is_passing,
        ) = line.split('\t')
        yield ((course_id, ), (user_id, letter_grade, is_passing))

    def reducer(self, key, entries):
        """
        Outputs records with the number of students in this course who have each letter grade.

        Output is designed to be loaded as a Hive table.
        """
        course_id = key[0]
        grade_breakdown = defaultdict(int)  # key is the letter grade, value is the # of students with that grade
        is_passing_map = {}   # key is the letter grade, value is boolean indicating if it's a passing grade
        total_enrolled = 0    # The offline grades table has an entry for each enrolled
                              # student, whether or not the student is active. This would only
                              # be unreliable if the offline grade computation did not begin and
                              # end after our 'updated' date cutoff time.

        for _user_id, letter_grade, is_passing in entries:
            total_enrolled += 1
            grade_breakdown[letter_grade] += 1
            if letter_grade not in is_passing_map:
                is_passing_map[letter_grade] = is_passing

        for letter_grade, num_students in grade_breakdown.items():
            is_passing = is_passing_map[letter_grade]
            percent_of_students = float(num_students) / total_enrolled * 100
            yield (
                course_id,
                self.today.isoformat(),
                letter_grade,
                num_students,
                percent_of_students,
                int(is_passing),
            )

    def output(self):
        return get_target_from_url(self.output_root)


class LetterGradeBreakdownTableTask(GradesParametersMixin, HiveTableTask):
    """Imports grade breakdown data into a Hive table."""

    @property
    def table(self):
        return 'grade_breakdown'

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
            ('date', 'STRING'),
            ('letter_grade', 'STRING'),
            ('num_students', 'INT'),
            ('percent_students', 'DOUBLE'),
            ('is_passing', 'INT'),  # Actually boolean but hive booleans don't import automatically to MySQL
        ]

    def requires(self):
        return LetterGradeBreakdownTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            output_root=self.partition_location,
            today=self.today,
            max_age_days=self.max_age_days,
        )

    def output(self):
        return get_target_from_url(self.partition_location)


class LetterGradeBreakdownToMysqlTask(GradesParametersMixin, MysqlInsertTask):
    """Imports grade breakdown data to MySQL."""
    table = 'grade_breakdown'
    overwrite = luigi.BooleanParameter(default=False)

    @property
    def insert_source_task(self):
        return LetterGradeBreakdownTableTask(
            warehouse_path=self.warehouse_path,
            today=self.today,
            max_age_days=self.max_age_days,
            n_reduce_tasks=self.n_reduce_tasks,
        )

    @property
    def columns(self):
        return [
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('date', 'DATE NOT NULL'),
            ('letter_grade', 'VARCHAR(64) NULL'),
            ('num_students', 'INTEGER'),
            ('percent_students', 'DOUBLE'),
            ('is_passing', 'BOOLEAN'),
        ]

    @property
    def default_columns(self):
        """ Columns and constraints that are managed by MySQL """
        return [
            ('created', 'TIMESTAMP DEFAULT NOW()'),
            ('CONSTRAINT course_date_grade', 'UNIQUE (course_id, date, letter_grade)'),
        ]


class PerStudentGradeBreakdownTableTask(GradesParametersMixin, HiveTableTask):
    """Imports per-student grade data into a Hive table."""
    table = 'grades'

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
            ('user_id', 'INT'),
            ('letter_grade', 'STRING'),
            ('percent_grade', 'DOUBLE'),
            ('is_passing', 'INT'),  # Actually boolean but hive booleans don't import automatically to MySQL
        ]

    def requires(self):
        return PerStudentGradeBreakdownTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            today=self.today,
            max_age_days=self.max_age_days,
        )

    def output(self):
        return get_target_from_url(self.partition_location)


class PerStudentGradeBreakdownToMysqlTask(GradesParametersMixin, MysqlInsertTask):
    """Imports grade breakdown data to MySQL."""
    table = 'grades'
    overwrite = luigi.BooleanParameter(default=True)

    @property
    def insert_source_task(self):
        return PerStudentGradeBreakdownTableTask(
            warehouse_path=self.warehouse_path,
            today=self.today,
            max_age_days=self.max_age_days,
            n_reduce_tasks=self.n_reduce_tasks,
        )

    @property
    def columns(self):
        return [
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('user_id', 'INT NOT NULL'),
            ('letter_grade', 'VARCHAR(64) NULL'),
            ('percent_grade', 'DOUBLE'),
            ('is_passing', 'BOOLEAN'),
        ]

    @property
    def default_columns(self):
        """ Columns and constraints that are managed by MySQL """
        return [
            ('created', 'TIMESTAMP DEFAULT NOW()'),
            ('CONSTRAINT course_user', 'UNIQUE (course_id, user_id)'),
        ]


class GradesPipelineTask(GradesParametersMixin, luigi.WrapperTask):
    """ Run all the grades tasks and output the reports to MySQL. """

    def requires(self):
        kwargs = {
            'mapreduce_engine': self.mapreduce_engine,
            'n_reduce_tasks': self.n_reduce_tasks,
            'warehouse_path': self.warehouse_path,
            'today': self.today,
            'max_age_days': self.max_age_days,
        }
        yield (
            LetterGradeBreakdownToMysqlTask(**kwargs),
            PerStudentGradeBreakdownToMysqlTask(**kwargs),
        )
