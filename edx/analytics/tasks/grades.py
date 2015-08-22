# -*- coding: utf-8 -*-
"""
Grades: Ingest Grades into analytics to supplement various reports.

Grades are imported from the OfflineComputedGrades models
(the courseware_offlinecomputedgrade table).

We assume that some scheduled process is in place that computes offline grades
(e.g. daily) before this pipeline is run.
"""
import datetime
from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.mysql_dump import MysqlSelectTask, mysql_datetime
from edx.analytics.tasks.url import get_target_from_url
from edx.analytics.tasks.util.hive import (
    WarehouseMixin,
    HivePartition,
    HiveTableTask,
    HiveQueryToMysqlTask,
)
import json
import logging
import luigi

log = logging.getLogger(__name__)


class GradesParametersMixin(object):
    """
    Parameters common to the grade tasks:
        today: The current date. Should always be left at the default.
        max_age_days: Only use offline computed grades if they are less than this many days old.
    """
    today = luigi.DateParameter(default=datetime.datetime.utcnow().date())
    max_age_days = luigi.IntParameter(default=2)


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


class LetterGradeBreakdownTask(GradesParametersMixin, MapReduceJobTask):
    """
    Count the # of students with each possible letter grade.
    """
    output_root = luigi.Parameter()

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
        Outputs records with the number of students in this course who have each letter grade.

        Output is designed to be loaded as a Hive table.
        """
        course_id = key[0]
        grade_breakdown = {}  # key is the letter grade, value is the # of students with that grade
        total_enrolled = 0    # The offline grades table has an entry for each enrolled
                              # student, whether or not the student is active. This would only
                              # be unreliable if the offline grade computation did not begin and
                              # end after our 'updated' date cutoff time.

        for user_id, _updated, gradeset_str in entries:
            total_enrolled += 1
            try:
                gradeset = json.loads(gradeset_str)
            except ValueError:
                log.error('Unable to parse gradeset for user %s in course %s', user_id, course_id)
                continue

            letter_grade = gradeset['grade']
            if letter_grade is None:
                # This user has not done any graded work yet or is not passing.
                letter_grade = '\\N'  # Hive representation of NULL
            grade_breakdown[letter_grade] = grade_breakdown.get(letter_grade, 0) + 1

        for letter_grade, num_students in grade_breakdown.items():
            # Note: the pass cutoff is not necessarily 50%, so the only way to know if the
            # student is passing is to look at the letter grade. Currently the LMS sets the
            # letter grade to NULL (\N) when the student is not passing. (We also check for 'F'
            # and 'Fail' since those are shown in the LMS UI, but they don't seem to be used.)
            is_passing = letter_grade not in ('\\N', 'Fail', 'F')
            percent_of_students = float(num_students) / total_enrolled * 100
            yield (
                course_id,
                self.today.isoformat(),
                letter_grade,
                num_students,
                percent_of_students,
                is_passing,
            )

    def output(self):
        return get_target_from_url(self.output_root)


class GradesTaskDownstreamMixin(GradesParametersMixin, WarehouseMixin, MapReduceJobTaskMixin):
    """All parameters needed to run the tasks below."""
    pass


class LetterGradeBreakdownTableTask(GradesTaskDownstreamMixin, HiveTableTask):
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
            ('is_passing', 'BOOLEAN'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.today.isoformat())

    def requires(self):
        return LetterGradeBreakdownTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            output_root=self.partition_location,
            today=self.today,
            max_age_days=self.max_age_days,
        )

    def output(self):
        return self.requires().output()


class GradesPipelineTask(GradesTaskDownstreamMixin, luigi.WrapperTask):
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
            LetterGradeBreakdownTableTask(**kwargs),
        )
