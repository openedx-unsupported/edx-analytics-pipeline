"""
Luigi tasks for extracting tags distribution statistics from tracking log files.
"""
import logging

import luigi

import edx.analytics.tasks.util.eventlog as eventlog
import edx.analytics.tasks.util.opaque_key_util as opaque_key_util
from edx.analytics.tasks.common.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.common.mysql_load import MysqlInsertTask
from edx.analytics.tasks.common.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.util.decorators import workflow_entry_point
from edx.analytics.tasks.util.record import IntegerField, Record, StringField
from edx.analytics.tasks.util.url import get_target_from_url

log = logging.getLogger(__name__)


class TagsDistributionDownstreamMixin(object):
    """
    Base class for tags distribution calculations.

    """
    output_root = luigi.Parameter(
        description='Directory to store the output in.',
    )


class TagsDistributionPerCourse(
        TagsDistributionDownstreamMixin,
        EventLogSelectionMixin,
        MapReduceJobTask):
    """Calculates tags distribution."""

    def output(self):
        return get_target_from_url(self.output_root)

    def mapper(self, line):
        """
        Args:
            line: text line from a tracking event log.

        Yields:  (course_id, org_id, problem_id), (timestamp, saved_tags, is_correct)

        """
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, _ = value

        if event.get('event_type') != 'problem_check' or event.get('event_source') != 'server':
            return

        timestamp = eventlog.get_event_time_string(event)
        if timestamp is None:
            return

        course_id = eventlog.get_course_id(event)
        if not course_id:
            return

        org_id = opaque_key_util.get_org_id_for_course(course_id)

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            return

        problem_id = event_data.get('problem_id')
        if not problem_id:
            return

        is_correct = event_data.get('success') == 'correct'

        saved_tags = event.get('context').get('asides', {}).get('tagging_aside', {}).get('saved_tags', {})

        yield (course_id, org_id, problem_id), (timestamp, saved_tags, is_correct)

    def reducer(self, key, values):
        """
        Calculate the count of total/correct submissions for each pair: problem + related tag

        Args:
            key:  (course_id, org_id, problem_id)
            values:  iterator of (timestamp, saved_tags, is_correct)

        """
        course_id, org_id, problem_id = key

        num_correct = 0
        num_total = 0

        latest_timestamp = None
        latest_tags = None

        for timestamp, saved_tags, is_correct in values:
            if latest_timestamp is None or timestamp > latest_timestamp:
                latest_timestamp = timestamp
                latest_tags = saved_tags.copy() if saved_tags else None

            if is_correct:
                num_correct += 1

            num_total += 1

        if not latest_tags:
            return
        else:
            for tag_key, tag_val in latest_tags.iteritems():
                tag_val_lst = [tag_val] if isinstance(tag_val, basestring) else tag_val
                for val in tag_val_lst:
                    yield TagsDistributionRecord(
                        course_id=course_id,
                        org_id=org_id,
                        module_id=problem_id,
                        tag_name=tag_key,
                        tag_value=val,
                        total_submissions=num_total,
                        correct_submissions=num_correct).to_string_tuple()


class TagsDistributionRecord(Record):
    """Represents a count of total/correct submissions for the particular bunch (problem_id, tag_key, tag_value)."""

    course_id = StringField(length=255, nullable=False, description='Course id')
    org_id = StringField(length=255, nullable=False, description='Org id')
    module_id = StringField(length=255, nullable=False, description='Problem id')
    tag_name = StringField(length=255, nullable=False, description='Tag key')
    tag_value = StringField(length=255, nullable=False, description='Tag value')
    total_submissions = IntegerField(nullable=False, description='Number of total submissions')
    correct_submissions = IntegerField(nullable=False, description='Number of correct submissions')


@workflow_entry_point
class TagsDistributionWorkflow(
        TagsDistributionDownstreamMixin,
        EventLogSelectionDownstreamMixin,
        MapReduceJobTaskMixin,
        MysqlInsertTask,
        luigi.WrapperTask):
    """
    This task calculates total and correct submissions for each unique pair: problem id + connected tag.
    It makes sense to use this task only if:

    - xblock asides are enabled in your CMS and LMS settings (it could be done through the Django admin panel)
    - you have already populated MySQL tables `tagging_tagcategories` and `tagging_tagavailablevalues` with necessary
    tags values
    - you have already marked some questions in CMS with existed tags

    Once the LMS is properly configured, the assigned tags will be included in the emitted event each time
    a learner answers a tagged question. This task can then calculate aggregates based on these tagged events.

    The results of this task will be stored in the `tags_distribution` table in the resultstore database.
    When enabled in Insights, this table is used to display counts for a given set of tags.
    When the tags are used to label learning outcomes for individual problems, Insights can provide a sense of how
    learners in the LMS perform on different learning outcomes.
    """

    # Override the parameter that normally defaults to false. This ensures that the table will always be overwritten.
    overwrite = luigi.BoolParameter(
        default=True,
        description="Whether or not to overwrite existing outputs",
        significant=False
    )

    @property
    def insert_source_task(self):
        """
        Write to tags_distribution table.
        """
        return TagsDistributionPerCourse(
            n_reduce_tasks=self.n_reduce_tasks,
            output_root=self.output_root,
            interval=self.interval,
            source=self.source
        )

    @property
    def table(self):
        return "tags_distribution"

    @property
    def columns(self):
        return TagsDistributionRecord.get_sql_schema()

    @property
    def indexes(self):
        return [
            ('course_id',),
            ('module_id',),
            ('course_id', 'module_id'),
        ]
