from edx.analytics.tasks.mysql_load import MysqlInsertTask

from edx.analytics.tasks.url import get_target_from_url
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.util import eventlog
import luigi
import re
from edx.analytics.tasks.util.record import Record, StringField, IntegerField


class ViewRecord(Record):
    course_id = StringField(length=255, nullable=False)
    section = StringField(length=255, nullable=False)
    subsection = StringField(length=255, nullable=False)
    unique_user_views = IntegerField()
    total_views = IntegerField()


class ViewDistribution(EventLogSelectionMixin, MapReduceJobTask):

    SUBSECTION_ACCESSED_PATTERN = r'/courses/.*?courseware/([^/]+)/([^/]+)/.*$'

    output_root = luigi.Parameter()

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

        event_type = event.get('event_type')
        if event_type is None:
            return

        if event_type[:9] != '/courses/':
            return

        m = re.match(self.SUBSECTION_ACCESSED_PATTERN, event_type)
        if not m:
            return
        section, subsection = m.group(1, 2)

        username = event.get('username', '').strip()
        if not username:
            return

        course_id = eventlog.get_course_id(event)
        if not course_id:
            return

        yield (course_id, section, subsection), (username)

    def reducer(self, key, values):
        course_id, section, subsection = key

        unique_usernames = set()
        total_views = 0
        for username in values:
            unique_usernames.add(username)
            total_views += 1

        yield ViewRecord(
            course_id=course_id,
            section=section,
            subsection=subsection,
            unique_user_views=len(unique_usernames),
            total_views=total_views
        ).to_string_tuple()

    def output(self):
        return get_target_from_url(self.output_root)


class ViewDistributionMysqlTask(MapReduceJobTaskMixin, EventLogSelectionDownstreamMixin, MysqlInsertTask):

    output_root = luigi.Parameter()

    @property
    def table(self):
        return "content_views"

    @property
    def columns(self):
        return ViewRecord.get_sql_schema()

    @property
    def indexes(self):
        return [
            ('course_id',)
        ]

    @property
    def insert_source_task(self):
        return ViewDistribution(
            interval=self.interval,
            output_root=self.output_root,
            n_reduce_tasks=self.n_reduce_tasks
        )
