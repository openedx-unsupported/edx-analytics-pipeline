"""
Luigi tasks for extracting course enrollment statistics from tracking log files.
"""
import luigi
import luigi.s3
import datetime

from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.pathutil import PathSetTask
from edx.analytics.tasks.url import get_target_from_url, url_path_join
import edx.analytics.tasks.util.eventlog as eventlog
import edx.analytics.tasks.util.opaque_key_util as opaque_key_util

import logging
log = logging.getLogger(__name__)

UNENROLLED = -1
ENROLLED = 1

################################
# Task Map-Reduce definitions
################################


class CourseEnrollmentEventsPerDayMixin(object):
    """Calculates daily change in enrollment for a user in a course, given raw event log input."""

    def mapper(self, line):
        """
        Generates output values for explicit enrollment events.

        Args:
            line: text line from a tracking event log.

        Yields:
            (course_id, user_id), (timestamp, action_value)

            where `timestamp` is in ISO format, with resolution to the millisecond
            and `action_value` = 1 (enrolled) or -1 (unenrolled).

        Example:
            (edX/DemoX/Demo_Course, dummy_userid), (2013-09-10T00:01:05.123456, 1)
        """
        parsed_tuple_or_none = get_explicit_enrollment_output(line)
        if parsed_tuple_or_none is not None:
            yield parsed_tuple_or_none

    def reducer(self, key, values):
        """
        Calculate status for each user on the end of each day where they changed their status.

        Args:
            key:  (course_id, user_id) tuple
            values:  iterator of (timestamp, action_value) tuples

        Yields:
            (course_id, datestamp), enrollment_change

            where `datestamp` is in ISO format, with resolution to the day
            and `enrollment_change` is the change on that date for an individual user.
            Produced values are -1 or 1.

        No output is yielded if a user enrolls and then unenrolls (or unenrolls and
        then enrolls) on a given day.  Only days with a change at the end of the day
        when compared with the previous day are output.

        Note that we don't bother to actually output the user_id,
        since it's not needed downstream (though it might be sometimes useful
        for debugging).

        Also note that this can be greatly simplified if we have
        confidence that there are no duplicate enrollment events. Then
        we could just have the mapper generate output with the key as
        (course_id, user_id, datestamp), with a value of just the
        action.  Then the reducer just sums the actions for the date,
        and yields those days with non-zero values.

        Example output:
            (edX/DemoX/Demo_Course, 2013-09-10), 1
            (edX/DemoX/Demo_Course, 2013-09-12), -1

        """
        log.debug("Found key in reducer: %s", key)
        course_id, user_id = key

        # Sort input values (by timestamp) to easily detect the end of a day.
        # Note that this assumes the timestamp values (strings) are in ISO
        # representation, so that the tuples will be ordered in ascending time value.
        sorted_values = sorted(values)
        if len(sorted_values) <= 0:
            return

        # Convert timestamps to dates, so we can group them by day.
        func = eventlog.timestamp_to_datestamp
        values = [(func(timestamp), value) for timestamp, value in sorted_values]

        # Add a stop item to ensure we process the last entry.
        values = values + [(None, None)]

        # Remove the first action and use it to initialize the state machine.
        first_date, first_enrollment_status = values.pop(0)
        last_reported_enrollment_status = UNENROLLED if first_enrollment_status == ENROLLED else ENROLLED
        enrollment_status = first_enrollment_status
        prev_date = first_date

        for (this_date, new_enrollment_status) in values:
            # Before we process a new date, report the state if it has
            # changed from the previously reported, if any.
            if this_date != prev_date and enrollment_status != last_reported_enrollment_status:
                log.debug("outputting date and value: %s %s", prev_date, enrollment_status)
                last_reported_enrollment_status = enrollment_status
                yield (course_id, prev_date), enrollment_status

            log.debug("accumulating date and value: %s %s", this_date, new_enrollment_status)
            # Consecutive changes of the same kind don't affect the state.
            if new_enrollment_status != enrollment_status:
                enrollment_status = new_enrollment_status
            else:
                log.warning("WARNING: duplicate enrollment event {status} "
                            "for user_id {user_id} in course {course_id} on {date}".format(
                                status=new_enrollment_status, user_id=user_id, course_id=course_id, date=this_date))

            prev_date = this_date

    def init_local(self):
        """
        Empty local initialization method to make this mixin of the same form as other reducer-containing tasks.
        """
        return


class CourseEnrollmentChangesPerDayMixin(object):
    """Calculates daily changes in enrollment, given per-user net changes by date."""

    def mapper(self, line):
        """
        Args:  tab-delimited values in a single text line

        Yields:  (course_id, datestamp), enrollment_change

        Example:
            (edX/DemoX/Demo_Course, 2013-09-10), 1
            (edX/DemoX/Demo_Course, 2013-09-12), -1

        """
        course_id, date, enrollment_change = line.split('\t')
        yield (course_id, date), enrollment_change

    def reducer(self, key, values):
        """
        Reducer: sums enrollments for a given course on a particular date.

        Args:
            key:  (course_id, datestamp) tuple
            values:  iterator of enrollment_changes

            Input `enrollment_changes` are the enrollment changes on a day due to a specific user.
            Each user with a change has a separate input, either -1 (unenroll) or 1 (enroll).

        Yields:
            (course_id, datestamp), enrollment_change

            Output `enrollment_change` is summed across all users, one output per course.

        """
        log.debug("Found key in second reducer: %s", key)
        count = sum(int(v) for v in values)
        yield key, count

    def init_local(self):
        """
        Empty local initialization method to make this mixin of the same form as other reducer-containing tasks.
        """
        return


class BaseCourseEnrollmentTaskDownstreamMixin(OverwriteOutputMixin, MapReduceJobTaskMixin):
    """
    Base class mixin for course enrollment calculations.

    """
    name = luigi.Parameter(
        description='A unique identifier to distinguish one run from another.  It is used in '
        'the construction of output filenames, so each run will have distinct outputs.',
    )
    src = luigi.Parameter(
        is_list=True,
        description='A list of URLs to the root location of input tracking log files.',
    )
    dest = luigi.Parameter(
        description='A URL to the root location to write output file(s).',
    )
    include = luigi.Parameter(
        is_list=True,
        default=('*',),
        description='A list of patterns to be used to match input files, relative to `src` URL. '
        'The default value is [\'*\'].',
    )
    manifest = luigi.Parameter(
        default=None,
        description='A URL to a file location that can store the complete set of input files.',
    )
    run_date = luigi.Parameter(
        default=datetime.date.today(),
        description='The date to use as the partition version. Default is today.',
    )


##################################
# Task requires/output definitions
##################################

class CourseEnrollmentEventsPerDay(
        CourseEnrollmentEventsPerDayMixin,
        BaseCourseEnrollmentTaskDownstreamMixin,
        MapReduceJobTask):
    """Calculates daily change in enrollment for a user in a course, given raw event log input."""
    # input_format overwrites the default value of none from MapReduceJobTaskMixin
    input_format = luigi.Parameter(config_path={'section': 'manifest', 'name': 'input_format'})

    def requires(self):
        return PathSetTask(self.src, self.include, self.manifest)

    def output(self):
        output_name = 'course_enrollment_events_per_day_{name}/dt={date}/'.format(name=self.name, date=self.run_date)
        return get_target_from_url(url_path_join(self.dest, output_name))

    def run(self):
        self.remove_output_on_overwrite()
        super(CourseEnrollmentEventsPerDay, self).run()


class CourseEnrollmentChangesPerDay(
        CourseEnrollmentChangesPerDayMixin,
        BaseCourseEnrollmentTaskDownstreamMixin,
        MapReduceJobTask):
    """Calculates daily changes in enrollment, given per-user net changes by date."""
    def requires(self):
        return CourseEnrollmentEventsPerDay(
            mapreduce_engine=self.mapreduce_engine,
            lib_jar=self.lib_jar,
            n_reduce_tasks=self.n_reduce_tasks,
            name=self.name,
            src=self.src,
            dest=self.dest,
            include=self.include,
            manifest=self.manifest,
            overwrite=self.overwrite,
        )

    def run(self):
        self.remove_output_on_overwrite()
        super(CourseEnrollmentChangesPerDay, self).run()

    def output(self):
        output_name = 'course_enrollment_changes_per_day_{name}/dt={date}/'.format(name=self.name, date=self.run_date)

        return get_target_from_url(url_path_join(self.dest, output_name))


################################
# Helper methods
################################

def get_explicit_enrollment_output(line):
    """
    Generates output values for explicit enrollment events.

    Args:

      line: text line from a tracking event log.

    Returns:

      (course_id, user_id), (timestamp, action_value)

        where action_value = 1 (enrolled) or -1 (unenrolled)
        and timestamp is in ISO format, with resolution to the millisecond.

      or None if there is no valid enrollment event on the line.

    Example:
            (edX/DemoX/Demo_Course, dummy_userid), (2013-09-10T00:01:05.123456, 1)

    """
    # Before parsing, check that the line contains something that
    # suggests it's an enrollment event.
    if 'edx.course.enrollment' not in line:
        return None

    # try to parse the line into a dict:
    event = eventlog.parse_json_event(line)
    if event is None:
        # The line didn't parse.  For this specific purpose,
        # we can assume that all enrollment-related lines would parse,
        # and these non-parsing lines would get skipped anyway.
        return None

    # get event type, and check that it exists:
    event_type = event.get('event_type')
    if event_type is None:
        log.error("encountered event with no event_type: %s", event)
        return None

    # convert the type to a value:
    if event_type == 'edx.course.enrollment.activated':
        action_value = ENROLLED
    elif event_type == 'edx.course.enrollment.deactivated':
        action_value = UNENROLLED
    else:
        # not an enrollment event...
        return None

    # get the timestamp:
    datetime = eventlog.get_event_time(event)
    if datetime is None:
        log.error("encountered event with bad datetime: %s", event)
        return None
    timestamp = eventlog.datetime_to_timestamp(datetime)

    # Use the `user_id` from the event `data` field, since the
    # `user_id` in the `context` field is the user who made the
    # request but not necessarily the one who got enrolled.  (The
    # `course_id` should be the same in `context` as in `data`.)

    # Get the event data:
    event_data = eventlog.get_event_data(event)
    if event_data is None:
        # Assume it's already logged (and with more specifics).
        return None

    # Get the course_id from the data, and validate.
    course_id = opaque_key_util.normalize_course_id(event_data['course_id'])
    if not opaque_key_util.is_valid_course_id(course_id):
        log.error("encountered explicit enrollment event with bogus course_id: %s", event)
        return None

    # Get the user_id from the data:
    user_id = event_data.get('user_id')
    if user_id is None:
        log.error("encountered explicit enrollment event with no user_id: %s", event)
        return None

    # For now, ignore the enrollment 'mode' (e.g. 'honor').

    return (course_id, user_id), (timestamp, action_value)
