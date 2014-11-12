"""Compute metrics related to user enrollments in courses"""

import datetime
import gzip
import json
import logging
import os
import re

import luigi

from edx.analytics.tasks.mapreduce import MultiOutputMapReduceJobTask, MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.url import get_target_from_url, url_path_join, ExternalURL
from edx.analytics.tasks.util import eventlog, opaque_key_util
from edx.analytics.tasks.util.datetime_util import add_microseconds, mysql_datetime_to_isoformat
from edx.analytics.tasks.util.event_factory import SyntheticEventFactory
from edx.analytics.tasks.util.hive import WarehouseMixin


log = logging.getLogger(__name__)

DEACTIVATED = 'edx.course.enrollment.deactivated'
ACTIVATED = 'edx.course.enrollment.activated'
VALIDATED = 'edx.course.enrollment.validated'
SENTINEL = 'edx.course.enrollment.sentinel'


class CourseEnrollmentValidationDownstreamMixin(EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin):
    """
    Defines parameters for passing upstream to tasks that use CourseEnrollmentValidationTask.

    Parameters:

        output_root: A URL to a path where output event files will be written.

        event_output:  A flag indicating that output should be in the form of events.
            Default = tuples.

        generate_before:  A flag indicating that events should be created preceding the specified interval.
            Default behavior is to suppress the generation of events before the specified interval.

    """
    # location to write output
    output_root = luigi.Parameter()

    # flag indicating whether to output synthetic events or tuples
    event_output = luigi.BooleanParameter(default=False)

    # If set, generates events that occur before the start of the specified interval.
    # Default is incremental validation.
    generate_before = luigi.BooleanParameter(default=False)

    # if set, expects that every user/course combination will have a validation event
    # TODO: implement.  Decide what happens if event is missing. (Output missing validation event.)
    require_validation = luigi.BooleanParameter(default=False)


class CourseEnrollmentValidationTask(
        CourseEnrollmentValidationDownstreamMixin, EventLogSelectionMixin, MapReduceJobTask):
    """Produce a data set that shows which days each user was enrolled in each course."""

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, _date_string = value

        event_type = event.get('event_type')
        if event_type is None:
            log.error("encountered event with no event_type: %s", event)
            return

        # TODO: add in mode changes as well...
        if event_type not in (DEACTIVATED, ACTIVATED, VALIDATED):
            return

        timestamp = eventlog.get_event_time_string(event)
        if timestamp is None:
            log.error("encountered event with bad timestamp: %s", event)
            return

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            return

        course_id = event_data.get('course_id')
        if course_id is None or not opaque_key_util.is_valid_course_id(course_id):
            log.error("encountered explicit enrollment event with invalid course_id: %s", event)
            return

        user_id = event_data.get('user_id')
        if user_id is None:
            log.error("encountered explicit enrollment event with no user_id: %s", event)
            return

        mode = event_data.get('mode')
        # TODO: update synthetic events with mode information.
        # (For now, permit synthetic events to be processed without mode info for validation purposes.)
        if mode is None:
            if 'synthesized' in event:
                mode = "honor"
            else:
                log.error("encountered explicit enrollment event with no mode: %s", event)
                return

        # Pull in extra properties provided only by synthetic enrollment validation events.
        validation_info = None
        if 'dump_start' in event_data:
            validation_info = {
                'is_active': event_data.get('is_active'),
                'created': event_data.get('created'),
                'dump_start': event_data.get('dump_start'),
                'dump_end': event_data.get('dump_end'),
            }

        # Make sure key values that are strings are properly encoded.
        # Note, however, that user_id is an int.
        key = (unicode(course_id).encode('utf-8'), user_id)
        yield key, (timestamp, event_type, mode, validation_info)

    def reducer(self, key, values):
        """Emit records for each day the user was enrolled in the course."""
        course_id, user_id = key

        options = {
            'event_output': self.event_output,
            'require_validation': self.require_validation,
            'generate_before': self.generate_before,
            'lower_bound_date_string': self.lower_bound_date_string,
        }
        event_stream_processor = ValidateEnrollmentForEvents(
            course_id, user_id, self.interval, values, **options
        )
        for datestamp, missing_enroll_event in event_stream_processor.missing_enrolled():
            yield datestamp, missing_enroll_event

    def output(self):
        return get_target_from_url(self.output_root)


#from functools import total_ordering


#@total_ordering
class EnrollmentEvent(object):
    """The critical information necessary to process the event in the event stream."""

    def __init__(self, timestamp, event_type, mode, validation_info):
        self.timestamp = timestamp
        self.event_type = event_type
        self.mode = mode
        if validation_info:
            self.is_active = validation_info['is_active']
            self.created = validation_info['created']
            self.dump_start = validation_info['dump_start']
            self.dump_end = validation_info['dump_end']

    def is_during_dump(self, timestamp):
        """Determine if a timestamp occurs during the current event's dump (if any)."""
        return (self.dump_start is not None and
                self.dump_start < timestamp and
                timestamp < self.dump_end)

    STATE_MAP = {
        VALIDATED: "validate",
        ACTIVATED: "activate",
        DEACTIVATED: "deactivate",
        SENTINEL: "start",
    }

    def get_state_string(self):
        """Output string representation of event type and is_active (if applies)."""
        state_name = self.STATE_MAP.get(self.event_type, "unknown")
        if self.event_type == VALIDATED:
            state_name += "(active)" if self.is_active else "(inactive)"
        return state_name

    def __repr__(self):
        return "{} at {} mode {}".format(self.get_state_string(), self.timestamp, self.mode)


class ValidateEnrollmentForEvents(object):
    """TODO: More to say...."""

    def __init__(self, course_id, user_id, interval, events, **kwargs):
        self.course_id = course_id
        self.user_id = user_id
        self.interval = interval
        self.creation_timestamp = None
        self.event_output = kwargs.get('event_output')
        self.require_validation = kwargs.get('require_validation')
        self.generate_before = kwargs.get('generate_before')
        self.lower_bound_date_string = kwargs.get('lower_bound_date_string')

        if self.event_output:
            self.factory = SyntheticEventFactory(
                event_source='server',
                synthesizer='enrollment_validation',
            )
            self.generate_output = self.synthetic_event
        else:
            self.generate_output = self.create_tuple

        # Create list of events in reverse order, as processing goes backwards
        # from validation states.
        self.sorted_events = [
            EnrollmentEvent(timestamp, event_type, mode, validation_info)
            for timestamp, event_type, mode, validation_info in sorted(events, reverse=True)
        ]

        self.reorder_within_dumps()

        # Add a marker event to signal the beginning of the interval.
        initial_state = EnrollmentEvent(None, SENTINEL, mode=None, validation_info=None)
        self.sorted_events.append(initial_state)

    def reorder_within_dumps(self):
        """
        Fix the timestamp of a validation event if an enrollment event occurs during the dump.
        """
        num_events = len(self.sorted_events) - 1
        for index in range(num_events):
            event = self.sorted_events[index]
            prev_event = self.sorted_events[index + 1]
            is_nonvalidate_during_validate = (
                event.event_type == VALIDATED and
                prev_event.event_type != VALIDATED and
                event.is_during_dump(prev_event.timestamp)
            )
            if is_nonvalidate_during_validate:
                is_active_is_inconsistent = (
                    (event.is_active and prev_event.event_type == DEACTIVATED) or
                    (not event.is_active and prev_event.event_type == ACTIVATED)
                )
                if is_active_is_inconsistent:
                    # Change the timestamp of the validation event to precede
                    # the other event, and swap them.
                    event.timestamp = add_microseconds(prev_event.timestamp, -1)
                    self.sorted_events[index] = prev_event
                    self.sorted_events[index + 1] = event

    def missing_enrolled(self):
        """
        A synthetic event is yielded for each transition in user's events for which a real event is missing.

        Yields:
            json-encoded string representing a synthetic event, or a tuple.
        """
        # The last element of the list is a placeholder indicating the beginning of the interval.
        # Don't process it.
        num_events = len(self.sorted_events) - 1

        all_missing_events = []
        validation_found = False
        for index in range(num_events):
            event = self.sorted_events[index]
            prev_event = self.sorted_events[index + 1]

            if event.event_type == VALIDATED:
                validation_found = True

            missing_events = self.check_transition(prev_event, event)
            if missing_events:
                all_missing_events.extend(missing_events)

            if self.require_validation and not validation_found:
                log.error("No validation event found for user %s in course %s", self.user_id, self.course_id)
        return all_missing_events

    def create_tuple(self, timestamp, event_type, reason, after=None, before=None):
        """Returns a tuple representation of the output, for TSV-based debugging."""
        datestamp = eventlog.timestamp_to_datestamp(timestamp)
        return datestamp, (self.course_id, self.user_id, timestamp, event_type, reason, after, before)

    def synthetic_event(self, timestamp, event_type, reason, after=None, before=None):
        """Create a synthetic event."""
        # data specific to course enrollment events:
        event_data = {
            'course_id': self.course_id,
            'user_id': self.user_id,
            # TODO: add the correct mode here.  (Should be passed in, with a default.)
            # 'mode': mode,
        }

        event_properties = {
            # main properties:
            'time': timestamp,
            'event_type': event_type,
            # stuff for context:
            'user_id': self.user_id,
            'course_id': self.course_id,
            'org_id': opaque_key_util.get_org_id_for_course(self.course_id),
            # stuff for synthesized:
            'reason': reason,
        }

        event = self.factory.create_event_dict(event_data, **event_properties)
        synthesized = event['synthesized']
        if after:
            synthesized['after_time'] = after
        if before:
            synthesized['before_time'] = before

        datestamp = eventlog.timestamp_to_datestamp(timestamp)
        return datestamp, json.dumps(event)

    def _get_fake_timestamp(self, after, before):
        """
        Pick a time in an interval.

        Picks a microsecond after `after`, else a microsecond before `before`.

        Input and output values are ISO format strings.
        """
        # Just pick the time at the beginning of the interval.
        if after:
            # Add a microsecond to 'after'
            return add_microseconds(after, 1)
        else:
            # Subtract a microsecond from 'before'
            return add_microseconds(before, -1)

    def _get_transition_string(self, prev_event, curr_event):
        """TODO"""
        return "{prev} => {curr}".format(
            prev=prev_event.get_state_string(),
            curr=curr_event.get_state_string(),
        )

    def check_transition(self, prev_event, curr_event):
        """TODO: """
        prev_type = prev_event.event_type
        curr_type = curr_event.event_type
        prev = prev_event.timestamp
        curr = curr_event.timestamp
        timestamp = self._get_fake_timestamp(prev, curr)
        reason = self._get_transition_string(prev_event, curr_event)

        if curr_type == VALIDATED:
            if self.creation_timestamp:
                # compare with previously-viewed (i.e. later-in-time) validation:
                if curr_event.created != self.creation_timestamp:
                    log.error("Encountered validation with different creation timestamp: %s => %s",
                              curr_event.created, self.creation_timestamp)
            # Use the earliest validation:
            self.creation_timestamp = curr_event.created

            if prev_type == VALIDATED:
                if curr_event.is_active and not prev_event.is_active:
                    return [self.generate_output(timestamp, ACTIVATED, reason, prev, curr)]
                elif not curr_event.is_active and prev_event.is_active:
                    return [self.generate_output(timestamp, DEACTIVATED, reason, prev, curr)]
            elif prev_type == ACTIVATED:
                if not curr_event.is_active:
                    return [self.generate_output(timestamp, DEACTIVATED, reason, prev, curr)]
            elif prev_type == DEACTIVATED:
                if curr_event.is_active:
                    return [self.generate_output(timestamp, ACTIVATED, reason, prev, curr)]
            elif prev_type == SENTINEL:
                # If we are validating only within an interval and the create_timestamp
                # is outside this interval, we can't know whether the events are really
                # missing or just not included.
                if not self.generate_before and self.creation_timestamp < self.lower_bound_date_string:
                    pass
                elif curr_event.is_active:
                    # TODO: check that creation_timestamp is before 'curr'
                    return [self.generate_output(
                        self.creation_timestamp, ACTIVATED, reason, self.creation_timestamp, curr
                    )]
                else:
                    # missing Activate and Deactivate events
                    # TODO: check that creation_timestamp is before 'curr'
                    event1 = self.generate_output(
                        self.creation_timestamp, ACTIVATED, reason, self.creation_timestamp, curr
                    )
                    timestamp2 = self._get_fake_timestamp(self.creation_timestamp, curr)
                    event2 = self.generate_output(timestamp2, DEACTIVATED, reason, self.creation_timestamp, curr)
                    return [event1, event2]

        elif curr_type == ACTIVATED:
            if prev_type == VALIDATED:
                if prev_event.is_active:
                    return [self.generate_output(timestamp, DEACTIVATED, reason, prev, curr)]
            elif prev_type == ACTIVATED:
                return [self.generate_output(timestamp, DEACTIVATED, reason, prev, curr)]
            elif prev_type == DEACTIVATED:
                pass  # normal case
            elif prev_type == SENTINEL:
                pass  # normal case

        elif curr_type == DEACTIVATED:
            if prev_type == VALIDATED:
                if not prev_event.is_active:
                    return [self.generate_output(timestamp, ACTIVATED, reason, prev, curr)]
            elif prev_type == ACTIVATED:
                pass  # normal case
            elif prev_type == DEACTIVATED:
                return [self.generate_output(timestamp, ACTIVATED, reason, prev, curr)]
            elif prev_type == SENTINEL:
                # TODO: check that creation_timestamp is before 'curr'
                # Note:  we are getting a significant number of deactivate events
                # without any later validation, so the creation timestamp is 'None'.

                # If we had a validation after the deactivation,
                # and it provided a creation_timestamp within the interval,
                # then there should be an activate within the interval.
                if self.creation_timestamp and (
                        self.generate_before or
                        self.creation_timestamp >= self.lower_bound_date_string):
                    return [self.generate_output(
                        self.creation_timestamp, ACTIVATED, reason, self.creation_timestamp, curr
                    )]
                elif self.generate_before:
                    # For now, hack the timestamp by making it a little before the deactivate,
                    # so that it at least has a value.
                    timestamp2 = self._get_fake_timestamp(None, curr)
                    return [self.generate_output(timestamp2, ACTIVATED, reason, None, curr)]


class CourseEnrollmentValidationPerDateTask(
        CourseEnrollmentValidationDownstreamMixin, MultiOutputMapReduceJobTask):
    """
    Outputs CourseEnrollmentValidationTask according to key (i.e. datestamp).

    Parameters:
        intermediate_output: a URL for the location to write intermediate output.

        output_root: location where the one-file-per-date outputs
            are written.

    """

    intermediate_output = luigi.Parameter()

    def requires(self):
        return CourseEnrollmentValidationTask(
            mapreduce_engine=self.mapreduce_engine,
            lib_jar=self.lib_jar,
            n_reduce_tasks=self.n_reduce_tasks,
            interval=self.interval,
            source=self.source,
            pattern=self.pattern,
            output_root=self.intermediate_output,
            event_output=self.event_output,
            generate_before=self.generate_before,
        )

    def mapper(self, line):
        datestamp, values = line.split('\t', 1)
        yield datestamp, values

    def multi_output_reducer(self, _key, values, output_file):
        with gzip.GzipFile(mode='wb', fileobj=output_file) as outfile:
            for value in values:
                outfile.write(value)
                outfile.write('\n')

    def output_path_for_key(self, datestamp):
        if self.event_output:
            # Match tracking.log-{datestamp}.gz format.
            filename = u'synthetic_enroll.log-{datestamp}.gz'.format(
                datestamp=datestamp.replace('-', ''),
            )
        else:
            # Want to have tsv as extension, rather than date.
            filename = u'synthetic_enroll-{datestamp}.tsv.gz'.format(
                datestamp=datestamp.replace('-', ''),
            )

        return url_path_join(self.output_root, filename)


class CreateEnrollmentValidationEventsTask(MultiOutputMapReduceJobTask):
    """
    Convert a database dump of course enrollment into log files of validation events.

    Read from a directory location that points to a Sqoop dump of student_courseenrollment
    table.  Use map reduce simply because it allows the multiple file output to be read
    uniformly.  But it allows us to also separate the enrollment results into separate
    courses so that validation runs can be more fine-grained.

    The date for the synthesized events is the start time of the Sqoop dump.  This
    is when the particular enrollment states were observed.
    """
    # Note: we could just read the corresponding validation data into
    # the reducer.  So this would just need to produce reducer input
    # instead of mapper input.  Problem with that is that if there
    # were courses for which there were database entries but no
    # events, they wouldn't get validated.  So we put the events into
    # the mapper to make sure all courses get processed.

    # This defines the directory (with the dt=<date> partition) that contains
    # the desired database dump.
    source_dir = luigi.Parameter()

    def requires_hadoop(self):
        # Check first if running locally with Sqoop output.
        target = get_target_from_url(self.source_dir)
        if isinstance(target, luigi.LocalTarget) and os.path.isdir(self.source_dir):
            files = [f for f in os.listdir(self.source_dir) if f.startswith("part")]
            for filename in files:
                yield ExternalURL(url_path_join(self.source_dir, filename))
        else:
            yield ExternalURL(self.source_dir)

    def init_local(self):
        super(CreateEnrollmentValidationEventsTask, self).init_local()

        # need to determine the date of the input, by reading the appropriate
        # metadata file.  File looks like this:
        # {"start_time": "2014-10-08T04:52:48.154228", "end_time": "2014-10-08T04:55:18.269070"}

        metadata_target = self._get_metadata_target()
        with metadata_target.open('r') as metadata_file:
            metadata = json.load(metadata_file)
            self.dump_start_time = metadata["start_time"]
            self.dump_end_time = metadata["end_time"]
            log.debug("Found self.dump_start_time = %s  end_time = %s", self.dump_start_time, self.dump_end_time)
            self.dump_date = ''.join((self.dump_start_time.split('T')[0]).split('-'))

        # Set the timestamp of all events to be the dump's end time.
        # The events that are actually dumped are not within a transaction,
        # so the actual event time may be earlier, anywhere up to the dump's start time.
        self.factory = SyntheticEventFactory(
            timestamp=self.dump_end_time,
            event_source='server',
            event_type=VALIDATED,
            synthesizer='enrollment_from_db',
            reason='db entry'
        )

    def _get_metadata_target(self):
        """Returns target for metadata file from the given dump."""
        # find the .metadata file in the source directory.
        metadata_path = url_path_join(self.source_dir, ".metadata")
        return get_target_from_url(metadata_path)

    def mapper(self, line):
        fields = line.split('\x01')
        if len(fields) != 6:
            log.error("Encountered bad input: %s", line)
            return

        (_db_id, user_id_string, encoded_course_id, mysql_created, mysql_is_active, mode) = fields

        # `created` is of the form '2012-07-25 12:26:22.0', coming out of
        # mysql.  Convert it to isoformat.
        created = mysql_datetime_to_isoformat(mysql_created)
        # `is_active` should be a boolean and `user_id` is an int.
        is_active = (mysql_is_active == "true")
        user_id = int(user_id_string)

        # Note that we do not have several standard properties that we
        # might expect in such an event.  These include a username,
        # host, session_id, agent.  These values will be stubbed by
        # the factory as empty strings.

        course_id = encoded_course_id.decode('utf-8')
        # data for the particular type of event:
        event_data = {
            'course_id': course_id,
            'user_id': user_id,
            'mode': mode,
            'is_active': is_active,
            'created': created,
            'dump_start': self.dump_start_time,
            'dump_end': self.dump_end_time,
        }

        # stuff for context:
        event_properties = {
            'user_id': user_id,
            'course_id': course_id,
            'org_id': opaque_key_util.get_org_id_for_course(course_id),
        }

        event = self.factory.create_event(event_data, **event_properties)

        # Use the original utf-8 version of the course_id as the key.
        # (Note that if we want everything zipped into a single file,
        # then we can just pass a single dummy value for the key instead of
        # breaking the output out by course_id.)
        yield encoded_course_id, event

    def multi_output_reducer(self, _key, values, output_file):
        with gzip.GzipFile(mode='wb', fileobj=output_file) as outfile:
            for value in values:
                outfile.write(value)
                outfile.write('\n')

    def output_path_for_key(self, course_id):
        filename_safe_course_id = opaque_key_util.get_filename_safe_course_id(course_id, '_')
        filename = u'{course_id}_enroll_validated_{dumpdate}.log.gz'.format(
            course_id=filename_safe_course_id,
            dumpdate=self.dump_date,
        )
        return url_path_join(self.output_root, filename)


class CreateAllEnrollmentValidationEventsTask(WarehouseMixin, MapReduceJobTaskMixin, luigi.WrapperTask):
    """
    TODO:
    """
    interval = luigi.DateIntervalParameter()
    output_root = luigi.Parameter()

    required_tasks = None

    def _get_required_tasks(self):
        """Internal method to actually calculate required tasks once."""
        start_date = self.interval.date_a
        end_date = self.interval.date_b
        table_name = "student_courseenrollment"
        source_root = url_path_join(self.warehouse_path, table_name)

        current_date = start_date
        while current_date < end_date:
            datestring = current_date.strftime('%Y-%m-%d')
            current_date += datetime.timedelta(days=1)

            src_datestring = "dt={}".format(datestring)
            source_dir = url_path_join(source_root, src_datestring)
            target = get_target_from_url(source_dir)
            if target.exists():
                output_dir = url_path_join(self.output_root, datestring)
                yield CreateEnrollmentValidationEventsTask(
                    source_dir=source_dir,
                    output_root=output_dir,
                    n_reduce_tasks=self.n_reduce_tasks,
                )

    def requires(self):
        if not self.required_tasks:
            self.required_tasks = [task for task in self._get_required_tasks()]

        return self.required_tasks

    def output(self):
        return [task.output() for task in self.requires()]
