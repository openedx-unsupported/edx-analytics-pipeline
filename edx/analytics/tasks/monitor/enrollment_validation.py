"""Compute metrics related to user enrollments in courses"""

import datetime
import gzip
import json
import logging
import os

import luigi
import luigi.task

from edx.analytics.tasks.common.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin, MultiOutputMapReduceJobTask
from edx.analytics.tasks.common.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.common.sqoop import METADATA_FILENAME
from edx.analytics.tasks.insights.database_imports import ImportStudentCourseEnrollmentTask
from edx.analytics.tasks.util import eventlog, opaque_key_util
from edx.analytics.tasks.util.datetime_util import add_microseconds, ensure_microseconds, mysql_datetime_to_isoformat
from edx.analytics.tasks.util.event_factory import SyntheticEventFactory
from edx.analytics.tasks.util.hive import WarehouseMixin
from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join

log = logging.getLogger(__name__)

# Product event_type values:
DEACTIVATED = 'edx.course.enrollment.deactivated'
ACTIVATED = 'edx.course.enrollment.activated'
MODE_CHANGED = 'edx.course.enrollment.mode_changed'

# Validation-event event_type values:
VALIDATED = 'edx.course.enrollment.validated'

# Internal markers:
SENTINEL = 'sentinel_event_type'
MISSING = 'missing_event_type'


class CourseEnrollmentValidationDownstreamMixin(EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin):
    """
    Defines parameters for passing upstream to tasks that use CourseEnrollmentValidationTask.

    """
    # location to write output
    output_root = luigi.Parameter(
        description='A URL to a path where output event files will be written.',
    )

    # Flag indicating whether to output synthetic events or tuples
    tuple_output = luigi.BoolParameter(
        default=False,
        description='A flag indicating that output should be in the form of tuples, not events. '
        'Default is False (output is events).',
    )

    # If set, generates events that occur before the start of the specified interval.
    # Default is incremental validation.
    generate_before = luigi.BoolParameter(
        default=False,
        description='A flag indicating that events should be created preceding the '
        'specified interval. Default behavior is to suppress the generation of events '
        'before the specified interval.',
    )

    # If set, events are included for transitions that don't result in a
    # change in enrollment state.  (For example, two activations in a row.)
    include_nonstate_changes = luigi.BoolParameter(
        default=False,
        description='A flag indicating that events should be created '
        'to fix all transitions, even those that don\'t result in a change in enrollment '
        'state.  An "activate" following another "activate" is one such example. '
        'Default behavior is to skip generating events for non-state changes.',
    )

    # If set, events that would be generated before this timestamp would instead
    # be assigned this timestamp.
    earliest_timestamp = luigi.DateHourParameter(
        default=None,
        description='A "DateHour" parameter ("yyyy-mm-ddThh"), which if set, '
        'specifies the earliest timestamp that should occur in the output.  Events '
        'that would be generated before this timestamp would instead be assigned this '
        'timestamp.  This is left unspecified by default.',
    )

    # If set, users with events before this timestamp would be expected to have
    # a corresponding validation event.
    expected_validation = luigi.DateHourParameter(
        default=None,
        description='A "DateHour" parameter ("yyyy-mm-ddThh"), which if set, '
        'specifies a point in time where every user with events before this time '
        'should also have a corresponding validation event.  Those without such an '
        'validation event were not really created, and events should be synthesized '
        'to simulate "roll back" of the events.',
    )


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

        if event_type not in (DEACTIVATED, ACTIVATED, MODE_CHANGED, VALIDATED):
            return

        timestamp = eventlog.get_event_time_string(event)
        if timestamp is None:
            log.error("encountered event with bad timestamp: %s", event)
            return

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            return

        course_id = opaque_key_util.normalize_course_id(event_data.get('course_id'))
        if course_id is None or not opaque_key_util.is_valid_course_id(course_id):
            log.error("encountered explicit enrollment event with invalid course_id: %s", event)
            return

        user_id = event_data.get('user_id')
        if user_id is None:
            log.error("encountered explicit enrollment event with no user_id: %s", event)
            return

        mode = event_data.get('mode')
        if mode is None:
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

        earliest_timestamp_value = None
        if self.earliest_timestamp is not None:
            earliest_timestamp_value = ensure_microseconds(
                self.earliest_timestamp.isoformat()  # pylint: disable=no-member
            )
        expected_validation_value = None
        if self.expected_validation is not None:
            expected_validation_value = ensure_microseconds(
                self.expected_validation.isoformat()  # pylint: disable=no-member
            )
        options = {
            'tuple_output': self.tuple_output,
            'include_nonstate_changes': self.include_nonstate_changes,
            'generate_before': self.generate_before,
            'lower_bound_date_string': self.lower_bound_date_string,
            'earliest_timestamp': earliest_timestamp_value,
            'expected_validation': expected_validation_value,
        }
        event_stream_processor = ValidateEnrollmentForEvents(
            course_id, user_id, self.interval, values, **options
        )
        for datestamp, missing_enroll_event in event_stream_processor.missing_enrolled():
            yield datestamp, missing_enroll_event

    def output(self):
        return get_target_from_url(self.output_root)


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
        MODE_CHANGED: "mode_change",
        SENTINEL: "start",
        MISSING: "missing",
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
    """
    Collects and validates events for a given user in a given course.
    """
    def __init__(self, course_id, user_id, interval, events, **kwargs):
        self.course_id = course_id
        self.user_id = user_id
        self.interval = interval
        self.creation_timestamp = None
        self.tuple_output = kwargs.get('tuple_output')
        self.include_nonstate_changes = kwargs.get('include_nonstate_changes')
        self.generate_before = kwargs.get('generate_before')
        self.lower_bound_date_string = kwargs.get('lower_bound_date_string')
        self.earliest_timestamp = kwargs.get('earliest_timestamp')
        self.expected_validation = kwargs.get('expected_validation')

        if self.tuple_output:
            self.generate_output = self._create_tuple
        else:
            self.factory = SyntheticEventFactory(
                event_source='server',
                synthesizer='enrollment_validation',
            )
            self.generate_output = self._synthetic_event

        # Create list of events in reverse order, as processing goes backwards
        # from validation states.
        self.sorted_events = [
            EnrollmentEvent(timestamp, event_type, mode, validation_info)
            for timestamp, event_type, mode, validation_info in sorted(events, reverse=True)
        ]

        self._reorder_within_dumps()

        if self.expected_validation:
            self._check_for_missing_validation()

        # Add a marker event to signal the beginning of the interval.
        initial_state = EnrollmentEvent(None, SENTINEL, mode='honor', validation_info=None)
        self.sorted_events.append(initial_state)

    def _reorder_within_dumps(self):
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
                mode_is_inconsistent = (event.mode != prev_event.mode and prev_event.event_type == MODE_CHANGED)

                if is_active_is_inconsistent or mode_is_inconsistent:
                    # Change the timestamp of the validation event to precede
                    # the other event, and swap them.
                    event.timestamp = add_microseconds(prev_event.timestamp, -1)
                    self.sorted_events[index] = prev_event
                    self.sorted_events[index + 1] = event

    def _check_for_missing_validation(self):
        """Check last event to see if it is missing an expected validation event."""
        if len(self.sorted_events) > 0 and self.sorted_events[0].timestamp < self.expected_validation:
            validation_info = {
                'is_active': False,
                'created': None,
                'dump_start': None,
                'dump_end': None,
            }
            missing_event = [EnrollmentEvent(
                self.expected_validation, MISSING, mode=None, validation_info=validation_info
            )]
            missing_event.extend(self.sorted_events)
            self.sorted_events = missing_event

    def missing_enrolled(self):
        """
        A synthetic event is yielded for each transition in user's events for which a real event is missing.

        Yields:
            json-encoded string representing a synthetic event, or a tuple.
        """
        # The last element of the list is a placeholder indicating the beginning of the interval.
        # Don't process it.
        num_events = len(self.sorted_events) - 1

        self._initialize_state(self.sorted_events[0])
        all_missing_events = []
        for index in range(num_events):
            event = self.sorted_events[index + 1]
            missing_events = self._check_event(event)
            if missing_events:
                all_missing_events.extend(missing_events)

        return all_missing_events

    def _create_tuple(self, timestamp, event_type, mode, reason, after=None, before=None):
        """Returns a tuple representation of the output, for TSV-based debugging."""
        datestamp = eventlog.timestamp_to_datestamp(timestamp)
        return datestamp, (self.course_id, self.user_id, timestamp, event_type, mode, reason, after, before)

    def _synthetic_event(self, timestamp, event_type, mode, reason, after=None, before=None):
        """Create a synthetic event."""
        # data specific to course enrollment events:
        event_data = {
            'course_id': self.course_id,
            'user_id': self.user_id,
            'mode': mode,
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

    def _truncate_timestamp(self, timestamp):
        """Makes sure that timestamp is no earlier than limit specified, if any."""
        if self.earliest_timestamp and timestamp < self.earliest_timestamp:
            return self.earliest_timestamp
        else:
            return timestamp

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

    def _get_reason_string(self, prev_event, curr_event_string, curr_mode=None):
        """Provide a readable string giving the reason for generating the synthetic event."""
        if curr_mode:
            return "{prev} => {curr} ({prev_mode}=>{curr_mode})".format(
                prev=prev_event.get_state_string(),
                curr=curr_event_string,
                prev_mode=prev_event.mode,
                curr_mode=curr_mode,
            )
        else:
            return "{prev} => {curr}".format(
                prev=prev_event.get_state_string(),
                curr=curr_event_string,
            )

    def _initialize_state(self, event):
        """Define initial values for validation state machine."""
        self._update_state(event)
        # If the most-recent event is a mode-change event, then we don't
        # know from it what the activation is.
        if event.event_type == MODE_CHANGED:
            self.activation_type = None

    def _update_state(self, event):
        """Define current values for validation state machine."""
        # Some events define an activation state.
        if event.event_type != MODE_CHANGED:
            self.activation_type = event.event_type
            self.activation_state = event.get_state_string()
            self.activation_timestamp = event.timestamp

        # Most events set mode.
        self.mode_changed = (event.event_type == MODE_CHANGED)
        if event.event_type != MISSING:
            self.current_mode = event.mode
            self.mode_type = event.event_type
            self.mode_state = event.get_state_string()
            self.mode_timestamp = event.timestamp
        else:
            self.current_mode = None

        # Only validation events define a created timestamp and activation state.
        if event.event_type == VALIDATED:
            self.currently_active = event.is_active

            if self.creation_timestamp:
                # compare with previously-viewed (i.e. later-in-time) validation:
                if event.created != self.creation_timestamp:
                    log.error("Encountered validation with different creation timestamp: %s => %s",
                              event.created, self.creation_timestamp)
            # Use the earliest validation:
            self.creation_timestamp = event.created

    def _check_for_mode_change(self, prev_event, last_timestamp):
        """Check if a mode-change event should be synthesized."""
        # There are several reasons to suppress a mode-change.
        # The most obvious is if there is no change in mode.
        # Other cases include:
        #  * No "current" mode (e.g. current event is "MISSING").
        #  * Current event is known to reset the mode, so no comparison
        #    is needed (or possible).  This includes MODE_CHANGED and ACTIVATE
        #    events.
        #  * No previous event (i.e. previous event is "SENTINEL").
        suppress_mode_change = (
            prev_event.mode == self.current_mode or
            not self.current_mode or
            self.mode_changed or
            self.mode_type == ACTIVATED or
            prev_event.event_type == SENTINEL
        )

        if suppress_mode_change:
            return []
        else:
            curr = self.mode_timestamp
            timestamp = self._get_fake_timestamp(last_timestamp, curr)
            reason = self._get_reason_string(prev_event, self.mode_state, self.current_mode)
            return [self.generate_output(timestamp, MODE_CHANGED, self.current_mode, reason, last_timestamp, curr)]

    def _check_on_activated(self, generate_output_for_event):
        """Check if a deactivation event should be synthesized after an activation event."""
        if self.activation_type == ACTIVATED and self.include_nonstate_changes:
            # Duplicate activate event (a/a).
            return [generate_output_for_event(DEACTIVATED)]
        elif self.activation_type == DEACTIVATED:
            pass  # normal case
        elif self.activation_type == VALIDATED and self.currently_active:
            pass  # normal case
        elif self.activation_type == VALIDATED and not self.currently_active:
            # Missing deactivate event (a/vi)
            return [generate_output_for_event(DEACTIVATED)]
        elif self.activation_type == MISSING:
            # Missing deactivate event (a/m)
            return [generate_output_for_event(DEACTIVATED)]
        return []

    def _check_on_deactivated(self, generate_output_for_event):
        """Check if an activation event should be synthesized after a deactivation event."""
        if self.activation_type == ACTIVATED:
            pass  # normal case
        elif self.activation_type == DEACTIVATED and self.include_nonstate_changes:
            # Duplicate deactivate event (d/d).
            return [generate_output_for_event(ACTIVATED)]
        elif self.activation_type == VALIDATED and not self.currently_active:
            pass  # normal case
        elif self.activation_type == VALIDATED and self.currently_active:
            # Missing activate event (d/va)
            return [generate_output_for_event(ACTIVATED)]
        elif self.activation_type == MISSING and self.include_nonstate_changes:
            # Missing validation event (d/m) -- generate a second deactivate event
            # just to mark the inconsistency.
            return [generate_output_for_event(DEACTIVATED)]
        return []

    def _check_on_validation(self, prev_event, generate_output_for_event):
        """Check if an event should be synthesized after a validation event."""
        if self.activation_type == ACTIVATED:
            if prev_event.is_active and self.include_nonstate_changes:
                # Missing deactivate (va/a)
                return [generate_output_for_event(DEACTIVATED)]
        elif self.activation_type == DEACTIVATED:
            if not prev_event.is_active and self.include_nonstate_changes:
                # Missing activate (vi/d)
                return [generate_output_for_event(ACTIVATED)]
        elif self.activation_type == VALIDATED:
            if prev_event.is_active and not self.currently_active:
                # Missing deactivate (va/vi)
                return [generate_output_for_event(DEACTIVATED)]
            elif not prev_event.is_active and self.currently_active:
                # Missing activate (vi/va)
                return [generate_output_for_event(ACTIVATED)]
        return []

    def _check_earliest_event(self, reason):
        """Check if an event should be synthesized before the first real event."""
        curr = self.activation_timestamp

        if self.activation_type == ACTIVATED:
            pass  # normal case
        elif self.activation_type == DEACTIVATED:
            # If we had a validation after the deactivation,
            # and it provided a creation_timestamp within the interval,
            # then there should be an activate within the interval.
            if self.creation_timestamp and (
                    self.generate_before or
                    self.creation_timestamp >= self.lower_bound_date_string):
                timestamp = self._truncate_timestamp(self.creation_timestamp)
                return [(self.generate_output(
                    timestamp, ACTIVATED, self.current_mode, reason, self.creation_timestamp, curr
                ))]
            elif self.generate_before:
                # For now, hack the timestamp by making it a little before the deactivate,
                # so that it at least has a value.
                timestamp = self._get_fake_timestamp(None, curr)
                return [(self.generate_output(timestamp, ACTIVATED, self.current_mode, reason, None, curr))]

        elif self.activation_type == VALIDATED:
            creation_timestamp = self._truncate_timestamp(self.creation_timestamp)

            if not self.generate_before and self.creation_timestamp < self.lower_bound_date_string:
                # If we are validating only within an interval and the create_timestamp
                # is outside this interval, we can't know whether the events are really
                # missing or just not included.
                pass
            elif self.currently_active:
                return [(self.generate_output(
                    creation_timestamp, ACTIVATED, self.current_mode, reason, self.creation_timestamp, curr
                ))]
            elif self.include_nonstate_changes:
                # There may be missing Activate and Deactivate events, or there may
                # just be an inactive table row that was created as part of an enrollment
                # flow, but no enrollment was completed.
                event1 = self.generate_output(
                    creation_timestamp, ACTIVATED, self.current_mode, reason, self.creation_timestamp, curr
                )
                timestamp = self._get_fake_timestamp(creation_timestamp, curr)
                event2 = self.generate_output(
                    timestamp, DEACTIVATED, self.current_mode, reason, self.creation_timestamp, curr
                )
                return [event1, event2]
        return []

    def _check_event(self, prev_event):
        """Compare a previous event with current state generated from later events. """
        prev_timestamp_for_mode_change = prev_event.timestamp

        missing = []
        if self.activation_type is not None:
            reason = self._get_reason_string(prev_event, self.activation_state)
            timestamp = self._get_fake_timestamp(prev_event.timestamp, self.activation_timestamp)

            def generate_output_for_event(event_type):
                """Wrapper to generate a synthetic event with common values."""
                return self.generate_output(
                    timestamp, event_type, prev_event.mode, reason, prev_event.timestamp, self.activation_timestamp
                )

            prev_type = prev_event.event_type
            if prev_type == ACTIVATED:
                missing.extend(self._check_on_activated(generate_output_for_event))
            elif prev_type == DEACTIVATED:
                missing.extend(self._check_on_deactivated(generate_output_for_event))
            elif prev_type == VALIDATED:
                missing.extend(self._check_on_validation(prev_event, generate_output_for_event))
            elif prev_type == SENTINEL:
                missing.extend(self._check_earliest_event(reason))

            if missing:
                prev_timestamp_for_mode_change = timestamp

        # Check for mode change for all events:
        missing.extend(self._check_for_mode_change(prev_event, prev_timestamp_for_mode_change))

        # Finally, set state for the next one.
        self._update_state(prev_event)

        return missing


class CourseEnrollmentValidationPerDateTask(
        CourseEnrollmentValidationDownstreamMixin, MultiOutputMapReduceJobTask):
    """
    Outputs CourseEnrollmentValidationTask according to key (i.e. datestamp).

    """

    intermediate_output = luigi.Parameter(
        default=None,
        description='A URL for the location to write intermediate output.',
    )

    def __init__(self, *args, **kwargs):
        super(CourseEnrollmentValidationPerDateTask, self).__init__(*args, **kwargs)

        # Update intermediate_output if not present.
        if self.intermediate_output is None:
            self.intermediate_output = url_path_join(self.output_root, "all-events")

    def requires(self):
        return CourseEnrollmentValidationTask(
            mapreduce_engine=self.mapreduce_engine,
            lib_jar=self.lib_jar,
            n_reduce_tasks=self.n_reduce_tasks,
            interval=self.interval,
            source=self.source,
            pattern=self.pattern,
            output_root=self.intermediate_output,
            tuple_output=self.tuple_output,
            generate_before=self.generate_before,
            include_nonstate_changes=self.include_nonstate_changes,
            earliest_timestamp=self.earliest_timestamp,
            expected_validation=self.expected_validation,
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
        if not self.tuple_output:
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

    The date for the synthesized events is the end time of the Sqoop dump.  This
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
    source_dir = luigi.Parameter(
        description='The URL of the location of the desired database dump.  This should '
        'include the "dt=<date>" partition specification.',
    )

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
        metadata_path = url_path_join(self.source_dir, METADATA_FILENAME)
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

    def output_path_for_key(self, encoded_course_id):
        course_id = encoded_course_id.decode('utf-8')
        filename_safe_course_id = opaque_key_util.get_filename_safe_course_id(course_id, '_')
        filename = u'{course_id}_enroll_validated_{dumpdate}.log.gz'.format(
            course_id=filename_safe_course_id,
            dumpdate=self.dump_date,
        )
        return url_path_join(self.output_root, filename)


class CreateEnrollmentValidationEventsForTodayTask(CreateEnrollmentValidationEventsTask):
    """Task that makes sure there's a dump of course enrollment for today before making events."""

    credentials = luigi.Parameter(default=None)

    def requires_hadoop(self):
        # Instead of just pointing to the output directory of a dump, let's make sure
        # there is a dump.
        # We don't have a way to just dump the Mysql table, so deal with the Hive table
        # definition as well.
        yield ImportStudentCourseEnrollmentTask(credentials=self.credentials)

    def input_hadoop(self):
        # The requires() method will return the Hive target, but really we need the
        # file dump instead.
        yield ExternalURL(self.source_dir).output()


class CreateAllEnrollmentValidationEventsTask(WarehouseMixin, MapReduceJobTaskMixin, luigi.WrapperTask):
    """
    Task to create enrollment validation events over an interval.

    Scans all dates in the specified interval, and checks if there is
    a courseenrollment dump for that date but no corresponding validation events.
    If so, it generates validation events from the courseenrollment dump.

    """
    interval = luigi.DateIntervalParameter(
        description='Date interval (specified as yyyy-mm-dd-yyyy-mm-dd) over which to check.',
    )
    output_root = luigi.Parameter(
        description='Root directory to which to write validation events.  Actual '
        'output files get written to a subdirectory named with the dump date (yyyy-mm-dd).',
    )
    credentials = luigi.Parameter(
        default=None,
        description='Path to the external access credentials file, if performing a dump. '
        'Default is to use value from config file for database-import.',
    )

    required_tasks = None

    def _get_required_tasks(self):
        """Internal method to actually calculate required tasks once."""
        start_date = self.interval.date_a  # pylint: disable=no-member
        end_date = self.interval.date_b  # pylint: disable=no-member
        table_name = "student_courseenrollment"
        source_root = url_path_join(self.warehouse_path, table_name)
        today_datestring = datetime.datetime.utcnow().strftime('%Y-%m-%d')
        current_date = start_date
        while current_date <= end_date:
            datestring = current_date.strftime('%Y-%m-%d')
            current_date += datetime.timedelta(days=1)
            src_datestring = "dt={}".format(datestring)
            source_dir = url_path_join(source_root, src_datestring)
            target = get_target_from_url(source_dir)
            output_dir = url_path_join(self.output_root, datestring)
            if datestring == today_datestring:
                yield CreateEnrollmentValidationEventsForTodayTask(
                    source_dir=source_dir,
                    output_root=output_dir,
                    n_reduce_tasks=self.n_reduce_tasks,
                    credentials=self.credentials,
                )
            elif target.exists():
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


class EnrollmentValidationWorkflow(CourseEnrollmentValidationPerDateTask):
    """
    Performs validation of enrollment data over an interval of time.

    Makes sure that a recent dump has been performed, if necessary,
    and that enrollment validation events have been created.

    """

    validation_root = luigi.Parameter(
        config_path={'section': 'enrollment-validation', 'name': 'validation_root'}
    )
    validation_pattern = luigi.Parameter(
        config_path={'section': 'enrollment-validation', 'name': 'validation_pattern'}
    )
    credentials = luigi.Parameter(default=None)

    def requires(self):
        # we need to add the CreateAllEnrollmentValidationEventsTask to the
        # requirements of the base task.
        for requirement in luigi.task.flatten(super(EnrollmentValidationWorkflow, self).requires()):
            yield requirement

        yield CreateAllEnrollmentValidationEventsTask(
            output_root=self.validation_root,
            interval=self.interval,
            credentials=self.credentials,
            n_reduce_tasks=self.n_reduce_tasks,
        )

    def _append_value_to_tuple(self, source_tuple, value):
        """Converts tuple to list, appends, and converts back."""
        source_list = [source for source in source_tuple]
        source_list.append(value)
        return tuple(source_list)

    def __init__(self, *args, **kwargs):
        super(EnrollmentValidationWorkflow, self).__init__(*args, **kwargs)

        # Update source and pattern with special values to include
        # validation events.  Values here are tuples, not lists.
        self.source = self._append_value_to_tuple(self.source, self.validation_root)
        self.pattern = self._append_value_to_tuple(self.pattern, self.validation_pattern)
