"""EXPERIMENTAL:

Load most grade events into warehouse for internal reporting purposes.
This combines tracking log events from regular and worker event logs,
and defines a common (wide) representation for all grading events to
share, sparsely.  Requires definition of a Record enumerating these
columns, and also a mapping from event values to column values.

"""
import datetime
import logging
import os
import re
from importlib import import_module

import ciso8601
import dateutil
import luigi
import luigi.task
import pytz
from luigi.configuration import get_config
from luigi.date_interval import DateInterval

from edx.analytics.tasks.common.bigquery_load import BigQueryLoadDownstreamMixin, BigQueryLoadTask
from edx.analytics.tasks.common.mapreduce import MapReduceJobTaskMixin, MultiOutputMapReduceJobTask
from edx.analytics.tasks.common.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.common.vertica_load import SchemaManagementTask, VerticaCopyTask, VerticaCopyTaskMixin
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.hive import BareHiveTableTask, HivePartition, HivePartitionTask, WarehouseMixin
from edx.analytics.tasks.util.obfuscate_util import backslash_encode_value
from edx.analytics.tasks.util.opaque_key_util import get_org_id_for_course
from edx.analytics.tasks.util.record import (
    BooleanField, DateField, DateTimeField, FloatField, IntegerField, SparseRecord, StringField
)
from edx.analytics.tasks.util.url import ExternalURL, url_path_join

log = logging.getLogger(__name__)


class GradingEventRecord(SparseRecord):
    """Represents a grading event."""

    timestamp = DateTimeField(nullable=True, description='Timestamp when event was emitted.')
    received_at = DateTimeField(nullable=True, description='Timestamp when event was received.')
    user_id = StringField(length=255, nullable=True, description='The identifier of the user who was logged in when the event was emitted. '
                          'This is often but not always numeric.')
    event_type = StringField(
        length=255,
        nullable=False,
        description='The name of the event (event name in the segment logs).',
    )
    course_id = StringField(length=255, nullable=True, description='The course_id associated with this event (if any).')
    org_id = StringField(length=255, nullable=True, description='Id of organization, as used in course_id.')

    block_id = StringField(length=255, nullable=True, description='')

    percent_grade = StringField(length=256, nullable=True, description='blah.')
    letter_grade = StringField(length=256, nullable=True, description='blah.')
    weighted_possible = StringField(length=256, nullable=True, description='blah.')
    weighted_earned = StringField(length=256, nullable=True, description='blah.')
    weighted_graded_possible = StringField(length=256, nullable=True, description='blah.')
    weighted_graded_earned = StringField(length=256, nullable=True, description='blah.')
    edited_timestamp = StringField(length=256, nullable=True, description='blah.')
    event_transaction_type = StringField(length=256, nullable=True, description='blah.')
    event_transaction_id = StringField(length=256, nullable=True, description='blah.')

    # Other stuff added for completeness.
    instructor_id = StringField(length=256, nullable=True, description='blah.')
    grading_policy_hash = StringField(length=256, nullable=True, description='blah.')
    course_version = StringField(length=256, nullable=True, description='blah.')
    only_if_higher = StringField(length=256, nullable=True, description='blah.')
    override_deleted = StringField(length=256, nullable=True, description='blah.')
    first_attempted = StringField(length=256, nullable=True, description='blah.')
    visible_blocks_hash = StringField(length=256, nullable=True, description='blah.')

    date = DateField(nullable=False, description='The date when the event was received.')
    input_date = DateField(nullable=True, description='The date in the input filename.')
    input_timestamp = IntegerField(nullable=True, description='The unix timestamp in the input filename, if any.')
    input_timestamp_datetime = DateTimeField(nullable=True, description='Timestamp in the input filename, if any, converted from unix to timestamp.')


class EventRecordClassMixin(object):

    event_table_name = luigi.Parameter(
        description='The kind of event record to load.',
        default='mit_grading_events',
    )

    event_record_type = luigi.Parameter(
        description='The kind of event record to load.',
        default='GradingEventRecord',
    )

    def __init__(self, *args, **kwargs):
        super(EventRecordClassMixin, self).__init__(*args, **kwargs)
        module_name = self.__class__.__module__
        local_module = import_module(module_name)
        self.record_class = getattr(local_module, self.event_record_type)
        if not self.record_class:
            raise ValueError("No event record class found:  {}".format(self.event_record_type))

    def get_event_record_class(self):
        return self.record_class


class EventRecordDownstreamMixin(EventRecordClassMixin, WarehouseMixin, MapReduceJobTaskMixin):
    pass


class EventRecordDataDownstreamMixin(EventRecordDownstreamMixin):

    """Common parameters and base classes used to pass parameters through the event record workflow."""
    output_root = luigi.Parameter(
        description='Root directory where to write event records.',
        default=None,
    )

    def __init__(self, *args, **kwargs):
        super(EventRecordDataDownstreamMixin, self).__init__(*args, **kwargs)
        if self.output_root is None:
            self.output_root = self.warehouse_path


class BaseEventRecordDataTask(EventRecordDataDownstreamMixin, MultiOutputMapReduceJobTask):
    """Base class for loading EventRecords from different sources."""

    # Create a DateField object to help with converting date_string
    # values for assignment to DateField objects.
    date_field_for_converting = DateField()
    date_time_field_for_validating = DateTimeField()

    # This is a placeholder.  It is expected to be overridden in derived classes.
    counter_category_name = 'Event Record Exports'

    # TODO: maintain support for info about events.  We may need something similar to identify events
    # that should -- or should not -- be included in the event dump.

    def multi_output_reducer(self, _key, values, output_file):
        """
        Write values to the appropriate file as determined by the key.
        """
        for value in values:
            # Assume that the value is a dict containing the relevant sparse data,
            # either raw or encoded in a json string.
            # Either that, or we could ship the original event as a json string,
            # or ship the resulting sparse record as a tuple.
            # It should be a pretty arbitrary decision, since it all needs
            # to be done, and it's just a question where to do it.
            # For now, keep this simple, and assume it's tupled already.
            output_file.write(value)
            output_file.write('\n')
            # WARNING: This line ensures that Hadoop knows that our process is not sitting in an infinite loop.
            # Do not remove it.
            self.incr_counter(self.counter_category_name, 'Raw Bytes Written', len(value) + 1)

    def output_path_for_key(self, key):
        """
        Output based on date and project.

        Mix them together by date, but identify with different files for each project/environment.

        Output is in the form {warehouse_path}/event_records/dt={CCYY-MM-DD}/{project}.tsv
        """
        date_received, project = key

        return url_path_join(
            self.output_root,
            self.event_table_name,
            'dt={date}'.format(date=date_received),
            '{project}.tsv'.format(project=project),
        )

    def extra_modules(self):
        return [pytz, dateutil]

    def normalize_time(self, event_time):
        """
        Convert time string to ISO-8601 format in UTC timezone.

        Returns None if string representation cannot be parsed.
        """
        datetime = ciso8601.parse_datetime(event_time)
        if datetime:
            return datetime.astimezone(pytz.utc).isoformat()
        else:
            return None

    def extended_normalize_time(self, event_time):
        """
        Convert time string to ISO-8601 format in UTC timezone.

        Returns None if string representation cannot be parsed.
        """
        datetime = dateutil.parser.parse(event_time)
        if datetime:
            return datetime.astimezone(pytz.utc).isoformat()
        else:
            return None

    def convert_date(self, date_string):
        """Converts date from string format to date object, for use by DateField."""
        if date_string:
            try:
                # TODO: for now, return as a string.
                # When actually supporting DateField, then switch back to date.
                # ciso8601.parse_datetime(ts).astimezone(pytz.utc).date().isoformat()
                return self.date_field_for_converting.deserialize_from_string(date_string).isoformat()
            except ValueError:
                self.incr_counter(self.counter_category_name, 'Cannot convert to date', 1)
                # Don't bother to make sure we return a good value
                # within the interval, so we can find the output for
                # debugging.  Should not be necessary, as this is only
                # used for the column value, not the partitioning.
                return u"BAD: {}".format(date_string)
                # return self.lower_bound_date_string
        else:
            self.incr_counter(self.counter_category_name, 'Missing date', 1)
            return date_string

    def _add_event_entry(self, event_dict, event_record_key, event_record_field, label, obj):
        if isinstance(event_record_field, StringField):
            if obj is None:
                # TODO: this should really check to see if the record_field is nullable.
                value = None
            else:
                value = backslash_encode_value(unicode(obj))
                if '\x00' in value:
                    value = value.replace('\x00', '\\0')
                # Avoid validation errors later due to length by truncating here.
                field_length = event_record_field.length
                value_length = len(value)
                # TODO: This implies that field_length is at least 4.
                if value_length > field_length:
                    log.error("Record value length (%d) exceeds max length (%d) for field %s: %r", value_length, field_length, event_record_key, value)
                    value = u"{}...".format(value[:field_length - 4])
                    self.incr_counter(self.counter_category_name, 'Quality Truncated string value', 1)
            event_dict[event_record_key] = value
        elif isinstance(event_record_field, IntegerField):
            try:
                event_dict[event_record_key] = int(obj)
            except ValueError:
                log.error('Unable to cast value to int for %s: %r', label, obj)
        elif isinstance(event_record_field, DateTimeField):
            datetime_obj = None
            try:
                if obj is not None:
                    datetime_obj = ciso8601.parse_datetime(obj)
                    if datetime_obj.tzinfo:
                        datetime_obj = datetime_obj.astimezone(pytz.utc)
                else:
                    datetime_obj = obj
            except ValueError:
                log.error('Unable to cast value to datetime for %s: %r', label, obj)

            # Because it's not enough just to create a datetime object, also perform
            # validation here.
            if datetime_obj is not None:
                validation_errors = self.date_time_field_for_validating.validate(datetime_obj)
                if len(validation_errors) > 0:
                    log.error('Invalid assigment of value %r to field "%s": %s', datetime_obj, label, ', '.join(validation_errors))
                    datetime_obj = None

            event_dict[event_record_key] = datetime_obj
        elif isinstance(event_record_field, DateField):
            date_obj = None
            try:
                if obj is not None:
                    date_obj = self.date_field_for_converting.deserialize_from_string(obj)
            except ValueError:
                log.error('Unable to cast value to date for %s: %r', label, obj)

            # Because it's not enough just to create a datetime object, also perform
            # validation here.
            if date_obj is not None:
                validation_errors = self.date_field_for_converting.validate(date_obj)
                if len(validation_errors) > 0:
                    log.error('Invalid assigment of value %r to field "%s": %s', date_obj, label, ', '.join(validation_errors))
                    date_obj = None

            event_dict[event_record_key] = date_obj
        elif isinstance(event_record_field, BooleanField):
            try:
                event_dict[event_record_key] = bool(obj)
            except ValueError:
                log.error('Unable to cast value to bool for %s: %r', label, obj)
        elif isinstance(event_record_field, FloatField):
            try:
                event_dict[event_record_key] = float(obj)
            except ValueError:
                log.error('Unable to cast value to float for %s: %r', label, obj)
        else:
            event_dict[event_record_key] = obj

    def _add_event_info_recurse(self, event_dict, event_mapping, obj, label):
        if obj is None:
            pass
        elif isinstance(obj, dict):
            for key in obj.keys():
                new_value = obj.get(key)
                # Normalize labels to be all lower-case, since all field (column) names are lowercased.
                new_label = u"{}.{}".format(label, key.lower())
                self._add_event_info_recurse(event_dict, event_mapping, new_value, new_label)
        elif isinstance(obj, list):
            # We will not output any values that are stored in lists.
            pass
        else:
            # We assume it's a single object, and look it up now.
            if label in event_mapping:
                event_record_key, event_record_field = event_mapping[label]
                self._add_event_entry(event_dict, event_record_key, event_record_field, label, obj)

    def add_event_info(self, event_dict, event_mapping, event):
        self._add_event_info_recurse(event_dict, event_mapping, event, 'root')

    def add_calculated_event_entry(self, event_dict, event_record_key, obj):
        """Use this to explicitly add calculated entry values."""
        event_record_field = self.get_event_record_class().get_fields()[event_record_key]
        label = event_record_key
        self._add_event_entry(event_dict, event_record_key, event_record_field, label, obj)


class GradingEventRecordDataTask(EventLogSelectionMixin, BaseEventRecordDataTask):
    """Task to compute event_type and event_source values being encountered on each day in a given time interval."""

    # Override superclass to disable this parameter
    event_mapping = None

    counter_category_name = 'Grading Event Exports'

    def get_event_emission_time(self, event):
        return super(GradingEventRecordDataTask, self).get_event_time(event)

    def get_event_arrival_time(self, event):
        try:
            return event['context']['received_at']
        except KeyError:
            return self.get_event_emission_time(event)
        except TypeError:
            return self.get_event_emission_time(event)

    def get_event_time(self, event):
        # Some events may emitted and stored for quite some time before actually being entered into the tracking logs.
        # The primary cause of this is mobile devices that go offline for a significant period of time. They will store
        # events locally and then when connectivity is restored transmit them to the server. We log the time that they
        # were received by the server and use that to batch them into exports since it is much simpler than trying to
        # inject them into past exports.
        return self.get_event_arrival_time(event)

    def get_event_mapping(self):
        """Return dictionary of event attributes to the output keys they map to."""
        if self.event_mapping is None:
            self.event_mapping = {}
            fields = self.get_event_record_class().get_fields()
            field_keys = fields.keys()
            for field_key in field_keys:
                field_tuple = (field_key, fields[field_key])

                def add_event_mapping_entry(source_key):
                    self.event_mapping[source_key] = field_tuple

                # Most common is to map first-level entries in event data directly.
                # Skip values that are explicitly calculated rather than copied:
                if field_key in ['event_type', 'timestamp', 'received_at', 'date', 'course_id', 'org_id']:
                    pass

                # Collapse a few values together into a single column.
                elif field_key == 'block_id':
                    add_event_mapping_entry('root.event.block_id')
                    add_event_mapping_entry('root.event.problem_id')

                elif field_key == 'weighted_possible':
                    add_event_mapping_entry('root.event.weighted_total_possible')
                    add_event_mapping_entry('root.event.weighted_possible')
                    add_event_mapping_entry('root.event.new_weighted_possible')

                elif field_key == 'weighted_earned':
                    add_event_mapping_entry('root.event.weighted_total_earned')
                    add_event_mapping_entry('root.event.weighted_earned')
                    add_event_mapping_entry('root.event.new_weighted_earned')

                elif field_key == 'edited_timestamp':
                    add_event_mapping_entry('root.event.course_edited_timestamp')
                    add_event_mapping_entry('root.event.subtree_edited_timestamp')

                else:
                    # user_id
                    # percent_grade
                    # letter_grade
                    # weighted_graded_possible
                    # weighted_graded_earned
                    # event_transaction_type
                    # event_transaction_id

                    # We also add for completeness:
                    # instructor_id
                    # grading_policy_hash
                    # course_version
                    # only_if_higher
                    # override_deleted
                    # first_attempted
                    # visible_blocks_hash
                    add_event_mapping_entry(u"root.event.{}".format(field_key))

        return self.event_mapping

    def get_timestamps_from_input_filename(self):
        """Get various date and timestamp information from the input_file."""
        input_file = self.get_map_input_file()
        PATTERN = ".*\\.log-(?P<date>\\d{8})(\\-(?P<timestamp>\\d{10}))?\\.gz$"
        DATE_PATTERN = '%Y%m%d'
        input_filename = os.path.basename(input_file.strip())
        match = re.match(PATTERN, input_filename)
        parsed_date = None
        timestamp = None
        timestamp_datetime = None
        if match:
            if 'date' in match.groupdict():
                parsed_datetime = datetime.datetime.strptime(match.group('date'), DATE_PATTERN)
                parsed_date = datetime.date(parsed_datetime.year, parsed_datetime.month, parsed_datetime.day)

            if 'timestamp' in match.groupdict():
                timestamp_val = match.group('timestamp')
                if timestamp_val:
                    timestamp = int(timestamp_val)
                    timestamp_datetime = datetime.datetime.utcfromtimestamp(timestamp)
        return parsed_date, timestamp, timestamp_datetime

    def mapper(self, line):
        # skip over non-grading events:
        if 'edx.grades' not in line:
            return

        event, date_received = self.get_event_and_date_string(line) or (None, None)
        if event is None:
            return
        self.incr_counter(self.counter_category_name, 'Inputs with Dates', 1)

        event_type = event.get('event_type')
        if event_type is None:
            self.incr_counter(self.counter_category_name, 'Discard Missing Event Type', 1)
            return

        # Handle events that begin with a slash (i.e. implicit events).
        # * For JSON events, give them a marker event_type so we can more easily search (or filter) them,
        #   and treat the type as the URL that was called.
        # * For regular events, ignore those that begin with a slash (i.e. implicit events).
        event_url = None
        if event_type.startswith('/'):
            self.incr_counter(self.counter_category_name, 'Discard Implicit Events', 1)
            return

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            self.incr_counter(self.counter_category_name, 'Discard Missing Event Data', 1)
            return
        # Put the fixed value back, so it can be properly mapped.
        event['event'] = event_data

        # We always get the course_id from the event, not from context.
        # (It's not always in context.)
        # Then try to extract the org from course_id, since we're using
        # that to do filtering (for now).

        course_id = event_data.get('course_id')
        org_id = None
        if course_id is not None:
            org_id = get_org_id_for_course(course_id)

        # Hardcode a check here for now, to limit to one organization.
        if not org_id or org_id.lower() != 'mitx':
            self.incr_counter(self.counter_category_name, 'Discard Different Org', 1)
            return

        # Get input file timestamp, so we can also check lag time on grading
        # events.
        input_date, input_timestamp, input_timestamp_datetime = self.get_timestamps_from_input_filename()

        # Add calculated values.
        event_dict = {}
        self.add_calculated_event_entry(event_dict, 'event_type', event_type)
        self.add_calculated_event_entry(event_dict, 'timestamp', self.get_event_emission_time(event))
        self.add_calculated_event_entry(event_dict, 'received_at', self.get_event_arrival_time(event))
        self.add_calculated_event_entry(event_dict, 'date', self.convert_date(date_received))
        self.add_calculated_event_entry(event_dict, 'course_id', course_id)
        if org_id:
            self.add_calculated_event_entry(event_dict, 'org_id', org_id)
        if input_date:
            self.add_calculated_event_entry(event_dict, 'input_date', input_date.isoformat())
        if input_timestamp:
            self.add_calculated_event_entry(event_dict, 'input_timestamp', input_timestamp)
        if input_timestamp_datetime:
            self.add_calculated_event_entry(event_dict, 'input_timestamp_datetime', input_timestamp_datetime.isoformat())

        # Map remaining values.
        event_mapping = self.get_event_mapping()
        self.add_event_info(event_dict, event_mapping, event)

        record = self.get_event_record_class()(**event_dict)
        key = (date_received, 'grading_events')

        self.incr_counter(self.counter_category_name, 'Output From Mapper', 1)

        # Convert to form for output by reducer here,
        # so that reducer doesn't do any conversion.
        # yield key, record.to_string_tuple()
        yield key, record.to_separated_values()


##########################
# Bulk Loading into S3
##########################

class BulkGradingEventRecordIntervalTask(EventRecordDownstreamMixin, luigi.WrapperTask):
    """Compute event information over a range of dates and insert the results into Hive."""

    interval = luigi.DateIntervalParameter(
        description='The range of dates for which to create event records.',
    )

    def requires(self):
        kwargs = {
            'output_root': self.warehouse_path,
            'n_reduce_tasks': self.n_reduce_tasks,
            'interval': self.interval,
            'event_record_type': self.event_record_type,
            'event_table_name': self.event_table_name,
        }
        yield (
            GradingEventRecordDataTask(**kwargs),
        )

    def output(self):
        return [task.output() for task in self.requires()]


##########################
# Loading into BigQuery
##########################

class LoadDailyGradingEventRecordToBigQuery(EventRecordDownstreamMixin, BigQueryLoadTask):

    @property
    def table(self):
        return self.event_table_name

    @property
    def partitioning_type(self):
        """Set to 'DAY' in order to partition by day."""
        return 'DAY'

    @property
    def schema(self):
        return self.get_event_record_class().get_bigquery_schema()

    @property
    def insert_source_task(self):
        return ExternalURL(url=self.hive_partition_path(self.event_table_name, self.date))


class LoadGradingEventRecordIntervalToBigQuery(EventRecordDownstreamMixin, BigQueryLoadDownstreamMixin, luigi.WrapperTask):
    """
    Loads the event records table from Hive into the BigQuery data warehouse.

    """

    interval = luigi.DateIntervalParameter(
        description='The range of dates for which to create event records.',
    )

    def requires(self):
        for date in reversed([d for d in self.interval]):  # pylint: disable=not-an-iterable
            # should_overwrite = date >= self.overwrite_from_date
            yield LoadDailyGradingEventRecordToBigQuery(
                event_record_type=self.event_record_type,
                event_table_name=self.event_table_name,
                date=date,
                n_reduce_tasks=self.n_reduce_tasks,
                warehouse_path=self.warehouse_path,
                overwrite=self.overwrite,
                dataset_id=self.dataset_id,
                credentials=self.credentials,
                max_bad_records=self.max_bad_records,
            )

    def output(self):
        return [task.output() for task in self.requires()]

    def complete(self):
        # OverwriteOutputMixin changes the complete() method behavior, so we override it.
        return all(r.complete() for r in luigi.task.flatten(self.requires()))
