"""
Luigi tasks for extracting problem answer distribution statistics from
tracking log files.
"""
import csv
import datetime
import hashlib
import json
from operator import itemgetter

import html5lib
import luigi
import luigi.hdfs
import luigi.s3
from luigi.configuration import get_config

from edx.analytics.tasks.common.mapreduce import MapReduceJobTask, MultiOutputMapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.common.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.common.mysql_load import MysqlInsertTask, MysqlInsertTaskMixin
from edx.analytics.tasks.insights.answer_dist import get_text_from_html, try_str_to_float
import edx.analytics.tasks.util.eventlog as eventlog
import edx.analytics.tasks.util.opaque_key_util as opaque_key_util
from edx.analytics.tasks.util.url import get_target_from_url, url_path_join

# TODO: move MultipartitionHiveTableTask to util.hive, and clean these out...
import textwrap
from luigi.hive import HiveTableTarget, HivePartitionTarget
# from edx.analytics.tasks.util.datetime_util import all_dates_between
from edx.analytics.tasks.util.hive import HiveTableTask, WarehouseMixin, HivePartition, hive_database_name

import logging
log = logging.getLogger(__name__)


################################
# Task Map-Reduce definitions
################################

UNKNOWN_ANSWER_VALUE = ''
UNMAPPED_ANSWER_VALUE = ''
PROBLEM_CHECK_EVENT = 'problem_check'


class AllProblemCheckEventsParamMixin(EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin):
    """Parameters for AllProblemCheckEventsTask."""


class AllProblemCheckEventsTask(AllProblemCheckEventsParamMixin, EventLogSelectionMixin, MultiOutputMapReduceJobTask):
    """Identifies last problem_check event for a user on a problem in a course, given raw event log input."""

    # TODO: set this up to use warehouse_path instead...
    output_root = luigi.Parameter()

    def mapper(self, line):
        """
        Generates output values for explicit problem_check events.

        Args:
            line: text line from a tracking event log.

        Returns:
            (problem_id, username), (timestamp, problem_check_info)

            where timestamp is in ISO format, with resolution to the millisecond

            and problem_check_info is a JSON-serialized dict
            containing the contents of the problem_check event's
            'event' field, augmented with entries for 'timestamp',
            'username', and 'context' from the event.

            or None if there is no valid problem_check event on the line.

        Example:
                (i4x://edX/DemoX/Demo_Course/problem/PS1_P1, dummy_username), (2013-09-10T00:01:05.123456, blah)

        """
        parsed_tuple_or_none = self.get_problem_check_event(line)
        if parsed_tuple_or_none is not None:
            yield parsed_tuple_or_none

    def get_problem_check_event(self, line):
        # Add a quick short-circuit to avoid more expensive parsing of most events.
        if PROBLEM_CHECK_EVENT not in line:
            return None

        event, date_string = self.get_event_and_date_string(line) or (None, None)
        if event is None:
            return None

        event_type = event.get('event_type')
        if event_type is None:
            log.error("encountered event with no event_type: %s", event)
            return None
        if event_type != PROBLEM_CHECK_EVENT:
            return None

        event_source = event.get('event_source')
        if event_source is None:
            log.error("encountered event with no event_source: %s", event)
            return None
        if event_source != 'server':
            return None

        # Get the "problem data".  This is the event data, the context, and anything else that would
        # be useful further downstream.
        # TODO: make the timestamp logic more efficient than this (i.e. avoid the parse).
        augmented_data_fields = ['context', 'timestamp']
        problem_data = eventlog.get_augmented_event_data(event, augmented_data_fields)
        if problem_data is None:
            return None

        # Get the course_id from context.  We won't work with older events
        # that do not have context information, since they do not directly
        # provide course_id information.  (The problem_id/answer_id values
        # contain the org and course name, but not the run.)  Course_id
        # information could be found from other events, but it would
        # require expanding the events being selected.
        course_id = problem_data.get('context').get('course_id')
        if course_id is None:
            log.error("encountered explicit problem_check event with missing course_id: %s", event)
            return None

        if not opaque_key_util.is_valid_course_id(course_id):
            log.error("encountered explicit problem_check event with bogus course_id: %s", event)
            return None

        # There was a short period of time where context contained course_id but no user_id.
        # We will be requiring the user_id as well.
        user_id = problem_data.get('context').get('user_id')
        if user_id is None:
            return None

        problem_data_json = json.dumps(problem_data)
        key = (date_string, course_id)
        value = (problem_data_json,)

        return key, value

    def extra_modules(self):
        import six
        return [html5lib, six]

    def output_path_for_key(self, key):
        """
        Creates separate folders by date, so that processing can be performed
        incrementally.
        """
        date_string, course_id = key
        date_directory = "dt={0}".format(date_string)
        filename_safe_course_id = opaque_key_util.get_filename_safe_course_id(course_id, '_')
        filename = u'{course_id}_answers.dat'.format(course_id=filename_safe_course_id)
        return url_path_join(self.output_root, date_directory, filename)

    def multi_output_reducer(self, _key, values, output_file):
        """
        Calculate a list of answers from the final response of a user to a problem in a course.

        Args:
            key:  (date_string, course_id)
            values:  iterator of (problem_check_info)

        Outputs:

        TODO: UPDATE THIS!

            list of answer data tuples, where a tuple consists of:

                (course_id, answer_id), (timestamp, answer_data)

            where answer_data is NO LONGER a json-encoded dict, containing:

              'problem_id': the id of the problem (i4x)
              'problem_display_name': the display name for the problem
              'answer': if an event with 'submission' information,
                 this is the text of the answer.  For events with no
                 'submission' information, this is not defined.
              'answer_value_id': if an event with 'submission'
                 information, this is the moniker for the answer, and
                 is not defined if there is no moniker.  For events
                 with no 'submission' information, this holds either
                 the moniker (if used) or the answer (if no moniker is
                 used).
              'question': the display text for the problem part being answered, if available.
              'correct': boolean if the answer is correct.
              'variant': seed value

        """

        for event_string in values:
            for answer in self._generate_answers(event_string):
                self._output_answer(answer, output_file)

    COLUMN_NAMES = [
        'time',
        # Info about problem:
        'course_id',
        'problem_id',
        'part_id',
        'problem_display_name',
        'question',
        # Info about user:
        'user_id',
        'course_user_tags',
        'variant',
        # Info about answer:
        'correct',
        'answer',
        'answer_value_id',
        'answer_uses_value_id',
        'grouping_key',
        'uses_submission',
        'should_include_answer',
    ]

    def _output_answer(self, answer, output_file):
        """
        Outputs answer dict in columnar format for input to Hive.

        An example answer dict:
        {'course_user_tags': {}, 'answer_value_id': u'4', 'uses_submission': False, 'user_id': 14,
         'timestamp': u'2014-02-24T20:19:47.840908', 'grouping_key': u'4_1', 'variant': 1,
         'problem_id': u'i4x://MITx/7.00x/problem/PSet1:PS1_Q1', 'part_id': u'i4x-MITx-7_00x-problem-PSet1_PS1_Q1_5_1',
         'course_id': u'MITx/7.00x/2013_Spring', 'answer_uses_value_id': False, 'problem_display_name': None,
         'correct': True}
        }
        """
        # TODO: figure out how to encode results that have embedded tabs, or use a different delimiter.
        # Mostly 'question', but maybe 'problem_display_name'.
        # Also, course_user_tags is just a string representation of a dict.  Placeholder....
        # Yuck.  It won't work like this.  There are embedded newlines, and lots of non-ascii characters.
        # answer_string = '\t'.join([str(answer.get(value, "")) for value in self.COLUMN_NAMES])
        # answer_string = json.dumps(answer)
        answer_string = '\t'.join([answer.get(value, u'').encode('utf-8') for value in self.COLUMN_NAMES])
        output_file.write(answer_string)
        output_file.write("\n")

    def _generate_answers(self, event_string):
        """
        Generates a list of answers given a problem_check event.

        Args:
            event_string:  a json-encoded string version of an event's data.

        Returns:
            list of answer data tuples.

        See docstring for reducer() for more details.
        """
        event_string = event_string[0]
        event = json.loads(event_string)

        # Get context information:
        course_id = event.get('context').get('course_id')
        user_id = event.get('context').get('user_id')
        timestamp = event.get('timestamp')
        problem_id = event.get('problem_id')
        problem_display_name = event.get('context').get('module', {}).get('display_name', None)
        course_user_tags = event.get('context').get('course_user_tags', {})
        results = []

        def append_to_answers(answer_id, answer):
            """Convert submission to result to be returned."""
            # First augment submission with problem-level information
            # not found in the submission:
            answer['time'] = timestamp
            answer['course_id'] = course_id
            answer['user_id'] = user_id
            answer['problem_id'] = problem_id
            answer['problem_display_name'] = problem_display_name
            answer['course_user_tags'] = course_user_tags
            # Some should already be in the dict beforehand:
            # i.e. question, variant, answer_value_id, answer, correct.
            # Some are added:  uses_submission, should_include_answer,
            # answer_uses_value_id, grouping_key.

            answer['grouping_key'] = self.get_answer_grouping_key(answer)
            answer['part_id'] = answer_id

            # TODO: remove embedded newlines here at least, so that downstream
            # processing can safely use newlines as record delimiters.
            # If we did that, we could also do the same for tabs.
            def cleaned(value):
                if isinstance(value, basestring):
                    return value.replace('\t', '\\t').replace('\n', '\\n')
                elif isinstance(value, dict):
                    # Convert 'course_user_tags' to a json string.
                    return json.dumps(value).replace('\t', '\\t').replace('\n', '\\n')
                else:
                    return unicode(value)

            clean_answer = {key: cleaned(answer[key]) for key in answer}
            results.append(clean_answer)

        answers = event.get('answers')
        if 'submission' in event:
            submissions = event.get('submission')
            for answer_id in submissions:
                if not self.is_hidden_answer(answer_id):
                    submission = submissions.get(answer_id)
                    # But submission doesn't contain moniker value for answer.
                    # So we check the raw answers, and see if its value is
                    # different.  If so, we assume it's a moniker.
                    answer_value = answers[answer_id]
                    submission_answer = submission.get('answer')
                    if answer_value != submission_answer:
                        submission['answer_value_id'] = stringify_value(answer_value, contains_html=False)
                        # If we have a moniker, we need to handle HTML in the actual answer.
                        submission['answer'] = stringify_value(submission_answer, contains_html=True)
                        submission['answer_uses_value_id'] = True
                    else:
                        # Answer_value_id is left unset, indicating no moniker.
                        submission['answer'] = stringify_value(submission_answer, contains_html=False)
                        submission['answer_uses_value_id'] = False

                    # Add additional information that is specific to events with submission info:
                    submission['uses_submission'] = True
                    submission['should_include_answer'] = self.should_include_answer(submission)
                    append_to_answers(answer_id, submission)

        else:
            # Otherwise, it's an older event with no 'submission'
            # information, so parse it as well as possible.
            correct_map = event.get('correct_map')
            for answer_id in answers:
                if not self.is_hidden_answer(answer_id):
                    answer_value = answers[answer_id]

                    # Argh. It seems that sometimes we're encountering
                    # bogus answer_id values.  In particular, one that
                    # is including the possible choice values, instead
                    # of any actual values selected by the student.
                    # For now, let's just dump an error and skip it,
                    # so that it becomes the equivalent of a hidden
                    # answer.

                    # TODO: Eventually treat it explicitly as a hidden
                    # answer.
                    if answer_id not in correct_map:
                        log.error("Unexpected answer_id %s not in correct_map: %s", answer_id, event)
                        continue
                    correctness = correct_map[answer_id].get('correctness') == 'correct'

                    variant = event.get('state', {}).get('seed')

                    # We do not know the values for 'input_type',
                    # 'response_type', or 'question'.  We also don't know if
                    # answer_value should be identified as 'answer_value_id' or
                    # 'answer', so we choose to use 'answer_value_id' here and
                    # never define 'answer'.  This allows disambiguation from
                    # events with a submission field, which will always have
                    # an 'answer' and only sometimes have an 'answer_value_id'.
                    answer_info = {
                        'answer_value_id': stringify_value(answer_value),
                        'correct': correctness,
                        'variant': variant,
                        'uses_submission': False,
                        'answer_uses_value_id': False,
                    }
                    append_to_answers(answer_id, answer_info)

        return results

    def is_hidden_answer(self, answer_id):
        """Check Id to identify hidden kinds of values."""
        # some problems have additional answers that have '_dynamath' appended
        # to the regular answer_id.  In this case, the contents seem to contain
        # something like:
        #
        # <math xmlns="http://www.w3.org/1998/Math/MathML">
        #   <mstyle displaystyle="true">
        #     <mo></mo>
        #   </mstyle>
        # </math>
        if answer_id.endswith('_dynamath'):
            return True

        # Others seem to end with _comment, and I don't know yet what these
        # look like.
        if answer_id.endswith('_comment'):
            return True

        return False

    def should_include_answer(self, answer):
        """Determine if a problem "part" should be included in the distribution."""
        response_type = answer.get('response_type')

        # For problems which only have old responses, we don't
        # have information about whether to include their answers.
        if response_type is None:
            return False

        # read out valid types from client.cfg file.  The 3rd argument below sets a default in case the
        # config file is somehow misread.  But to change the list, please update the client.cfg
        valid_type_str = get_config().get(
            'answer-distribution',
            'valid_response_types',
            'choiceresponse,optionresponse,multiplechoiceresponse,numericalresponse,stringresponse,formularesponse'
        )

        valid_types = set(valid_type_str.split(","))
        if response_type in valid_types:
            return True

        return False

    def get_answer_grouping_key(self, answer):
        """Return value to use for uniquely identify an answer value in the distribution."""
        # For variants, we want to treat missing variants with the
        # same value as used for events that lack 'submission'
        # information, so that they will be grouped together.  That
        # value is a seed value of '1'.  We want to map both missing
        # values and zero-length values to this default value.
        variant = answer.get('variant', '')
        if variant == '':
            variant = '1'
        # Events that lack 'submission' information will have a value
        # for 'answer_value_id' and none for 'answer'.  Events with
        # 'submission' information will have the reverse situation
        # most of the time, but both values filled in for multiple
        # choice.  In the latter case, we need to use the
        # answer_value_id for comparison.
        if 'answer_value_id' in answer:
            answer_value = answer.get('answer_value_id')
        else:
            answer_value = answer.get('answer')

        # answer_value may be a list of multiple values, so we need to
        # convert it to a string that can be used as an index (i.e. to
        # increment a previous occurrence).
        return u'{value}_{variant}'.format(value=stringify_value(answer_value), variant=variant)


def stringify_value(answer_value, contains_html=False):
    """
    Convert answer value to a canonical string representation.

    If answer_value is a list, then returns list values
    surrounded by square brackets and delimited by pipes
    (e.g. "[choice_1|choice_3|choice_4]").

    If answer_value is a string, just returns as-is.

    If contains_html is True, the answer_string is parsed as XML,
    and the text value of the answer_value is returned.

    """
    # If it's a list, convert to a string.  Note that it's not
    # enough to call str() or unicode(), as this will appear as
    # "[u'choice_5']".
    def normalize(value):
        """Pull out HTML tags if requested."""
        return get_text_from_html(value) if contains_html else value.strip()

    if isinstance(answer_value, basestring):
        return normalize(answer_value)
    elif isinstance(answer_value, list):
        list_val = u'|'.join(normalize(value) for value in answer_value)
        return u'[{list_val}]'.format(list_val=list_val)
    else:
        # unexpected type:
        log.error("Unexpected type for an answer_value: %s", answer_value)
        return unicode(answer_value)


class MultipartitionHiveTableTask(HiveTableTask):
    """
    Abstract class to import data into a Hive table with multiple partitions.
    """

    def query(self):
        query_format = """
            USE {database_name};
            DROP TABLE IF EXISTS {table};
            CREATE EXTERNAL TABLE {table} (
                {col_spec}
            )
            PARTITIONED BY ({partition_key} STRING)
            {table_format}
            LOCATION '{location}';
            {recover_partitions}
        """

        query = query_format.format(
            database_name=hive_database_name(),
            table=self.table,
            col_spec=','.join([' '.join(c) for c in self.columns]),
            location=self.table_location,
            table_format=self.table_format,
            partition_key=self.partition.key,
            recover_partitions=self.recover_partitions,
        )

        query = textwrap.dedent(query)
        return query

    @property
    def recover_partitions(self):
        # TODO: make this sensitive to the underlying client.  At the moment, it only
        # works on EMR, and not on analyticstack.
        # On non-EMR hadoop, one must use:  'MSCK REPAIR TABLE {table};' 
        return 'ALTER TABLE {table} RECOVER PARTITIONS;'.format(table=self.table)

    def output(self):
        # TODO:  at present, this just checks to see if the table is present.
        # Instead, we should see if all requested partitions in a specified
        # interval are present.
        return HiveTableTarget(self.table, database=hive_database_name())


# class HivePartitionInIntervalTarget(HivePartitionTarget):
#     """
#     This should see if all requested partitions in a specified
#     interval are present.

#     This is a short-cut for creating a separate list of HivePartitionTargets.
#     """
#     def __init__(self, table, partition, database='default', interval, fail_missing_table=False, client=default_client):
#         super(HivePartitionInIntervalTarget).__init__(self, table, partition, database=database, fail_missing_table=fail_missing_table, client=client)
#         self.interval = interval


class AllProblemCheckEventsInHiveTask(AllProblemCheckEventsParamMixin, MultipartitionHiveTableTask):
    """
    A task to load all problem-check events into Hive.

    """
    @property
    def partition(self):
        """Provides name of Hive database table partition."""
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    @property
    def table(self):
        """Provides name of Hive database table."""
        return 'all_answers'

    @property
    def columns(self):
        return [
            ('time', 'STRING'),
            ('course_id', 'STRING'),
            ('problem_id', 'STRING'),
            ('part_id', 'STRING'),
            ('problem_display_name', 'STRING'),
            ('question', 'STRING'),
            ('user_id', 'STRING'),
            ('course_user_tags', 'STRING'),
            ('variant', 'STRING'),
            ('correct', 'STRING'),
            ('answer', 'STRING'),
            ('answer_value_id', 'STRING'),
            ('answer_uses_value_id', 'STRING'),
            ('grouping_key', 'STRING'),
            ('uses_submission', 'STRING'),
            ('should_include_answer', 'STRING'),
        ]

    def requires(self):
        return AllProblemCheckEventsTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            output_root=self.table_location,  # this is the table, not the partition
        )

    def output(self):
        # Approximate this by just looking for the first and last partition in the interval, and if
        # both are present, then assume that all data in between is present.
        # And if one or the other is not present, then assume that the interval is not present,
        # and trigger the entire task.
        # TODO: this breaks when the starting date of the interval doesn't actually have data.
        #   This is requiring that the date of the first actual data be known,
        #   rather than putting in something safe (e.g. 2012-01-01 or even 2013-10-01).
        # starting_partition = HivePartition('dt', self.interval.date_a.isoformat()) # pylint: disable=no-member
        ending_date = self.interval.date_b - datetime.timedelta(days=1)  # pylint: disable=no-member
        ending_partition = HivePartition('dt', ending_date.isoformat())

        return [
            # HivePartitionTarget(self.table, starting_partition.as_dict(), database=hive_database_name()),
            HivePartitionTarget(self.table, ending_partition.as_dict(), database=hive_database_name()),
        ]

        # This should see if all requested partitions in a specified
        # interval are present.
        # return HivePartitionInIntervalTarget(self.table, self.partition, database=hive_database_name(), self.interval)


class HiveAnswerTableFromQueryTask(AllProblemCheckEventsParamMixin, HiveTableTask):

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    def query(self):
        create_table_statements = super(HiveAnswerTableFromQueryTask, self).query()
        full_insert_query = """
            INSERT INTO TABLE {table}
            PARTITION ({partition.query_spec})
            {insert_query}
        """.format(
            table=self.table,
            partition=self.partition,
            insert_query=self.insert_query.strip(),  # pylint: disable=no-member
        )

        return create_table_statements + textwrap.dedent(full_insert_query)

    def output(self):
        return get_target_from_url(self.partition_location)


class LatestProblemInfo(HiveAnswerTableFromQueryTask):

    @property
    def table(self):
        return 'latest_problem_info'

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
            ('part_id', 'STRING'),
            ('problem_id', 'STRING'),
            ('question', 'STRING'),
            ('problem_display_name', 'STRING'),
            ('answer_uses_value_id', 'STRING'),   # change to TINYINT sometime...
            ('time', 'STRING'),
        ]

    @property
    def insert_query(self):
        return """
        SELECT aat.course_id, aat.part_id, aat.problem_id, aat.question, aat.problem_display_name,
                     aat.answer_uses_value_id, aat.time
        FROM all_answers aat
        INNER JOIN (
            SELECT course_id, part_id, max(time) as latest_time
                  FROM all_answers
                  GROUP BY course_id, part_id
            ) latest
          ON (aat.course_id = latest.course_id
              AND aat.part_id = latest.part_id
              AND aat.time = latest.latest_time)
        WHERE aat.should_include_answer = 'True' AND aat.uses_submission = 'True'
        """

    def requires(self):
        return AllProblemCheckEventsInHiveTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
        )


class LatestAnswerInfo(HiveAnswerTableFromQueryTask):

    @property
    def table(self):
        return 'latest_answer_info'

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
            ('part_id', 'STRING'),
            ('grouping_key', 'STRING'),
            ('variant', 'STRING'),
            ('is_correct', 'STRING'),
            ('time', 'STRING'),
            ('answer_value', 'STRING'),
            ('value_id', 'STRING'),
        ]

    @property
    def insert_query(self):
        return """
        SELECT aat.course_id, aat.part_id, aat.grouping_key, aat.variant, aat.correct, aat.time,
            IF(lpi.answer_uses_value_id='True' OR aat.uses_submission='True', aat.answer, aat.answer_value_id) as answer_value,
            IF(lpi.answer_uses_value_id='True' OR aat.uses_submission='True', aat.answer_value_id, aat.answer) as value_id
        FROM all_answers aat
        INNER JOIN (
            SELECT max(time) as max_time, course_id, part_id, grouping_key
                    FROM all_answers
                    GROUP BY course_id, part_id, grouping_key
            ) latest
          ON (aat.course_id = latest.course_id
            AND aat.part_id = latest.part_id
            AND aat.grouping_key = latest.grouping_key
            AND aat.time = latest.max_time)
        INNER JOIN latest_problem_info lpi
          ON (lpi.course_id = aat.course_id AND lpi.part_id = aat.part_id)
        """

    def requires(self):
        # Also depends on all answers, but we know that LatestProblemInfo does as well.
        return LatestProblemInfo(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
        )


class LatestAnswers(HiveAnswerTableFromQueryTask):

    @property
    def table(self):
        return 'latest_answers'

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
            ('part_id', 'STRING'),
            ('user_id', 'STRING'),
            ('grouping_key', 'STRING'),
            ('course_user_tags', 'STRING'),
            ('time', 'STRING'),
        ]

    @property
    def insert_query(self):
        return """
        SELECT aat.course_id, aat.part_id, aat.user_id, aat.grouping_key, aat.course_user_tags, aat.time
        FROM all_answers aat
        INNER JOIN (
            SELECT max(time) as max_time, course_id, part_id, user_id
                    FROM all_answers
                    GROUP BY course_id, part_id, user_id
            ) latest
        ON (aat.course_id = latest.course_id
            AND aat.part_id = latest.part_id
            AND aat.user_id = latest.user_id
            AND aat.time = latest.max_time)
        """

    def requires(self):
        return AllProblemCheckEventsInHiveTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
        )


class LatestAnswerDist(HiveAnswerTableFromQueryTask):

    @property
    def table(self):
        return 'latest_answer_dist'

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
            ('part_id', 'STRING'),
            ('variant', 'STRING'),
            ('is_correct', 'STRING'),
            ('answer_value', 'STRING'),
            ('value_id', 'STRING'),
            ('count', 'INT'),
            ('problem_id', 'STRING'),
            ('question', 'STRING'),
            ('display_name', 'STRING'),
        ]

    @property
    def insert_query(self):
        return """
        SELECT la.course_id, la.part_id,
               lai.variant, lai.is_correct, lai.answer_value, lai.value_id,
               count(la.user_id) as count,
               lpi.problem_id, lpi.question, lpi.problem_display_name
        FROM latest_answers la
        INNER JOIN latest_problem_info lpi
            ON (lpi.course_id = la.course_id AND lpi.part_id = la.part_id)
        INNER JOIN latest_answer_info lai
            ON (lai.course_id = la.course_id AND lai.part_id = la.part_id
                AND lai.grouping_key = la.grouping_key)
        GROUP BY la.course_id, la.part_id, la.grouping_key,
                lai.variant, lai.is_correct, lai.answer_value, lai.value_id,
                lpi.problem_id, lpi.question, lpi.problem_display_name
        """

    def requires(self):
        yield LatestAnswerInfo(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
        )
        yield LatestAnswers(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
        )


class AnswerDistOneFilePerCourseTask(AllProblemCheckEventsParamMixin, MultiOutputMapReduceJobTask, WarehouseMixin):
    """
    Blah.
    """

    def requires(self):
        return LatestAnswerDist(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
        )

    def extra_modules(self):
        import six
        return [html5lib, six]

    def mapper(self, line):
        """
        Groups inputs by course_id, writes all records with the same course_id to the same output file.

        Each input line is expected to consist of two tab separated columns. The first column is expected to be the
        course_id and is used to group the entries. The course_id is stripped from the output and the remaining column
        is written to the appropriate output file in the same format it was read in (i.e. as an encoded JSON string).
        """
        # Ensure that the first column is interpreted as the grouping key by the hadoop streaming API.  Note that since
        # Configuration values can change this behavior, the remaining tab separated columns are encoded in a python
        # structure before returning to hadoop.  They are decoded in the reducer.
        course_id, content = line.split('\t', 1)
        yield course_id, content

    def output_path_for_key(self, course_id):
        """
        Match the course folder hierarchy that is expected by the instructor dashboard.

        The instructor dashboard expects the file to be stored in a folder named sha1(course_id).  All files in that
        directory will be displayed on the instructor dashboard for that course.
        """
        hashed_course_id = hashlib.sha1(course_id).hexdigest()
        filename_safe_course_id = opaque_key_util.get_filename_safe_course_id(course_id, '_')
        filename = u'{course_id}_answer_distribution.csv'.format(course_id=filename_safe_course_id)
        return url_path_join(self.output_root, hashed_course_id, filename)

    CSV_FIELD_NAMES = [
        'ModuleID',
        'PartID',
        'Correct Answer',
        'Count',
        'ValueID',
        'AnswerValue',
        'Variant',
        'Problem Display Name',
        'Question',
    ]

    HIVE_FIELD_NAMES = [
        'PartID',
        'Variant',
        'Correct Answer',
        'AnswerValue',
        'ValueID',
        'Count',
        'ModuleID',
        'Question',
        'Problem Display Name',
    ]

    def multi_output_reducer(self, _course_id, values, output_file):
        """
        Each entry should be written to the output file in csv format.

        This output is visible to instructors, so use an excel friendly format (csv).
        """
        field_names = self.CSV_FIELD_NAMES
        writer = csv.DictWriter(output_file, field_names)
        writer.writerow(dict(
            (k, k) for k in field_names
        ))

        def load_into_dict(content):
            content_values = content.split('\t')
            return {val[0]: val[1] for val in zip(self.HIVE_FIELD_NAMES, content_values)}

        # Collect in memory the list of dicts to be output.  Then sort
        # the list of dicts by their field names before encoding.
        row_data = [load_into_dict(content) for content in values]
        row_data = sorted(row_data, key=itemgetter(*field_names))

        for row_dict in row_data:
            #encoded_dict = dict()
            #for key, value in row_dict.iteritems():
            #    encoded_dict[key] = unicode(value).encode('utf8')
            #writer.writerow(encoded_dict)
            writer.writerow(row_dict)


class AnswerDistributionFromHiveToMySQLTaskWorkflow(AllProblemCheckEventsParamMixin, WarehouseMixin, MysqlInsertTask):
    """
    Define answer_distribution_from_hive table.
    """
    @property
    def table(self):
        return "answer_distribution_from_hive"

    def rows(self):
        """
        Re-formats the output of AnswerDistributionPerCourse to something insert_rows can understand
        """
        with self.input()['insert_source'].open('r') as fobj:
            for line in fobj:
                (course_id, part_id, variant, is_correct, answer_value,
                 value_id, count, problem_id, question, display_name) = line.strip('\n').split('\t')
                # output is in a different order, and has extra value.
                output_list = [
                    course_id,
                    problem_id,
                    part_id,
                    1 if is_correct == 'True' else 0,
                    0,  # TODO:  calculate this correctly.
                    count,
                    value_id if value_id else None,
                    answer_value if answer_value else None,
                    try_str_to_float(answer_value),
                    variant if variant else None,
                    display_name if display_name else None,
                    question if question else None,
                ]
                yield output_list

    @property
    def columns(self):
        # Note that this does not align with the set of columns in Hive, so we cannot
        # assume that they're the same.
        return [
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('module_id', 'VARCHAR(255) NOT NULL'),
            ('part_id', 'VARCHAR(255) NOT NULL'),
            ('correct', 'TINYINT(1) NOT NULL'),
            ('first_response_count', 'INT(11) NOT NULL'),
            ('last_response_count', 'INT(11) NOT NULL'),
            ('value_id', 'VARCHAR(255)'),
            ('answer_value_text', 'LONGTEXT'),
            ('answer_value_numeric', 'DOUBLE'),
            ('variant', 'INT(11)'),
            ('problem_display_name', 'LONGTEXT'),
            ('question_text', 'LONGTEXT'),
        ]

    @property
    def indexes(self):
        return [
            ('course_id',),
            ('module_id',),
            ('part_id',),
            ('course_id', 'module_id'),
        ]

    def init_copy(self, connection):
        """
        Truncate the table before re-writing
        """
        # Use "DELETE" instead of TRUNCATE since TRUNCATE forces an implicit commit before it executes which would
        # commit the currently open transaction before continuing with the copy.
        query = "DELETE FROM {table}".format(table=self.table)
        connection.cursor().execute(query)

    @property
    def insert_source_task(self):
        """
        Write to answer_distribution table from AnswerDistributionTSVTask.
        """
        return LatestAnswerDist(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
        )

    def extra_modules(self):
        import six
        return [html5lib, six]


class HiveAnswerDistributionWorkflow(
        AllProblemCheckEventsParamMixin,
        WarehouseMixin,
        MysqlInsertTaskMixin,
        luigi.WrapperTask):
    """Calculate answer distribution and output to files and to database."""

    # Add additional args for MultiOutputMapReduceJobTask.
    output_root = luigi.Parameter()

    def requires(self):
        kwargs = {
            'mapreduce_engine': self.mapreduce_engine,
            'n_reduce_tasks': self.n_reduce_tasks,
            'source': self.source,
            'interval': self.interval,
            'pattern': self.pattern,
            'warehouse_path': self.warehouse_path,
        }

        # Add additional args for MultiOutputMapReduceJobTask.
        kwargs1 = {
            'output_root': self.output_root,
        }
        kwargs1.update(kwargs)

        # Add additional args for MysqlInsertTaskMixin.
        kwargs2 = {
            'database': self.database,
            'credentials': self.credentials,
            'insert_chunk_size': self.insert_chunk_size,
        }
        kwargs2.update(kwargs)

        yield (
            AnswerDistOneFilePerCourseTask(**kwargs1),
            AnswerDistributionFromHiveToMySQLTaskWorkflow(**kwargs2),
        )
