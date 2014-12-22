"""
Luigi tasks for extracting problem answer distribution statistics from
tracking log files.
"""
import math
import csv
import hashlib
import html5lib
import json
from operator import itemgetter

import luigi
import luigi.hdfs
import luigi.s3
from luigi.configuration import get_config

from edx.analytics.tasks.answer_dist import (
    get_text_from_html,
)
from edx.analytics.tasks.mapreduce import MapReduceJobTask, MultiOutputMapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.url import ExternalURL, IgnoredTarget
from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.mysql_load import MysqlInsertTask, MysqlInsertTaskMixin
import edx.analytics.tasks.util.eventlog as eventlog
import edx.analytics.tasks.util.opaque_key_util as opaque_key_util

import logging
log = logging.getLogger(__name__)


################################
# Task Map-Reduce definitions
################################

UNKNOWN_ANSWER_VALUE = ''
UNMAPPED_ANSWER_VALUE = ''
PROBLEM_CHECK_EVENT = 'problem_check'

class AllProblemCheckEventsParamMixin(EventLogSelectionDownstreamMixin):
    """Parameters for AllProblemCheckEventsTask."""

    output_root = luigi.Parameter()


class AllProblemCheckEventsTask(AllProblemCheckEventsParamMixin, EventLogSelectionMixin, MultiOutputMapReduceJobTask):
    """Identifies last problem_check event for a user on a problem in a course, given raw event log input."""

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
        filename = u'{course_id}_answers.tsv'.format(course_id=filename_safe_course_id)
        return url_path_join(self.output_root, date_directory, filename)

    def multi_output_reducer(self, key, values, output_file):
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

        date_string, course_id = key

        for event_string in values:
            for answer in self._generate_answers(event_string):
                self._output_answer(answer, output_file)

    COLUMN_NAMES = [
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
        'timestamp',
        'correct',
        'answer',
        'answer_value_id',
        'answer_uses_value_id',
        'grouping_key',
        'uses_submission',
    ]

    def _output_answer(self, answer, output_file):
        """
        Outputs answer dict in TSV format for input to Hive.

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
        answer_string = '\t'.join([str(answer.get(value, "")) for value in self.COLUMN_NAMES])
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
        # print "Generating answers from event string %s" % event_string
        # event_string = event_string.decode('utf-8')
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
            answer['timestamp'] = timestamp
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
            # results.append((output_key, output_value))
            results.append(answer)

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


class AllProblemCheckEventsInHiveTask(luigi.Task):
    """
    A task to load all problem-check events into Hive.

    For now, just use a TSV as input, instead of JSON SerDe.
    """
    pass
