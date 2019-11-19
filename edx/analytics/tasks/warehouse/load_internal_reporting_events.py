"""EXPERIMENTAL:

Load most events into warehouse for internal reporting purposes.  This
combines segment events and tracking log events, and defines a common
(wide) representation for all events to share, sparsely.  Requires
definition of a Record enumerating these columns, and also a mapping
from event values to column values.

"""
from __future__ import absolute_import

import datetime
import json
import logging
import re
from importlib import import_module

import ciso8601
import dateutil
import luigi
import luigi.task
import pytz
import six
import ua_parser
import user_agents
from luigi.configuration import get_config
from luigi.date_interval import DateInterval

from edx.analytics.tasks.common.bigquery_load import BigQueryLoadDownstreamMixin, BigQueryLoadTask
from edx.analytics.tasks.common.mapreduce import MapReduceJobTaskMixin, MultiOutputMapReduceJobTask
from edx.analytics.tasks.common.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.common.vertica_load import SchemaManagementTask, VerticaCopyTask, VerticaCopyTaskMixin
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.hive import BareHiveTableTask, HivePartition, HivePartitionTask, WarehouseMixin
from edx.analytics.tasks.util.obfuscate_util import backslash_encode_value
from edx.analytics.tasks.util.opaque_key_util import get_course_key_from_url, get_org_id_for_course, is_valid_course_id
from edx.analytics.tasks.util.record import (
    BooleanField, DateField, DateTimeField, FloatField, IntegerField, SparseRecord, StringField
)
from edx.analytics.tasks.util.url import ExternalURL, url_path_join

log = logging.getLogger(__name__)

VERSION = '0.2.4'

EVENT_TABLE_NAME = 'event_records'

# Define pattern to extract a course_id from a string by looking
# explicitly for a version string and two plus-delimiters.
# TODO: avoid this hack by finding a more opaque-keys-respectful way.
NEW_COURSE_ID_PATTERN = r'(?P<course_id>course\-v1\:[^/+]+(\+)[^/+]+(\+)[^/]+)'
NEW_COURSE_REGEX = re.compile(r'^.*?{}'.format(NEW_COURSE_ID_PATTERN))


class EventRecord(SparseRecord):
    """Represents an event, either a tracking log event or segment event."""

    # Metadata:
    version = StringField(length=20, nullable=False, description='blah.')
    input_file = StringField(length=255, nullable=True, description='blah.')
    # hash_id = StringField(length=255, nullable=False, description='blah.')

    # Globals:
    project = StringField(length=255, nullable=False, description='blah.')
    event_type = StringField(length=255, nullable=False, description='The type of event.  Example: video_play.')
    event_source = StringField(length=255, nullable=False, description='blah.')
    event_category = StringField(length=255, nullable=True, description='blah.')

    # TODO: decide what type 'timestamp' should be.
    # Also make entries required (not nullable), once we have confidence.
    timestamp = StringField(length=255, nullable=True, description='Timestamp when event was emitted.')
    received_at = StringField(length=255, nullable=True, description='Timestamp when event was received.')
    # TODO: figure out why these have errors, and then make DateField.
    date = StringField(length=255, nullable=False, description='The learner interacted with the entity on this date.')

    # Common (but optional) values:
    accept_language = StringField(length=255, nullable=True, description='')
    agent = StringField(length=1023, nullable=True, description='')
    # 'agent' string gets parsed into the following:
    agent_type = StringField(length=20, nullable=True, description='')
    agent_device_name = StringField(length=100, nullable=True, description='', truncate=True)
    agent_os = StringField(length=100, nullable=True, description='')
    agent_browser = StringField(length=100, nullable=True, description='')
    # agent_touch_capable = BooleanField(nullable=True, description='')
    agent_touch_capable = StringField(length=10, nullable=True, description='')

    host = StringField(length=80, nullable=True, description='')
    # TODO: geolocate ip to find country or more specific information?
    ip = StringField(length=64, nullable=True, description='')
    # name: not really used yet?
    page = StringField(length=1024, nullable=True, description='')
    referer = StringField(length=2047, nullable=True, description='')
    session = StringField(length=255, nullable=True, description='')
    username = StringField(length=50, nullable=True, description='Learner\'s username.')

    # Common (but optional) context values:
    # We exclude course_user_tags, as it's a set of key-value pairs that affords no stable naming scheme.
    # TODO:  decide how to deal with redundant data.  Shouldn't be specifying "context_" here,
    # since that doesn't generalize to segment data at all.
    context_course_id = StringField(length=255, nullable=True, description='Id of course.')
    context_org_id = StringField(length=255, nullable=True, description='Id of organization, as used in course_id.')
    context_path = StringField(length=1024, nullable=True, description='')
    context_user_id = StringField(length=255, nullable=True, description='')
    context_module_display_name = StringField(length=255, nullable=True, description='')
    context_module_usage_key = StringField(length=255, nullable=True, description='')
    context_module_original_usage_key = StringField(length=255, nullable=True, description='')
    context_module_original_usage_version = StringField(length=255, nullable=True, description='')
    # course_user_tags	object
    # application	object:  explicitly extracted to 'app_name', 'app_version'.
    # client	object
    context_component = StringField(length=255, nullable=True, description='')  # string
    context_mode = StringField(length=255, nullable=True, description='')  # string

    # This is handled for tracking logs by writing to received_at explicitly.
    # context_received_at = StringField(length=255, nullable=True, description='')  # number

    # Per-event values:
    # entity_type = StringField(length=10, nullable=True, description='Category of entity that the learner interacted'
    # ' with. Example: "video".')
    # entity_id = StringField(length=255, nullable=True, description='A unique identifier for the entity within the'
    # ' course that the learner interacted with.')

    add_method = StringField(length=255, nullable=True, description='')  # string
    # added	list
    allowance_key = StringField(length=255, nullable=True, description='')  # string
    allowance_user_id = StringField(length=255, nullable=True, description='')  # number
    allowance_value = StringField(length=255, nullable=True, description='')  # string
    amount = StringField(length=255, nullable=True, description='')  # string
    anonymous = StringField(length=255, nullable=True, description='')  # Boolean
    anonymous_to_peers = StringField(length=255, nullable=True, description='')  # Boolean
    answer = StringField(length=255, nullable=True, description='')  # integer
    # answer	object
    answers = StringField(length=255, nullable=True, description='')  # string
    # answers	object
    attempt_allowed_time_limit_mins = StringField(length=255, nullable=True, description='')  # number
    attempt_code = StringField(length=255, nullable=True, description='')  # string
    attempt_completed_at = StringField(length=255, nullable=True, description='')  # datetime
    attempt_event_elapsed_time_secs = StringField(length=255, nullable=True, description='')  # number
    attempt_id = StringField(length=255, nullable=True, description='')  # number
    attempt_number = StringField(length=255, nullable=True, description='')  # number
    attempt_started_at = StringField(length=255, nullable=True, description='')  # datetime
    attempt_status = StringField(length=255, nullable=True, description='')  # string
    attempt_user_id = StringField(length=255, nullable=True, description='')  # number
    attempts = StringField(length=255, nullable=True, description='')  # use int
    body = StringField(length=2047, nullable=True, description='')  # string
    bookmark_id = StringField(length=255, nullable=True, description='')  # string
    bookmarks_count = StringField(length=255, nullable=True, description='')  # integer
    bumper_id = StringField(length=255, nullable=True, description='')  # string
    casesensitive = StringField(length=255, nullable=True, description='')  # Boolean
    category = StringField(length=255, nullable=True, description='')  # number
    category_id = StringField(length=255, nullable=True, description='')  # string
    category_name = StringField(length=255, nullable=True, description='')  # string
    certificate_id = StringField(length=255, nullable=True, description='')  # string
    certificate_url = StringField(length=255, nullable=True, description='')  # string
    chapter = StringField(length=255, nullable=True, description='')  # string: pdf
    chapter_title = StringField(length=255, nullable=True, description='')  # string
    child_id = StringField(length=255, nullable=True, description='')  # string
    choice = StringField(length=255, nullable=True, description='')  # string: poll
    # choice_all	array
    # choices	object
    code = StringField(length=255, nullable=True, description='')  # string: video
    cohort_id = StringField(length=255, nullable=True, description='')  # number:  cohort
    cohort_name = StringField(length=255, nullable=True, description='')  # string
    commentable_id = StringField(length=255, nullable=True, description='')  # string: forums
    component_type = StringField(length=255, nullable=True, description='')  # string
    component_usage_id = StringField(length=255, nullable=True, description='')  # string
    content = StringField(length=255, nullable=True, description='')  # string
    # correct_map	object
    corrected_text = StringField(length=255, nullable=True, description='')  # forum search
    # corrections	object
    correctness = StringField(length=255, nullable=True, description='')  # Boolean
    course = StringField(length=255, nullable=True, description='')  # string
    course_id = StringField(length=255, nullable=True, description='')  # enrollment, certs
    created_at = StringField(length=255, nullable=True, description='')  # datetime
    # "current_time" is a SQL function name/alias, so we need to use something different here.
    # We will instead map it to "currenttime", which will receive values from "current_time" and "currentTime".
    currenttime = StringField(length=255, nullable=True, description='')  # float/int/str:  video
    current_slide = StringField(length=255, nullable=True, description='')  # number
    current_tab = StringField(length=255, nullable=True, description='')  # integer
    current_url = StringField(length=255, nullable=True, description='')  # string
    direction = StringField(length=255, nullable=True, description='')  # pdf
    discussion_id = StringField(length=255, nullable=True, description='')  # discussion.id forum
    displayed_in = StringField(length=255, nullable=True, description='')  # googlecomponent
    done = StringField(length=255, nullable=True, description='')  # Boolean
    duration = StringField(length=255, nullable=True, description='')  # int: videobumper
    enrollment_mode = StringField(length=255, nullable=True, description='')  # certs
    event = StringField(length=255, nullable=True, description='')  # string
    event_name = StringField(length=255, nullable=True, description='')  # string
    exam_content_id = StringField(length=255, nullable=True, description='')  # string
    exam_default_time_limit_mins = StringField(length=255, nullable=True, description='')  # number
    exam_id = StringField(length=255, nullable=True, description='')  # number
    exam_is_active = StringField(length=255, nullable=True, description='')  # Boolean
    exam_is_practice_exam = StringField(length=255, nullable=True, description='')  # Boolean
    exam_is_proctored = StringField(length=255, nullable=True, description='')  # Boolean
    exam_name = StringField(length=255, nullable=True, description='')  # string
    exploration_id = StringField(length=255, nullable=True, description='')  # string
    exploration_version = StringField(length=255, nullable=True, description='')  # string
    failure = StringField(length=255, nullable=True, description='')  # string
    feedback = StringField(length=2047, nullable=True, description='')  # string
    feedback_text = StringField(length=2047, nullable=True, description='')  # string
    field = StringField(length=255, nullable=True, description='')  # team
    fileName = StringField(length=255, nullable=True, description='')  # string
    fileSize = StringField(length=255, nullable=True, description='')  # number
    fileType = StringField(length=255, nullable=True, description='')  # string
    findprevious = StringField(length=255, nullable=True, description='')  # Boolean
    generation_mode = StringField(length=255, nullable=True, description='')  # cert
    grade = StringField(length=255, nullable=True, description='')  # float/int:  problem_check
    group_id = StringField(length=255, nullable=True, description='')  # int:  forum
    group_name = StringField(length=255, nullable=True, description='')  # user_to_partition
    highlightall = StringField(length=255, nullable=True, description='')  # highlightAll: Boolean
    highlighted_content = StringField(length=255, nullable=True, description='')  # string
    hint_index = StringField(length=255, nullable=True, description='')  # number
    hint_label = StringField(length=255, nullable=True, description='')  # string
    hint_len = StringField(length=255, nullable=True, description='')  # number
    hint_text = StringField(length=2047, nullable=True, description='')  # string
    # hints	array
    host_component_id = StringField(length=255, nullable=True, description='')  # string
    id = StringField(length=255, nullable=True, description='')  # string: video, forum
    input = StringField(length=255, nullable=True, description='')  # integer
    instructor = StringField(length=255, nullable=True, description='')
    is_correct = StringField(length=255, nullable=True, description='')  # Boolean
    is_correct_location = StringField(length=255, nullable=True, description='')  # Boolean
    item_id = StringField(length=255, nullable=True, description='')  # integer, string
    letter_grade = StringField(length=64, nullable=True)
    list_type = StringField(length=255, nullable=True, description='')  # string
    location = StringField(length=255, nullable=True, description='')  # library
    manually = StringField(length=255, nullable=True, description='')  # Boolean
    max_count = StringField(length=255, nullable=True, description='')  # int:  library
    max_grade = StringField(length=255, nullable=True, description='')  # int:  problem_check
    mode = StringField(length=255, nullable=True, description='')  # enrollment
    module_id = StringField(length=255, nullable=True, description='')  # hint
    name = StringField(length=255, nullable=True, description='')  # pdf
    # NEW is a keyword in SQL on Vertica, so use different name here.
    new_value = StringField(length=2047, nullable=True, description='')  # int: seq, str: book, team, settings
    new_score = StringField(length=255, nullable=True, description='')  # number
    new_speed = StringField(length=255, nullable=True, description='')  # video
    # new_state	object
    new_state_name = StringField(length=255, nullable=True, description='')  # string
    new_time = StringField(length=255, nullable=True, description='')  # float/int:  video
    new_total = StringField(length=255, nullable=True, description='')  # number
    note_id = StringField(length=255, nullable=True, description='')  # string
    note_text = StringField(length=255, nullable=True, description='')  # string
    # notes	array
    number_of_results = StringField(length=255, nullable=True, description='')  # integer, number

    # Not documented, but used by problembuilder:
    num_attempts = StringField(length=255, nullable=True, description='')  # int:  problem_builder

    # OLD is a keyword in SQL on Vertica, so use different name here.
    old_value = StringField(length=2047, nullable=True, description='')  # int: seq, str: book, team, settings
    old_attempts = StringField(length=255, nullable=True, description='')  # string
    old_note_text = StringField(length=255, nullable=True, description='')  # string
    old_speed = StringField(length=255, nullable=True, description='')  # video
    # old_state	object
    old_state_name = StringField(length=255, nullable=True, description='')  # string
    # old_tags	array of strings
    old_time = StringField(length=255, nullable=True, description='')  # number
    # options	array
    # options	object
    options_followed = StringField(length=255, nullable=True, description='')  # options.followed:  boolean
    # options_selected	object
    orig_score = StringField(length=255, nullable=True, description='')  # number
    orig_total = StringField(length=255, nullable=True, description='')  # number
    page = StringField(length=1023, nullable=True, description='')  # int/str:  forum, pdf
    page_name = StringField(length=255, nullable=True, description='')  # string
    page_number = StringField(length=255, nullable=True, description='')  # integer
    page_size = StringField(length=255, nullable=True, description='')  # integer
    partition_id = StringField(length=255, nullable=True, description='')  # number
    partition_name = StringField(length=255, nullable=True, description='')  # string
    percent_grade = FloatField(nullable=True)
    # parts: [criterion, option, feedback]	array
    previous_cohort_id = StringField(length=255, nullable=True, description='')  # int:  cohort
    previous_cohort_name = StringField(length=255, nullable=True, description='')  # cohort
    previous_count = StringField(length=255, nullable=True, description='')  # int:  lib
    problem = StringField(length=255, nullable=True, description='')  # show/reset/rescore
    problem_id = StringField(length=255, nullable=True, description='')  # capa
    problem_part_id = StringField(length=255, nullable=True, description='')  # hint
    query = StringField(length=255, nullable=True, description='')  # forum, pdf
    question_type = StringField(length=255, nullable=True, description='')  # hint
    rationale = StringField(length=1023, nullable=True, description='')  # string
    reason = StringField(length=255, nullable=True, description='')  # string
    remove_method = StringField(length=255, nullable=True, description='')  # string
    # removed	list
    report_type = StringField(length=255, nullable=True, description='')  # string
    report_url = StringField(length=255, nullable=True, description='')  # string
    requested_skip_interval = StringField(length=255, nullable=True, description='')  # number
    requesting_staff_id = StringField(length=255, nullable=True, description='')  # string
    requesting_student_id = StringField(length=255, nullable=True, description='')  # string
    response_id = StringField(length=255, nullable=True, description='')  # response.id:  forum
    # result	list
    review_attempt_code = StringField(length=255, nullable=True, description='')  # string
    review_status = StringField(length=255, nullable=True, description='')  # string
    review_video_url = StringField(length=255, nullable=True, description='')  # string
    # rubric	object
    # saved_response	object
    score_type = StringField(length=255, nullable=True, description='')  # string
    scored_at = StringField(length=255, nullable=True, description='')  # datetime
    scorer_id = StringField(length=255, nullable=True, description='')  # string
    search_string = StringField(length=255, nullable=True, description='')  # string
    search_text = StringField(length=255, nullable=True, description='')  # team
    selection = StringField(length=255, nullable=True, description='')  # number
    slide = StringField(length=255, nullable=True, description='')  # number
    # Not listed or attested:     seek_type = StringField(length=255, nullable=True, description='')  # video
    social_network = StringField(length=255, nullable=True, description='')  # certificate
    source_url = StringField(length=255, nullable=True, description='')  # string
    # state	object
    status = StringField(length=255, nullable=True, description='')  # status
    student = StringField(length=255, nullable=True, description='')  # reset/delete/rescore
    # student_answer	array
    # submission	object
    submission_returned_uuid = StringField(length=255, nullable=True, description='')  # string
    submission_uuid = StringField(length=255, nullable=True, description='')  # string
    submitted_at = StringField(length=255, nullable=True, description='')  # datetime
    success = StringField(length=255, nullable=True, description='')  # problem_check
    tab_count = StringField(length=255, nullable=True, description='')  # integer
    # tags	array of strings
    target_name = StringField(length=255, nullable=True, description='')  # string
    target_tab = StringField(length=255, nullable=True, description='')  # integer
    target_url = StringField(length=255, nullable=True, description='')  # string
    target_username = StringField(length=255, nullable=True, description='')  # string
    team_id = StringField(length=255, nullable=True, description='')  # team, forum
    thread_type = StringField(length=255, nullable=True, description='')  # forum
    title = StringField(length=1023, nullable=True, description='')  # forum, segment
    thumbnail_title = StringField(length=255, nullable=True, description='')  # string
    topic_id = StringField(length=255, nullable=True, description='')  # team
    total_results = StringField(length=255, nullable=True, description='')  # int: forum
    total_slides = StringField(length=255, nullable=True, description='')  # number
    trigger_type = StringField(length=255, nullable=True, description='')  # string
    truncated = StringField(length=255, nullable=True, description='')  # bool:  forum
    # truncated	array
    # truncated	array of strings
    type = StringField(length=255, nullable=True, description='')  # video, book
    undo_vote = StringField(length=255, nullable=True, description='')  # Boolean
    url_name = StringField(length=255, nullable=True, description='')  # poll/survey
    url = StringField(length=2047, nullable=True, description='')  # forum, googlecomponent, segment
    # USER is a keyword in SQL on Vertica, so use different name here.
    event_user = StringField(length=255, nullable=True, description='')  # string
    # user_course_roles	array
    # user_forums_roles	array
    user_id = StringField(length=255, nullable=True, description='')  # int: enrollment, cohort, etc.
    # event_username is mapped from root.event.username, to keep separate from root.username.
    event_username = StringField(length=255, nullable=True, description='')  # add/remove forum
    value = StringField(length=255, nullable=True, description='')  # number
    view = StringField(length=255, nullable=True, description='')  # string
    vote_value = StringField(length=255, nullable=True, description='')  # string
    widget_placement = StringField(length=255, nullable=True, description='')  # string

    # Stuff from segment:
    channel = StringField(length=255, nullable=True, description='')
    anonymous_id = StringField(length=255, nullable=True, description='')
    path = StringField(length=2047, nullable=True, description='')
    referrer = StringField(length=8191, nullable=True, description='')
    search = StringField(length=2047, nullable=True, description='')
    # title and url already exist
    variationname = StringField(length=255, nullable=True, description='')
    variationid = StringField(length=255, nullable=True, description='')
    experimentid = StringField(length=255, nullable=True, description='')
    experimentname = StringField(length=255, nullable=True, description='')
    category = StringField(length=255, nullable=True, description='')
    label = StringField(length=511, nullable=True, description='')
    display_name = StringField(length=255, nullable=True, description='')
    client_id = StringField(length=255, nullable=True, description='')
    locale = StringField(length=255, nullable=True, description='')
    timezone = StringField(length=255, nullable=True, description='')
    app_name = StringField(length=255, nullable=True, description='')
    app_version = StringField(length=255, nullable=True, description='')
    os_name = StringField(length=255, nullable=True, description='')
    os_version = StringField(length=255, nullable=True, description='')
    device_manufacturer = StringField(length=255, nullable=True, description='')
    device_model = StringField(length=255, nullable=True, description='')
    network_carrier = StringField(length=255, nullable=True, description='')
    action = StringField(length=255, nullable=True, description='')
    screen_width = StringField(length=255, nullable=True, description='')
    screen_height = StringField(length=255, nullable=True, description='')
    campaign_source = StringField(length=255, nullable=True, description='')
    campaign_medium = StringField(length=255, nullable=True, description='')
    campaign_content = StringField(length=255, nullable=True, description='')
    campaign_name = StringField(length=255, nullable=True, description='')


class JsonEventRecord(SparseRecord):
    """Represents an event, either a tracking log event or segment event."""

    timestamp = DateTimeField(nullable=True, description='Timestamp when event was emitted.')

    received_at = DateTimeField(nullable=True, description='Timestamp when event was received/recorded.')

    # was context_user_id:
    user_id = StringField(length=255, nullable=True, description='The identifier of the user who was logged in when the event was emitted. '
                          'This is often but not always numeric.')

    username = StringField(length=50, nullable=True, description='The username of the user who was logged in when the event was emitted.')

    anonymous_id = StringField(length=255, nullable=True, description='The anonymous_id of the user.')

    event_type = StringField(
        length=255,
        nullable=False,
        description='The name of the event (event name in the segment logs). For implicit events, this should be "edx.server.request".'
    )

    # was context_course_id
    course_id = StringField(length=255, nullable=True, description='The course_id associated with this event (if any).')
    org_id = StringField(length=255, nullable=True, description='Id of organization, as used in course_id.')

    label = StringField(length=511, nullable=True, description='The GA label associated with this event.')

    # This is not the same as "event_category".
    category = StringField(length=255, nullable=True, description='The GA category for this event.')

    # This was event_source:
    emitter_type = StringField(length=255, nullable=False, description='Where the event was collected from (browser, mobile, server, etc).')

    # This is populated for most segment events, but also forum, googlecomponent tracking log events.
    url = StringField(
        length=2047,
        nullable=True,
        description='For page events, the full URL (including hostname) that was accessed by the user. '
        'For implicit events, this should be the full URL that the request was for.'
    )

    # use the length (and name) from segment version (referrer), and write tracking log 'referer' here as well.
    referrer = StringField(length=8191, nullable=True, description='The HTTP referrer - also as a full URL.')

    # was project:
    source = StringField(length=255, nullable=False, description='The segment.com project the event was sent to.')

    input_file = StringField(length=255, nullable=False, description='The full URL of the file that contains this event in S3.')

    agent_type = StringField(length=20, nullable=True, description='The type of device used.')
    agent_device_name = StringField(length=100, nullable=True, description='The name of the device used.', truncate=True)
    agent_os = StringField(length=100, nullable=True, description='The name of the OS on the device used. ')
    agent_browser = StringField(length=100, nullable=True, description='The name of the browser used.')

    # So having a StringField (as with EventRecord) has this write out to disk as "True".
    # Using a BooleanField here has this end up being written out as 0 or 1.
    # However, when loading to BigQuery as BOOLEAN, the boolean is translated back to True/False, or null.
    agent_touch_capable = BooleanField(nullable=True, description='A boolean value indicating that the device was touch-capable.')

    raw_event = StringField(length=60000, nullable=True, description='The full text of the event as a JSON string. This can be parsed at query time using UDFs.')

    # This was originally a StringField, but a DateField outputs the same format and is more useful.
    date = DateField(nullable=False, description='The date when the event was received.')


class EventRecordClassMixin(object):

    event_record_type = luigi.Parameter(
        description='The kind of event record to load.  Default is EventRecord, override with JsonEventRecord as needed.',
        default='EventRecord',
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

    def uses_JSON_event_record(self):
        return self.event_record_type == 'JsonEventRecord'


class EventRecordDownstreamMixin(EventRecordClassMixin, WarehouseMixin, MapReduceJobTaskMixin):

    events_list_file_path = luigi.Parameter(default=None)


class EventRecordDataDownstreamMixin(EventRecordDownstreamMixin):

    """Common parameters and base classes used to pass parameters through the event record workflow."""
    output_root = luigi.Parameter()


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

    def requires_local(self):
        if self.events_list_file_path is not None:
            return ExternalURL(url=self.events_list_file_path)
        else:
            return []

    def init_local(self):
        super(BaseEventRecordDataTask, self).init_local()
        if self.events_list_file_path is None:
            self.known_events = {}
        else:
            self.known_events = self.parse_events_list_file()

    def parse_events_list_file(self):
        """Read and parse the known events list file and populate it in a dictionary."""
        parsed_events = {}
        with self.input_local().open() as f_in:
            lines = f_in.readlines()
            for line in lines:
                if not line.startswith('#') and len(line.split("\t")) is 3:
                    parts = line.rstrip('\n').split("\t")
                    parsed_events[(parts[1], parts[2])] = parts[0]
        return parsed_events

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
            EVENT_TABLE_NAME,
            'dt={date}'.format(date=date_received),
            '{project}.tsv'.format(project=project),
        )

    def extra_modules(self):
        return [pytz, ua_parser, user_agents, dateutil]

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

    def _canonicalize_user_agent(self, agent):
        """
        There is a lot of variety in the user agent field that is hard for humans to parse, so we canonicalize
        the user agent to extract the information we're looking for.
        Args:
            agent: an agent string.
        Returns:
            a dictionary of information about the user agent.
        """
        agent_dict = {}

        try:
            user_agent = user_agents.parse(agent)
        except Exception:  # If the user agent can't be parsed, just drop the agent data on the floor since it's of no use to us.
            self.incr_counter(self.counter_category_name, 'Quality Unparseable agent', 1)
            return agent_dict

        device_type = ''  # It is possible that the user agent isn't any of the below.
        if user_agent.is_mobile:
            device_type = "mobile"
        elif user_agent.is_tablet:
            device_type = "tablet"
        elif user_agent.is_pc:
            device_type = "desktop"
        elif user_agent.is_bot:
            device_type = "bot"

        if device_type:
            agent_dict['type'] = device_type
            agent_dict['device_name'] = user_agent.device.family
            agent_dict['os'] = user_agent.os.family
            agent_dict['browser'] = user_agent.browser.family
            # TODO: figure out how to handle this, so that it works
            # when the target field is either BooleanField or StringField.
            # agent_dict['touch_capable'] = unicode(user_agent.is_touch_capable)
            agent_dict['touch_capable'] = user_agent.is_touch_capable
        else:
            self.incr_counter(self.counter_category_name, 'Quality Unrecognized agent type', 1)

        return agent_dict

    def add_agent_info(self, event_dict, agent):
        if agent:
            agent_dict = self._canonicalize_user_agent(agent)
            for key in agent_dict.keys():
                new_key = u"agent_{}".format(key)
                # event_dict[new_key] = agent_dict[key]
                self.add_calculated_event_entry(event_dict, new_key, agent_dict[key])

    def _add_event_entry(self, event_dict, event_record_key, event_record_field, label, obj):
        if isinstance(event_record_field, StringField):
            if obj is None:
                # TODO: this should really check to see if the record_field is nullable.
                value = None
            else:
                value = backslash_encode_value(six.text_type(obj))
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


class TrackingEventRecordDataTask(EventLogSelectionMixin, BaseEventRecordDataTask):
    """Task to compute event_type and event_source values being encountered on each day in a given time interval."""

    # Override superclass to disable this parameter
    event_mapping = None
    PROJECT_NAME = 'tracking_prod'

    counter_category_name = 'Tracking Event Exports'

    def get_event_emission_time(self, event):
        return super(TrackingEventRecordDataTask, self).get_event_time(event)

    def get_event_arrival_time(self, event):
        try:
            return event['context']['received_at']
        except KeyError:
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
            field_keys = list(fields.keys())
            for field_key in field_keys:
                field_tuple = (field_key, fields[field_key])

                def add_event_mapping_entry(source_key):
                    self.event_mapping[source_key] = field_tuple
                # Most common is to map first-level entries in event data directly.
                # Skip values that are explicitly set in EventRecord:
                if field_key in ['version', 'input_file', 'project', 'event_type', 'event_source', 'context_course_id', 'username']:
                    pass
                # Skip values that are explicitly set in JSONEventRecord:
                elif field_key in ['source', 'emitter_type', 'raw_event']:
                    pass
                # Skip values that are explicitly calculated rather than copied:
                elif field_key.startswith('agent_') or field_key in ['event_category', 'timestamp', 'received_at', 'date']:
                    pass
                elif self.uses_JSON_event_record() and field_key == 'course_id':
                    pass
                elif self.uses_JSON_event_record() and field_key == 'referrer':
                    add_event_mapping_entry('root.referer')
                elif self.uses_JSON_event_record() and field_key == 'user_id':
                    add_event_mapping_entry('root.context.user_id')
                # Handle special-cases:
                elif field_key == "currenttime":
                    # Collapse values from either form into a single column.  No event should have both,
                    # though there are event_types that have used both at different times.
                    add_event_mapping_entry('root.event.currenttime')
                    add_event_mapping_entry('root.event.current_time')
                elif field_key in ['discussion_id', 'response_id', 'options_followed']:
                    add_event_mapping_entry(u"root.event.{}".format(field_key.replace('_', '.')))
                elif field_key in ['app_name', 'app_version']:
                    add_event_mapping_entry(u"root.context.application.{}".format(field_key[len('app_'):]))
                elif field_key == "old_value":
                    add_event_mapping_entry('root.event.old')
                elif field_key == "new_value":
                    add_event_mapping_entry('root.event.new')
                # Map values that are top-level:
                elif field_key in ['host', 'ip', 'page', 'referer', 'session', 'agent', 'accept_language']:
                    add_event_mapping_entry(u"root.{}".format(field_key))
                elif field_key.startswith('context_module_'):
                    add_event_mapping_entry(u"root.context.module.{}".format(field_key[15:]))
                elif field_key.startswith('context_'):
                    add_event_mapping_entry(u"root.context.{}".format(field_key[8:]))
                elif field_key in ['event_user', 'event_username']:
                    add_event_mapping_entry(u"root.event.{}".format(field_key[6:]))
                else:
                    add_event_mapping_entry(u"root.event.{}".format(field_key))

        return self.event_mapping

    def mapper(self, line):
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
            if self.uses_JSON_event_record():
                event_url = event_type
                event_type = 'edx.server.request'
            else:
                self.incr_counter(self.counter_category_name, 'Discard Implicit Events', 1)
                return

        username = event.get('username', '').strip()
        # if not username:
        #   return

        course_id = eventlog.get_course_id(event)
        # if not course_id:
        #   return

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            self.incr_counter(self.counter_category_name, 'Discard Missing Event Data', 1)
            return
        # Put the fixed value back, so it can be properly mapped.
        event['event'] = event_data

        event_source = event.get('event_source')
        if event_source is None:
            self.incr_counter(self.counter_category_name, 'Discard Missing Event Source', 1)
            return

        project_name = self.PROJECT_NAME

        event_dict = {}
        self.add_calculated_event_entry(event_dict, 'input_file', self.get_map_input_file())
        self.add_calculated_event_entry(event_dict, 'event_type', event_type)
        self.add_calculated_event_entry(event_dict, 'timestamp', self.get_event_emission_time(event))
        self.add_calculated_event_entry(event_dict, 'received_at', self.get_event_arrival_time(event))
        self.add_calculated_event_entry(event_dict, 'date', self.convert_date(date_received))
        self.add_calculated_event_entry(event_dict, 'username', username)
        self.add_agent_info(event_dict, event.get('agent'))

        if self.uses_JSON_event_record():
            # Add a check in the payload for a course_id -- it is sometimes there
            # instead of in context.
            if not course_id and event_data.get('course_id'):
                course_id = event_data.get('course_id')

            # was project
            self.add_calculated_event_entry(event_dict, 'source', project_name)
            # was event_source
            self.add_calculated_event_entry(event_dict, 'emitter_type', event_source)
            # was context_course_id
            self.add_calculated_event_entry(event_dict, 'course_id', course_id)
            self.add_calculated_event_entry(event_dict, 'raw_event', json.dumps(event, sort_keys=True))

            # Additional fields:
            # The event_url is the original event_type of implicit events.
            if event_url is not None:
                self.add_calculated_event_entry(event_dict, 'url', event_url)
            # Try to extract information from course_id.
            if course_id is not None:
                org_id = get_org_id_for_course(course_id)
                if org_id:
                    self.add_calculated_event_entry(event_dict, 'org_id', org_id)

        else:
            if (event_source, event_type) in self.known_events:
                event_category = self.known_events[(event_source, event_type)]
            else:
                event_category = 'unknown'

            self.add_calculated_event_entry(event_dict, 'version', VERSION)
            self.add_calculated_event_entry(event_dict, 'project', project_name)
            self.add_calculated_event_entry(event_dict, 'event_source', event_source)
            self.add_calculated_event_entry(event_dict, 'event_category', event_category)
            self.add_calculated_event_entry(event_dict, 'context_course_id', course_id)

        event_mapping = self.get_event_mapping()
        self.add_event_info(event_dict, event_mapping, event)

        record = self.get_event_record_class()(**event_dict)
        key = (date_received, project_name)

        self.incr_counter(self.counter_category_name, 'Output From Mapper', 1)

        # Convert to form for output by reducer here,
        # so that reducer doesn't do any conversion.
        # yield key, record.to_string_tuple()
        yield key, record.to_separated_values()


class SegmentEventLogSelectionDownstreamMixin(EventLogSelectionDownstreamMixin):
    """Defines parameters for passing upstream to tasks that use SegmentEventLogSelectionMixin."""

    source = luigi.ListParameter(
        config_path={'section': 'segment-logs', 'name': 'source'},
        description='A URL to a path that contains log files that contain the events. (e.g., s3://my_bucket/foo/).   Segment-logs',
    )
    pattern = luigi.ListParameter(
        config_path={'section': 'segment-logs', 'name': 'pattern'},
        description='A regex with a named capture group for the date or timestamp that approximates the date that the events '
        'within were emitted. Note that the search interval is expanded, so events don\'t have to be in exactly '
        'the right file in order for them to be processed.  Segment-logs',
    )


class SegmentEventLogSelectionMixin(SegmentEventLogSelectionDownstreamMixin, EventLogSelectionMixin):
    pass


class SegmentEventRecordDataTask(SegmentEventLogSelectionMixin, BaseEventRecordDataTask):
    """Task to compute event_type and event_source values being encountered on each day in a given time interval."""

    # Project information, pulled from config file.
    project_names = {}
    config = None

    event_mapping = None

    counter_category_name = 'Segment Event Exports'

    # TODO: this never actually worked in a cluster.  Figure out how to get it to work.
    def _get_project_name(self, project_id):
        if project_id not in self.project_names:
            if self.config is None:
                self.config = get_config()
            section_name = 'segment:' + project_id
            project_name = self.config.get(section_name, 'project_name', None)
            self.project_names[project_id] = project_name
        return self.project_names[project_id]

    def _get_time_from_segment_event(self, event, key):
        # This was written to deal with a broad spectrum of timestamp formats that
        # were appearing in events sent to Segment.  The greatest variation seemed
        # to stem from events emitted directly from mobile devices, particularly iOS.
        # There are many counters in place to allow for ongoing quality analysis.
        # A spot check in March, 2019 found no quality counters being triggered for
        # any of our Segment projects.
        try:
            event_time = event[key]
            event_time = self.normalize_time(event_time)
            if event_time is None:
                # Try again, with a more powerful (and more flexible) parser.
                try:
                    event_time = self.extended_normalize_time(event[key])
                    if event_time is None:
                        log.error("Really unparseable %s time from event: %r", key, event)
                        self.incr_counter(self.counter_category_name, 'Quality Unparseable {} Time Field'.format(key), 1)
                    else:
                        # Log this for now, until we have confidence this is reasonable.
                        log.warning("Parsable unparseable type for %s time in event: %r", key, event)
                        self.incr_counter(self.counter_category_name, 'Quality Parsable unparseable for {} Time Field'.format(key), 1)
                except Exception:
                    # This was commented out in the JSON event code because presumably it was happening a lot
                    # in cases where it was using multipe key values (e.g. get_event_arrival_time).
                    log.error("Unparseable %s time from event: %r", key, event)
                    self.incr_counter(self.counter_category_name, 'Quality Unparseable {} Time Field'.format(key), 1)
            return event_time
        except KeyError:
            log.error("Missing %s time from event: %r", key, event)
            self.incr_counter(self.counter_category_name, 'Quality Missing {} Time Field'.format(key), 1)
            return None
        except TypeError:
            log.error("Bad type for %s time in event: %r", key, event)
            self.incr_counter(self.counter_category_name, 'Quality Bad type for {} Time Field'.format(key), 1)
            return None
        except UnicodeEncodeError:
            # This is more specific than ValueError, so it is processed first.
            log.error("Bad encoding for %s time in event: %r", key, event)
            self.incr_counter(self.counter_category_name, 'Quality Bad encoding for {} Time Field'.format(key), 1)
            return None
        except ValueError:
            # Try again, with a more powerful (and more flexible) parser.
            try:
                event_time = self.extended_normalize_time(event[key])
                if event_time is None:
                    log.error("Unparseable %s time from event: %r", key, event)
                    self.incr_counter(self.counter_category_name, 'Quality Unparseable {} Time Field'.format(key), 1)
                else:
                    # Log this for now, until we have confidence this is reasonable.
                    log.warning("Parsable bad value for %s time in event: %r", key, event)
                    self.incr_counter(self.counter_category_name, 'Quality Parsable bad value for {} Time Field'.format(key), 1)
                return event_time
            except Exception:
                log.error("Bad value for %s time in event: %r", key, event)
                self.incr_counter(self.counter_category_name, 'Quality Bad value for {} Time Field'.format(key), 1)
            return None

    def get_event_arrival_time(self, event):
        # This gets the arrival time from 'receivedAt', which seems to always succeed.
        # No counters are appearing for event arrival being found from other values.
        if 'receivedAt' in event:
            return self._get_time_from_segment_event(event, 'receivedAt')

        if 'requestTime' in event:
            self.incr_counter(self.counter_category_name, 'Event arrival from requestTime', 1)
            return self._get_time_from_segment_event(event, 'requestTime')

        if 'timestamp' in event:
            self.incr_counter(self.counter_category_name, 'Event arrival from timestamp', 1)
            return self._get_time_from_segment_event(event, 'timestamp')

        self.incr_counter(self.counter_category_name, 'Event arrival not set', 1)
        log.error("Missing event arrival time in event '%r'", event)

        return None

    def get_event_emission_time(self, event):
        # The "originalTimestamp" is generally close to "sentAt", but not always.
        # It is the value provided by the host system that is emitting the event,
        # which could be a browser with a wildly different time.  The "receivedAt"
        # timestamp, in contrast, is a Segment server, so its timestamps are
        # presumably of better quality.  Segment then assumes that the "sentAt" and
        # "receivedAt" times are the same, and uses that to correct the original timestamp:
        #
        # "timestamp" = "originalTimestamp" + ("receivedAt" - "sentAt")
        #
        # So try to use the timestamp, if possible, and only fall back to 'sentAt' if
        # it is not found.
        if 'timestamp' in event:
            return self._get_time_from_segment_event(event, 'timestamp')

        # We don't expect this to be used, but add a counter to allow us to verify.
        self.incr_counter(self.counter_category_name, 'Event emission from sentAt', 1)
        return self._get_time_from_segment_event(event, 'sentAt')

    def get_event_time(self, event):
        """
        Returns time information from event if present, else returns None.

        Overrides base class implementation to get correct timestamp
        used by get_event_and_date_string(line).

        """
        return self.get_event_arrival_time(event)

    def get_event_mapping(self):
        """Return dictionary of event attributes to the output keys they map to."""
        if self.event_mapping is None:
            self.event_mapping = {}
            fields = self.get_event_record_class().get_fields()
            field_keys = list(fields.keys())
            for field_key in field_keys:
                field_tuple = (field_key, fields[field_key])

                def add_event_mapping_entry(source_key):
                    self.event_mapping[source_key] = field_tuple

                # Most common is to map first-level entries in event data directly.
                # Skip values that are explicitly set:
                if field_key in ['version', 'input_file', 'project', 'event_type', 'event_source']:
                    pass
                # Skip values that are explicitly calculated rather than copied:
                elif field_key.startswith('agent_') or field_key in ['event_category', 'timestamp', 'received_at', 'date']:
                    pass
                # Skip values that are explicitly set or calculated for JSONEventRecord:
                elif field_key in ['emitter_type', 'source', 'raw_event']:
                    pass
                # Map values that are top-level:
                elif field_key in ['channel']:
                    add_event_mapping_entry(u"root.{}".format(field_key))
                elif field_key in ['anonymous_id']:
                    add_event_mapping_entry(u"root.context.anonymousid")
                    add_event_mapping_entry("root.anonymousid")
                    add_event_mapping_entry(u"root.context.traits.anonymousid")
                    add_event_mapping_entry(u"root.traits.anonymousid")
                elif field_key in ['agent']:
                    add_event_mapping_entry(u"root.context.useragent")
                    add_event_mapping_entry(u"root.properties.context.agent")
                elif field_key in ['course_id']:
                    # This is sometimes a course, but not always.
                    # add_event_mapping_entry(u"root.properties.label")
                    add_event_mapping_entry(u"root.properties.courseid")
                    add_event_mapping_entry(u"root.properties.course_id")
                    add_event_mapping_entry(u"root.properties.course")
                    add_event_mapping_entry(u"root.properties.data.course_id")
                    add_event_mapping_entry(u"root.properties.data.course-id")
                    add_event_mapping_entry(u"root.properties.context.course_id")
                elif field_key in ['username']:
                    add_event_mapping_entry(u"root.traits.username")
                    add_event_mapping_entry(u"root.properties.context.username")
                    add_event_mapping_entry(u"root.context.traits.username")
                elif field_key in ['client_id', 'host', 'session', 'referer']:
                    add_event_mapping_entry(u"root.properties.context.{}".format(field_key))
                elif field_key in ['user_id']:
                    add_event_mapping_entry(u"root.context.user_id")
                    # I think this is more often a username than an id.
                    # TODO: figure it out later...  Exception is type=page,
                    # for which it's an id?  No, that's not consistent,
                    # even for the same projectId.  We may need more complicated
                    # logic to help sort that out (more) consistently.
                    add_event_mapping_entry(u"root.userid")
                    add_event_mapping_entry(u"root.properties.context.user_id")
                    add_event_mapping_entry(u"root.properties.data.user_id")
                    add_event_mapping_entry(u"root.context.traits.userid")
                    add_event_mapping_entry(u"root.traits.userid")
                elif field_key in [
                        'os_name', 'os_version', 'app_name', 'app_version', 'device_manufacturer',
                        'device_model', 'network_carrier', 'screen_width', 'screen_height',
                        'campaign_source', 'campaign_medium', 'campaign_content', 'campaign_name'
                ]:
                    add_event_mapping_entry(u"root.context.{}".format(field_key.replace('_', '.')))
                elif field_key in ['action']:
                    add_event_mapping_entry(u"root.properties.{}".format(field_key))
                elif field_key in ['locale', 'ip', 'timezone']:
                    add_event_mapping_entry(u"root.context.{}".format(field_key))
                    add_event_mapping_entry(u"root.properties.context.{}".format(field_key))
                elif field_key in ['path', 'referrer', 'search', 'title', 'url', 'variationname', 'variationid', 'experimentid', 'experimentname', 'category', 'label', 'display_name']:
                    add_event_mapping_entry(u"root.properties.{}".format(field_key))
                    add_event_mapping_entry(u"root.context.page.{}".format(field_key))
                    add_event_mapping_entry(u"root.properties.context.page.{}".format(field_key))
                    add_event_mapping_entry(u"root.data.{}".format(field_key))
                else:
                    pass

        return self.event_mapping

    def mapper(self, line):
        self.incr_counter(self.counter_category_name, 'Inputs', 1)

        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_received = value
        self.incr_counter(self.counter_category_name, 'Inputs with Dates', 1)

        segment_type = event.get('type')
        if segment_type is None and 'action' in event:
            segment_type = event.get('action').lower()
        self.incr_counter(self.counter_category_name, u'Subset Type {}'.format(segment_type), 1)

        channel = event.get('channel')
        self.incr_counter(self.counter_category_name, u'Subset Channel {}'.format(channel), 1)

        if segment_type == 'track':
            event_type = event.get('event')

            if event_type is None or date_received is None:
                # Ignore if any of the keys is None.  A spot check in March 2019 found it wasn't happening.
                self.incr_counter(self.counter_category_name, 'Discard Tracking with missing type', 1)
                return

            # Not all 'track' events have event_source information.  In particular, edx.bi.XX events.
            # Their 'properties' lack any 'context', having only label and category.

            event_category = event.get('properties', {}).get('category')
            if channel == 'server':
                event_source = event.get('properties', {}).get('context', {}).get('event_source')
                if event_source is None:
                    event_source = 'track-server'
                elif (event_source, event_type) in self.known_events:
                    event_category = self.known_events[(event_source, event_type)]
                self.incr_counter(self.counter_category_name, 'Subset Type track And Channel server', 1)
            else:
                # expect that channel is 'client'.
                event_source = channel
                self.incr_counter(self.counter_category_name, 'Subset Type track And Channel Not server', 1)

        else:
            # type is 'page' or 'identify' or 'screen'
            event_category = segment_type
            event_type = segment_type
            event_source = channel

        project_id = event.get('projectId')
        project_name = self._get_project_name(project_id) or project_id

        self.incr_counter(self.counter_category_name, u'Subset Project {}'.format(project_name), 1)

        event_dict = {}
        self.add_calculated_event_entry(event_dict, 'input_file', self.get_map_input_file())
        self.add_calculated_event_entry(event_dict, 'event_type', event_type)
        self.add_calculated_event_entry(event_dict, 'timestamp', self.get_event_emission_time(event))
        self.add_calculated_event_entry(event_dict, 'received_at', self.get_event_arrival_time(event))
        self.add_calculated_event_entry(event_dict, 'date', self.convert_date(date_received))

        # An issue with the original logic: if a key exists and contains a value of None, then None will
        # be returned instead of an empty dict specified as the default, and the next get() will fail.
        # So check specifically for non-false values.
        # self.add_agent_info(event_dict, event.get('context', {}).get('userAgent'))
        # self.add_agent_info(event_dict, event.get('properties', {}).get('context', {}).get('agent'))
        if event.get('context'):
            self.add_agent_info(event_dict, event.get('context').get('userAgent'))
        properties = event.get('properties')
        if properties and properties.get('context'):
            self.add_agent_info(event_dict, properties.get('context').get('agent'))

        if self.uses_JSON_event_record():
            self.add_calculated_event_entry(event_dict, 'source', project_name)  # was 'project'
            self.add_calculated_event_entry(event_dict, 'emitter_type', event_source)  # was 'event_source'

            # TODO: figure out why we check for this here, and not much earlier.  Why would
            # it be in event_type, but not in event_dict??  And why so bad if it's not found?
            # Is it required and cannot be 'None'? A spot check in March 2019 found it wasn't happening.
            if event_dict.get("event_type") is None:
                self.incr_counter(self.counter_category_name, 'Missing event_type field', 1)
                return

            self.add_calculated_event_entry(event_dict, 'raw_event', json.dumps(event, sort_keys=True))

        else:
            self.add_calculated_event_entry(event_dict, 'version', VERSION)
            self.add_calculated_event_entry(event_dict, 'project', project_name)
            self.add_calculated_event_entry(event_dict, 'event_source', event_source)
            self.add_calculated_event_entry(event_dict, 'event_category', event_category)

        event_mapping = self.get_event_mapping()
        self.add_event_info(event_dict, event_mapping, event)

        if self.uses_JSON_event_record():
            # Try harder to extract course_id and related information.
            course_id = event_dict.get('course_id')
            if course_id is None:
                # course_id may be stored in 'label', so try to parse what is there.
                label = event_dict.get('label')
                if label and is_valid_course_id(label):
                    self.add_calculated_event_entry(event_dict, 'course_id', label)
                    course_id = event_dict.get('course_id')

            if course_id is None:
                # course_id may be extractable from 'url' in the usual
                # way, so try to parse what is there.
                url = event_dict.get('url')
                course_key = get_course_key_from_url(url)
                if course_key:
                    course_id = six.text_type(course_key)
                    self.add_calculated_event_entry(event_dict, 'course_id', course_id)
                elif url:
                    # course_id may be extractable from 'url' by looking for the
                    # version string and plus-delimiters explicitly anywhere in the URL.
                    match = NEW_COURSE_REGEX.match(url)
                    if match:
                        course_id_string = match.group('course_id')
                        if is_valid_course_id(course_id_string):
                            self.add_calculated_event_entry(event_dict, 'course_id', course_id_string)
                            course_id = event_dict.get('course_id')

            if course_id is not None:
                org_id = get_org_id_for_course(course_id)
                if org_id:
                    self.add_calculated_event_entry(event_dict, 'org_id', org_id)

        record = self.get_event_record_class()(**event_dict)
        key = (date_received, project_name)

        self.incr_counter(self.counter_category_name, 'Output From Mapper', 1)

        # Convert to form for output by reducer here,
        # so that reducer doesn't do any conversion.
        # yield key, record.to_string_tuple()
        yield key, record.to_separated_values()


##########################
# Bulk Loading into S3
##########################


class BulkEventRecordIntervalTask(EventRecordDownstreamMixin, luigi.WrapperTask):
    """Compute event information over a range of dates and insert the results into Hive."""

    interval = luigi.DateIntervalParameter(
        description='The range of dates for which to create event records.',
    )

    def requires(self):
        kwargs = {
            'output_root': self.warehouse_path,
            'events_list_file_path': self.events_list_file_path,
            'n_reduce_tasks': self.n_reduce_tasks,
            'interval': self.interval,
            'event_record_type': self.event_record_type,
        }
        yield (
            TrackingEventRecordDataTask(**kwargs),
            SegmentEventRecordDataTask(**kwargs),
        )

    def output(self):
        return [task.output() for task in self.requires()]


##########################
# Loading into S3 by Date
##########################


class PerDateEventRecordDataDownstreamMixin(EventRecordDataDownstreamMixin):

    """Common parameters and base classes used to pass parameters through the event record workflow."""

    # Required parameter
    date = luigi.DateParameter(
        description='Upper bound date for the end of the interval to analyze. Data produced before 00:00 on this'
                    ' date will be analyzed. This workflow is intended to run nightly and this parameter is intended'
                    ' to be set to "today\'s" date, so that all of yesterday\'s data is included and none of today\'s.'
    )

    # Override superclass to disable this parameter
    interval = None


class PerDateEventRecordDataMixin(PerDateEventRecordDataDownstreamMixin):

    def __init__(self, *args, **kwargs):
        super(BaseEventRecordDataTask, self).__init__(*args, **kwargs)
        self.interval = luigi.date_interval.Date.from_date(self.date)

    def output_path_for_key(self, key):
        """
        Output based on project.

        Output is in the form {warehouse_path}/event_records/dt={CCYY-MM-DD}/{project}.tsv,
        but output_root is assumed to be set externally to {warehouse_path}/event_records/dt={CCYY-MM-DD}.
        """
        _date_received, project = key

        return url_path_join(
            self.output_root,
            '{project}.tsv'.format(project=project),
        )


class PerDateTrackingEventRecordDataTask(PerDateEventRecordDataMixin, TrackingEventRecordDataTask):
    pass


class PerDateSegmentEventRecordDataTask(PerDateEventRecordDataMixin, SegmentEventRecordDataTask):
    pass


class PerDateGeneralEventRecordDataTask(PerDateEventRecordDataDownstreamMixin, luigi.WrapperTask):
    """Runs all Event Record tasks for a given time interval."""

    def requires(self):
        kwargs = {
            'event_record_type': self.event_record_type,
            'output_root': self.output_root,
            'events_list_file_path': self.events_list_file_path,
            'n_reduce_tasks': self.n_reduce_tasks,
            'date': self.date,
        }
        yield (
            PerDateTrackingEventRecordDataTask(**kwargs),
            PerDateSegmentEventRecordDataTask(**kwargs),
        )


class EventRecordTableTask(EventRecordClassMixin, BareHiveTableTask):
    """The hive table for event_record data."""

    @property
    def partition_by(self):
        return 'dt'

    @property
    def table(self):
        return EVENT_TABLE_NAME

    @property
    def columns(self):
        return self.get_event_record_class().get_hive_schema()


class EventRecordPartitionTask(EventRecordDownstreamMixin, HivePartitionTask):
    """The hive table partition for this engagement data."""

    date = luigi.DateParameter()
    interval = None

    @property
    def partition_value(self):
        """Use a dynamic partition value based on the date parameter."""
        return self.date.isoformat()  # pylint: disable=no-member

    @property
    def hive_table_task(self):
        return EventRecordTableTask(
            event_record_type=self.event_record_type,
            warehouse_path=self.warehouse_path,
            # overwrite=self.overwrite,
        )

    @property
    def data_task(self):
        return PerDateGeneralEventRecordDataTask(
            event_record_type=self.event_record_type,
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            output_root=self.partition_location,
            # overwrite=self.overwrite,
            events_list_file_path=self.events_list_file_path,
        )


class EventRecordIntervalTask(EventRecordDownstreamMixin, luigi.WrapperTask):
    """Compute event information over a range of dates and insert the results into Hive."""

    interval = luigi.DateIntervalParameter(
        description='The range of received dates for which to create event records.',
    )

    def requires(self):
        for date in reversed([d for d in self.interval]):  # pylint: disable=not-an-iterable
            # should_overwrite = date >= self.overwrite_from_date
            yield EventRecordPartitionTask(
                event_record_type=self.event_record_type,
                date=date,
                n_reduce_tasks=self.n_reduce_tasks,
                warehouse_path=self.warehouse_path,
                # overwrite=should_overwrite,
                # overwrite_from_date=self.overwrite_from_date,
                events_list_file_path=self.events_list_file_path,
            )

    def output(self):
        return [task.output() for task in self.requires()]

    def get_raw_data_tasks(self):
        """
        A generator that iterates through all tasks used to generate the data in each partition in the interval.

        This can be used by downstream map reduce jobs to read all of the raw data.
        """
        for task in self.requires():
            if isinstance(task, EventRecordPartitionTask):
                yield task.data_task


##########################
# Loading into Vertica
##########################


class LoadDailyEventRecordToVertica(EventRecordDownstreamMixin, VerticaCopyTask):

    # Required parameter
    date = luigi.DateParameter()

    @property
    def partition(self):
        """The table is partitioned by date."""
        return HivePartition('dt', self.date.isoformat())  # pylint: disable=no-member

    @property
    def insert_source_task(self):
        # For now, let's just get by with ExternalURL.
        hive_table = EVENT_TABLE_NAME
        partition_location = url_path_join(self.warehouse_path, hive_table, self.partition.path_spec) + '/'
        return ExternalURL(url=partition_location)

    @property
    def table(self):
        return EVENT_TABLE_NAME

# Just use the default default:  "created"
#    @property
#    def default_columns(self):
#        """List of tuples defining name and definition of automatically-filled columns."""
#        return None

    @property
    def auto_primary_key(self):
        # The default is to use 'id', which would cause a conflict with field already having that name.
        # But there seems to be little value in having such a column.
        return None

    @property
    def columns(self):
        return self.get_event_record_class().get_sql_schema()

    @property
    def table_partition_key(self):
        return 'date'


class LoadEventRecordIntervalToVertica(EventRecordDownstreamMixin, VerticaCopyTaskMixin, luigi.WrapperTask):
    """
    Loads the event records table from Hive into the Vertica data warehouse.

    """

    interval = luigi.DateIntervalParameter(
        description='The range of received dates for which to create event records.',
    )

    def requires(self):
        for date in reversed([d for d in self.interval]):  # pylint: disable=not-an-iterable
            # should_overwrite = date >= self.overwrite_from_date
            yield LoadDailyEventRecordToVertica(
                event_record_type=self.event_record_type,
                date=date,
                n_reduce_tasks=self.n_reduce_tasks,
                warehouse_path=self.warehouse_path,
                events_list_file_path=self.events_list_file_path,
                schema=self.schema,
                credentials=self.credentials,
            )

    def output(self):
        return [task.output() for task in self.requires()]


class EventRecordLoadDownstreamMixin(EventRecordDownstreamMixin):
    """Define parameters for entrypoint for loading events."""

    interval = luigi.DateIntervalParameter(
        description='The range of dates for which to load event records.',
    )

    retention_interval = luigi.TimeDeltaParameter(
        config_path={'section': 'vertica-export', 'name': 'event_retention_interval'},
        description='The number of days of events to retain in Vertica. If not set, no pruning will occur.',
        default=None,
    )


class PruneEventPartitionsInVertica(EventRecordLoadDownstreamMixin, SchemaManagementTask):
    """Drop partitions that are beyond a specified retention interval."""

    # Mask date parameter from SchemaManagementTask so that it is not required.
    date = None

    # Date of earliest current record in Vertica.  Once calculated, this is used to
    # create queries to delete the excess partitions.
    earliest_date = None

    # Override the standard roles here since these tables will be rather raw. We may want to restrict access to a
    # subset of users.
    roles = luigi.ListParameter(
        config_path={'section': 'vertica-export', 'name': 'restricted_roles'},
    )

    def requires(self):
        return {
            'source': LoadEventRecordIntervalToVertica(
                event_record_type=self.event_record_type,
                interval=self.interval,
                n_reduce_tasks=self.n_reduce_tasks,
                warehouse_path=self.warehouse_path,
                events_list_file_path=self.events_list_file_path,
                schema=self.schema,
                credentials=self.credentials,
            ),
            'credentials': ExternalURL(self.credentials)
        }

    @property
    def queries(self):
        query_list = [
            "GRANT USAGE ON SCHEMA {schema} TO {roles};".format(schema=self.schema, roles=self.vertica_roles),
            "GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {roles};".format(
                schema=self.schema,
                roles=self.vertica_roles
            ),
        ]
        # Check for pruning.
        if self.interval and self.earliest_date and self.retention_interval:
            earliest_date_to_retain = self.interval.date_b - self.retention_interval
            split_date = self.earliest_date.split('-')
            earliest_date = datetime.date(int(split_date[0]), int(split_date[1]), int(split_date[2]))
            pruning_interval = DateInterval(earliest_date, earliest_date_to_retain)
            log.debug("Looking to prune partitions from %s up to but not including %s", earliest_date, earliest_date_to_retain)
            for date in pruning_interval:
                query_list.append(
                    "SELECT DROP_PARTITION('{schema}.{table}', '{date}');".format(
                        schema=self.schema,
                        table=EVENT_TABLE_NAME,
                        date=date,
                    )
                )
        else:
            log.warning("No pruning of event records: missing parameters:  earliest date=%s, retention_interval=%s ",
                        self.earliest_date, self.retention_interval)
        return query_list

    @property
    def marker_name(self):
        return 'prune_event_partitions' + self.interval.date_b.strftime('%Y-%m-%d')

    def run(self):
        # First figure out what needs pruning.
        connection = self.output().connect()
        cursor = connection.cursor()
        query = "SELECT min(date) FROM {schema}.{table}".format(
            schema=self.schema,
            table=EVENT_TABLE_NAME,
        )
        log.debug(query)
        cursor.execute(query)
        row = cursor.fetchone()
        if row is None:
            connection.close()
            raise Exception('Failed to find data in table: {schema}.{table}'.format(schema=self.schema, table=EVENT_TABLE_NAME))

        self.earliest_date = row[0]
        log.debug("Found earliest date for data in table: %s", self.earliest_date)
        connection.close()

        # Then execute the grants and the pruning queries.
        super(PruneEventPartitionsInVertica, self).run()


class LoadEventsIntoWarehouseWorkflow(EventRecordLoadDownstreamMixin, VerticaCopyTaskMixin, luigi.WrapperTask):
    """
    Provides entry point for loading event data into warehouse.
    """

    def requires(self):
        return PruneEventPartitionsInVertica(
            event_record_type=self.event_record_type,
            interval=self.interval,
            n_reduce_tasks=self.n_reduce_tasks,
            warehouse_path=self.warehouse_path,
            events_list_file_path=self.events_list_file_path,
            schema=self.schema,
            credentials=self.credentials,
        )


##########################
# Loading into BigQuery
##########################


class LoadDailyEventRecordToBigQuery(EventRecordDownstreamMixin, BigQueryLoadTask):

    @property
    def table(self):
        if self.uses_JSON_event_record():
            return 'json_event_records'
        else:
            return 'event_records'

    @property
    def partitioning_type(self):
        """Set to 'DAY' in order to partition by day."""
        return 'DAY'

    @property
    def schema(self):
        return self.get_event_record_class().get_bigquery_schema()

    @property
    def insert_source_task(self):
        return ExternalURL(url=self.hive_partition_path(EVENT_TABLE_NAME, self.date))


class LoadEventRecordIntervalToBigQuery(EventRecordDownstreamMixin, BigQueryLoadDownstreamMixin, luigi.WrapperTask):
    """
    Loads the event records table from Hive into the BigQuery data warehouse.

    """

    interval = luigi.DateIntervalParameter(
        description='The range of dates for which to create event records.',
    )

    def requires(self):
        for date in reversed([d for d in self.interval]):  # pylint: disable=not-an-iterable
            # should_overwrite = date >= self.overwrite_from_date
            yield LoadDailyEventRecordToBigQuery(
                event_record_type=self.event_record_type,
                date=date,
                n_reduce_tasks=self.n_reduce_tasks,
                warehouse_path=self.warehouse_path,
                events_list_file_path=self.events_list_file_path,
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
