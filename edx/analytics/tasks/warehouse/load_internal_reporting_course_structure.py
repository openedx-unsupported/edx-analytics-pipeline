"""Collect info from the course blocks API for processing of course structure for subsequent query."""
from __future__ import absolute_import

import datetime
import json
import logging
import os

import luigi
from requests.exceptions import HTTPError
from six.moves import range

from edx.analytics.tasks.common.pathutil import PathSetTask
from edx.analytics.tasks.common.vertica_load import VerticaCopyTask
from edx.analytics.tasks.util.edx_api_client import EdxApiClient
from edx.analytics.tasks.util.hive import HivePartition, WarehouseMixin
from edx.analytics.tasks.util.opaque_key_util import get_filename_safe_course_id
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.record import (
    BooleanField, DateTimeField, FloatField, IntegerField, RecordMapper, SparseRecord, StringField
)
from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join

VERSION = '0.1.3'

log = logging.getLogger(__name__)


class CourseListDownstreamMixin(WarehouseMixin, OverwriteOutputMixin):
    """Common parameters used by the Course List Data and Partition tasks."""
    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        description='Default is today, UTC.',
    )
    course_list_api_root_url = luigi.Parameter(
        config_path={'section': 'course-list', 'name': 'api_root_url'},
        description="The base URL for the courses API. This URL should look like"
                    "https://lms.example.com/api/v1/courses/"
    )
    course_list_api_page_size = luigi.IntParameter(
        config_path={'section': 'course-list', 'name': 'page_size'},
        significant=False,
        default=100,
        description="The number of records to request from the API in each HTTP request."
    )

    @property
    def partition_value(self):
        """Partition based on the task's datetime and partition format strings."""
        return self.date.isoformat()


class PullCourseListApiData(CourseListDownstreamMixin, luigi.Task):
    """
    This task fetches the courses list from the Courses edX REST API, and
    writes one line of JSON for each course to the output() target.

    See the EdxRestClient to configure the REST API connection parameters.
    """

    def run(self):
        self.remove_output_on_overwrite()
        client = EdxApiClient()
        params = {
            'page_size': self.course_list_api_page_size,
        }

        def _pagination(response):
            """Gets the next URL from the course list API response."""
            return response.get('pagination', {}).get('next')

        counter = 0
        with self.output().open('w') as output_file:
            for response in client.paginated_get(self.course_list_api_root_url, params=params, pagination_key=_pagination):
                parsed_response = response.json()
                for course in parsed_response.get('results', []):
                    output_file.write(json.dumps(course))
                    output_file.write('\n')
                    counter += 1

        log.info('Wrote %d records to output file', counter)

    def output(self):
        return get_target_from_url(
            url_path_join(
                self.hive_partition_path('course_list_raw', partition_value=self.partition_value),
                'course_list.json'
            )
        )


class CourseBlocksDownstreamMixin(CourseListDownstreamMixin):
    course_blocks_api_root_url = luigi.Parameter(
        config_path={'section': 'course-blocks', 'name': 'api_root_url'},
        description="The base URL for the course blocks API. This URL should look like"
                    "https://lms.example.com/api/v1/blocks/"
    )
    course_blocks_api_token_type = luigi.Parameter(
        config_path={'section': 'course-blocks', 'name': 'api_token_type'},
        default='bearer',
        description="Type of authentication required for the API call, e.g. jwt or bearer."
    )


class AllCourseMixin(CourseBlocksDownstreamMixin):
    """Class to allow an action to be performed for all course_id values in the underlying course list."""

    output_root = luigi.Parameter()

    def get_all_course_requires(self):
        kwargs = {
            'date': self.date,
            'warehouse_path': self.warehouse_path,
            'course_list_api_root_url': self.course_list_api_root_url,
            'course_list_api_page_size': self.course_list_api_page_size,
        }
        return {"all_course": PullCourseListApiData(**kwargs)}

    def generate_course_list_from_file(self):
        count = 0
        with self.input()['all_course'].open('r') as input_file:
            for course_list_str in input_file:
                course_list = json.loads(course_list_str)
                course_id = course_list['course_id'].strip()
                yield course_id
                count += 1
                # For testing, put a circuit-breaker here.
                # if count > 10:
                #    return

    def do_action_per_course(self, course_id, output_root):
        raise NotImplementedError

    def run(self):
        self.remove_output_on_overwrite()
        for course_id in self.generate_course_list_from_file():
            log.info("Fetching blocks for %s", course_id)
            self.do_action_per_course(course_id, self.output_root)
            log.info("Fetched blocks for %s", course_id)

        with self.output().open('w') as output_file:
            output_file.write("DONE.")

    def output(self):
        return get_target_from_url(url_path_join(self.output_root, "_SUCCESS"))


class BlocksPerCourseTask(CourseBlocksDownstreamMixin, luigi.Task):
    """Fetch info provided by course blocks API for a specific course_id, and output as TSV."""

    output_root = luigi.Parameter()
    course_id = luigi.Parameter()
    client = None

    def requires(self):
        pass

    def get_course_block_info(self, course_id):
        # Cache the client once, so we don't have to keep constructing it.
        if not self.client:
            self.client = EdxApiClient(token_type=self.course_blocks_api_token_type)

        # 'display_name', 'student_view_url' and 'lms_web_url' are generally returned, and don't have to be requested.
        # 'lti_url' never seems to successfully return anything, sadly.
        # As of now, not sure what effect 'special_exam_info' has.
        # Not specifying 'nav_depth'.
        requested_fields = 'children,graded,format,student_view_multi_device,due,special_exam_info,visible_to_staff_only,show_correctness,type,lti_url'
        # To get information back about student views, one must list the block_type values to fetch for.  These are the only ones I found to return anything.
        student_view_data = 'video,discussion'
        params = dict(depth="all", requested_fields=requested_fields, all_blocks="true", student_view_data=student_view_data, course_id=course_id)
        try:
            # Course Blocks are returned on one page
            response = self.client.get(self.course_blocks_api_root_url, params=params)
        except HTTPError as error:
            # 404 errors may occur if we try to fetch the course blocks for a deleted course.
            # So we just log and ignore them.
            if error.response.status_code == 404:
                log.error('Error fetching API resource %s: %s', params, error)
            else:
                raise error
        else:
            parsed_response = response.json()
            return parsed_response

    def get_output_path(self, course_id, output_root):
        suffix = 'json'
        safe_course_id = get_filename_safe_course_id(course_id)
        output_path = url_path_join(output_root, "{}_{}.{}".format(safe_course_id, 'blocks', suffix))
        return output_path

    def output_blocks_for_course(self, course_id, output_root):
        output_path = self.get_output_path(course_id, output_root)
        block_info = self.get_course_block_info(course_id)
        if block_info is not None:
            # Add the course_id into the block output, since it's not explicitly there.
            # And convert back to JSON.
            block_info['course_id'] = course_id
            block_info_string = json.dumps(block_info)
            log.info('Writing output file: %s', output_path)
            output_file_target = get_target_from_url(output_path)
            with output_file_target.open('w') as output_file:
                output_file.write(block_info_string)
                output_file.write('\n')
        else:
            log.info('No blocks fetched for course_id %s', course_id)

    def run(self):
        self.remove_output_on_overwrite()
        log.info("Fetching blocks for %s", self.course_id)
        self.output_blocks_for_course(self.course_id, self.output_root)
        log.info("Fetched blocks for %s", self.course_id)

    def output(self):
        return get_target_from_url(self.get_output_path(self.course_id, self.output_root))


class AllCourseBlocksTask(AllCourseMixin, BlocksPerCourseTask):
    """Fetch info provided by course blocks API for a list of course_id values read from raw course_list JSON, and output as TSV."""

    course_id = None

    def requires(self):
        return_value = {}
        parent = super(AllCourseBlocksTask, self).requires()
        if parent is not None:
            return_value['parent'] = parent
        all_course = self.get_all_course_requires()
        return_value.update(all_course)
        return return_value

    def do_action_per_course(self, course_id, output_root):
        self.output_blocks_for_course(course_id, output_root)


class CourseBlockRecord(SparseRecord):
    """Represents a block in a course."""

    # Metadata:
    version = StringField(
        length=20, nullable=False,
        description='This is an internal version number, used to identify changes to the schema of this table.'
    )
    course_id = StringField(length=255, nullable=False, description='Identifier for the course containing the block.')

    # Basic information for blocks
    block_id = StringField(length=255, nullable=False, description='Global block identifier (with course information).'
                           '  Newer blocks will have an identifier like "block-v1:(course_id)+type@(block_type)+block@(local_block_id)".'
                           '  Older blocks will have an identifier like "i4x:/(course_id)/(block_type)/(local_block_id)".')

    block_type = StringField(length=255, nullable=False, description='Block type, e.g. `video`, `chapter`, `problem`.')
    display_name = StringField(
        length=255, nullable=False, truncate=True, normalize_whitespace=True,
        description='User-facing title of the block. Will be truncated to 255 characters.'
    )
    local_block_id = StringField(
        length=255, nullable=False, description='Local block identifier (without course or type).'
        '  For most blocks, it is a hash identifier. '
    )

    # Calculated values:
    depth = IntegerField(
        nullable=True, description='This indicates how deep the block is in the course structure.'
        '  It corresponds to the number of blocks one must travel through to reach the block from'
        ' the course block that is the root of the course structure for a course.'
        '  It is one-based, so a course block has depth one.  For blocks that appear in more than'
        ' one location in a course (e.g. a discussion block that is pointed to by multiple vertical blocks),'
        ' this will be the minimum depth of all those alternatives.'
    )
    order_index = IntegerField(
        nullable=True,
        description='This indicates the relative ordering of the block relative to other blocks in the same course.'
        '  Blocks are sorted in "pre-order tree traversal" order -- a depth-first search, with parents counted before'
        ' children and all children of one sibling counted before any of the next.'
        '  This approximates the order of presentation of blocks to the user.'
    )
    extra_parents = IntegerField(
        nullable=True, description='This counts up the number of times that this block was'
        ' found to have a different parent.  It is NULL if the block has only one parent,'
        ' and an integer > 0 if there were extras encountered.  Note that the'
        ' sum(extra_parents) = sum(detached_children) over a course.'
    )
    detached_children = IntegerField(
        nullable=True, description='This counts up the number of times that this block'
        ' was found to have a child that was ultimately assigned to a different parent.'
        '  It is NULL if the block has all of its children assigned to it, and an integer > 0'
        ' if there were children assigned elsewhere.'
        '  Note that the sum(extra_parents) = sum(detached_children) over a course.'
    )

    course_block_id = StringField(length=255, nullable=True, description='Block id of top-level course block.')
    section_block_id = StringField(length=255, nullable=True, description='Block id of second-level section block.')
    subsection_block_id = StringField(length=255, nullable=True, description='Block id of third-level subsection block.')
    unit_block_id = StringField(length=255, nullable=True, description='Block id of fourth-level unit block.')
    ancestor_level_5_id = StringField(length=255, nullable=True, description='Block id of fifth-level ancestor block.')
    ancestor_level_6_id = StringField(length=255, nullable=True, description='Block id of sixth-level ancestor block.')
    ancestor_level_7_id = StringField(length=255, nullable=True, description='Block id of seventh-level ancestor block.')

    parent_id = StringField(length=255, nullable=True, description='Block id of (first) shallowest parent.')
    ancestor_gen_2_id = StringField(length=255, nullable=True, description='Block id of grandparent.')
    ancestor_gen_3_id = StringField(length=255, nullable=True, description='Block id of great-grandparent.')
    ancestor_gen_4_id = StringField(length=255, nullable=True, description='Block id of great-great-grandparent.')
    ancestor_gen_5_id = StringField(length=255, nullable=True, description='Block id of great-great-great-grandparent.')
    ancestor_gen_6_id = StringField(length=255, nullable=True, description='Block id of great-great-great-great-grandparent.')
    ancestor_gen_7_id = StringField(length=255, nullable=True, description='Block id of great-great-great-great-great-grandparent.')

    # Additional metadata:
    block_format = StringField(length=255, nullable=True, description='')  # originally 'format'
    is_graded = BooleanField(nullable=True, description='True if the block is marked to be graded.')
    is_student_view_multi_device = BooleanField(nullable=True, description='')
    student_view_url = StringField(
        length=255, nullable=True, description='The URL that identifies the location of the course.'
        '  These all use "xblock" syntax (i.e. "https://courses.edx.org/xblock/(block_id)").'
    )
    lms_web_url = StringField(
        length=255, nullable=True, description='The URL for navigating to the current block from within the course.'
        '  These all use "jump_to" syntax (i.e. "https://courses.edx.org/courses/(course_id)/jump_to/(block_id)").'
    )
    is_visible_to_staff_only = BooleanField(nullable=True, description='True if block is only intended to be visible to staff.')
    show_correctness = StringField(length=255, nullable=True, description='Values of "always", "never", or "past_due".')
    due = DateTimeField(nullable=True, description='Due date for the block, or NULL if none is set.')

    # Specific information from student_view_data (depending on block type):
    # (block_type=discussion)
    topic_id = StringField(length=255, nullable=True, description='For discussion blocks only: the topic_id used by this discussion block.')
    # (block_type=video)
    duration = FloatField(nullable=True, description='For video blocks only:  the duration of the video (in seconds) if known.'
                          '  (Youtube videos generally lack duration values.)')
    only_on_web = BooleanField(nullable=True, description='For video blocks only:   True if the video is only available on the web.')
    youtube_video_url = StringField(
        length=255, nullable=True, description='For video blocks only:  the URL to the "youtube" video, or NULL if none is available.'
    )
    mobile_low_video_url = StringField(
        length=255, nullable=True, description='For video blocks only:  the URL to the "mobile_low" video, or NULL if none is available.'
    )
    mobile_high_video_url = StringField(
        length=255, nullable=True, description='For video blocks only:  the URL to the "mobile_high" video, or NULL if none is available.'
    )
    hls_video_url = StringField(
        length=255, nullable=True, description='For video blocks only:  the URL to the "hls" video, or NULL if none is available.'
    )
    fallback_video_url = StringField(
        length=255, nullable=True, description='For video blocks only: the URL to the "fallback" video, or NULL if none is available.'
    )
    transcript_en_url = StringField(
        length=255, nullable=True, description='For video blocks only:  the URL to the English transcript, or NULL if none is available.'
    )


class CourseBlockRecordMapper(RecordMapper):
    """A `RecordMapper` implementation for mapping block information from Course Blocks to a CourseBlockRecord."""

    @property
    def record_class(self):
        return CourseBlockRecord

    def add_record_field_mapping(self, field_key, add_event_mapping_entry):
        """For each field in a record, identify the location in the input dictionary to find its value."""

        # Skip values that are explicitly calculated and set using add_calculated_entry, rather than copied using add_info.
        if field_key in ['version']:
            pass
        elif field_key.startswith('ancestor_'):
            pass
        elif field_key in ['course_block_id', 'section_block_id', 'subsection_block_id', 'unit_block_id', 'parent_id']:
            pass

        # Explicitly map values that are top-level:
        elif field_key in ['course_id', 'display_name', 'student_view_url', 'lms_web_url',
                           'show_correctness', 'lti_url',
                           'due', 'depth', 'order_index', 'extra_parents', 'detached_children']:
            add_event_mapping_entry(u"root.{}".format(field_key))

        # Handle block_id from id
        elif field_key.startswith('block_'):
            add_event_mapping_entry(u"root.{}".format(field_key[len('block_'):]))
        # Handle booleans: is_graded, is_student_view_multi_device, is_visible_to_staff_only
        elif field_key.startswith('is_'):
            add_event_mapping_entry(u"root.{}".format(field_key[len('is_'):]))
        # Handle local_block_id
        elif field_key.startswith('local_'):
            add_event_mapping_entry(u"root.{}".format(field_key[len('local_'):]))
        # Map student_view_data root values:
        elif field_key in ['topic_id', 'duration', 'only_on_web']:
            add_event_mapping_entry(u"root.student_view_data.{}".format(field_key))
        elif field_key in ['transcript_en_url']:
            add_event_mapping_entry(u"root.student_view_data.transcripts.en")
        # Map youtube, mobile_low, mobile_high, fallback, hls
        elif field_key.endswith('_video_url'):
            add_event_mapping_entry(u"root.student_view_data.encoded_videos.{}.url".format(field_key[:-len('_video_url')]))
        else:
            # For now, ignore anything that isn't explicitly specified above.
            pass

    def populate_depth_map(self, course_root, block_dict):
        """
        Selects a parent for each child.

        For a given dictionary of blocks and a root block_id, it add several fields to each block:

          'depth': The number of blocks between itself and the root block, inclusive.  (So the root block has depth 1.)

          'extra_parents':  for each block, the number of "extra" parents (beyond the normal one).
                            So if a block has three parents, its 'extra_parents' property will exist and equal 2.

          'ancestor_list':   The list of ancestors for the block, starting at root and continuing to the parent.

          'detached_children':  The number of children for a block who were assigned different parents.

        """
        def find_depth_for_children(block_id, depth):
            """
            Find tree depth of block_id within course.

            Performs search breadth-first, to make sure depth values are minima for a block that may appear
            in multiple locations in a course.
            """
            parent_block = block_dict[block_id]
            children = parent_block.get('children', [])
            for child_id in children:
                child_block = block_dict[child_id]
                # If the block already has a 'depth' property, then we have already visited it.
                # We increment counters on the parent and child relationship that we are skipping,
                # and add debug logging.
                if 'depth' in child_block:
                    if 'extra_parents' in child_block:
                        child_block['extra_parents'] += 1
                    else:
                        child_block['extra_parents'] = 1

                    if 'detached_children' in parent_block:
                        parent_block['detached_children'] += 1
                    else:
                        parent_block['detached_children'] = 1

                    # When we find a block that has already been encountered, we log it and skip on.
                    # But we try to log a few details to understand better why there is a duplicate.
                    prev_depth = child_block['depth']
                    if prev_depth < depth + 1:
                        log.debug("Found block %s child of %s at depth %d but already depth %d", child_id, block_id, depth + 1, prev_depth)
                    elif prev_depth == depth + 1:
                        # found sibling.  Ignore.
                        log.debug("Found block %s child of %s at depth %d but already found at that depth",
                                  child_id, block_id, depth + 1)
                    else:
                        log.debug("Weird: Found block %s child of %s at depth %d but already depth %d",
                                  child_id, block_id, depth + 1, prev_depth)
                else:
                    # Prefer those parents that are encountered first in a breadth-first search.
                    child_block['depth'] = depth + 1
                    child_block['ancestor_list'] = list()
                    if 'ancestor_list' in parent_block:
                        child_block['ancestor_list'].extend(parent_block['ancestor_list'])
                    child_block['ancestor_list'].append(block_id)

            for child_id in children:
                find_depth_for_children(child_id, block_dict[child_id]['depth'])

        # Initialize the state of the root node.  We use a one-based depth notation.
        # Note that the root node does not have an 'ancestor_list' property.
        block_dict[course_root]['depth'] = 1

        # Now run the traversal.
        find_depth_for_children(course_root, 1)

    def populate_order_index_map(self, course_root, block_dict):
        """
        Blocks are sorted in "pre-order tree traversal" order:
        https://en.wikipedia.org/wiki/Tree_traversal#Pre-order

        Adds 'order_index' property to each block in block_dict.
        """
        def find_order_index_for_children(block_id, current_index):
            """Returns the next index to use for siblings."""
            next_index = current_index + 1
            children = block_dict[block_id].get('children', [])
            for child_id in children:
                (block_dict[child_id])['order_index'] = next_index
                next_index = find_order_index_for_children(child_id, next_index)
            return next_index

        # Now initialize the root node and run the traversal from there.
        (block_dict[course_root])['order_index'] = 1
        find_order_index_for_children(course_root, 1)

    def generate_records_from_course_blocks(self, course_blocks_info):
        """Returns record objects for all blocks in `course_blocks_info` for a given course."""

        course_id = course_blocks_info.get('course_id')
        block_dict = course_blocks_info.get('blocks')
        course_root = course_blocks_info.get('root')

        # Traverse course from root, calculating depth and choosing ancestors.
        self.populate_depth_map(course_root, block_dict)

        # Traverse course again from root, calculating order_index values.
        self.populate_order_index_map(course_root, block_dict)

        # Initialize labels for ancestors.
        ancestor_gen_labels = ["ancestor_gen_{}_id".format(gen_index) for gen_index in range(1, 8)]
        ancestor_gen_labels[0] = "parent_id"
        ancestor_level_labels = ["ancestor_level_{}_id".format(gen_index) for gen_index in range(1, 8)]
        ancestor_level_labels[0] = "course_block_id"
        ancestor_level_labels[1] = "section_block_id"
        ancestor_level_labels[2] = "subsection_block_id"
        ancestor_level_labels[3] = "unit_block_id"

        # Traverse each block, and populate a record for it:
        for block_id in block_dict:
            block = block_dict[block_id]

            record_dict = {'version': VERSION}
            self.add_calculated_entry(record_dict, 'course_id', course_id)

            ancestors = block['ancestor_list'] if 'ancestor_list' in block else list()
            num_ancestors = len(ancestors)
            for index, ancestor_id in enumerate(ancestors):
                # Add entry for ancestor "above" block
                gen_index = num_ancestors - 1 - index
                if gen_index < 7:
                    label = ancestor_gen_labels[gen_index]
                    self.add_calculated_entry(record_dict, label, ancestor_id)
                # Add entry for level below root:
                if index < 7:
                    label = ancestor_level_labels[index]
                    self.add_calculated_entry(record_dict, label, ancestor_id)

            self.add_info(record_dict, block)

            record = self.record_class(**record_dict)
            yield record


class BlockRecordsPerCourseTask(CourseBlocksDownstreamMixin, luigi.Task):
    """Outputs CourseBlockRecord strings as a single TSV file for all blocks in a course."""

    course_id = luigi.Parameter()

    def output(self):
        record_table_name = 'course_block_records'
        output_dir = self.hive_partition_path(record_table_name, self.date)
        safe_course_id = get_filename_safe_course_id(self.course_id)
        suffix = 'tsv'
        output_pathname = "{}_{}.{}".format(safe_course_id, 'records', suffix)
        output_path = url_path_join(output_dir, output_pathname)
        return get_target_from_url(output_path)

    def requires(self):
        raw_table_name = 'course_block_raw'
        input_path = self.hive_partition_path(raw_table_name, self.date)
        kwargs = {
            'warehouse_path': self.warehouse_path,
            'course_blocks_api_root_url': self.course_blocks_api_root_url,
            'course_id': self.course_id,
            'output_root': input_path,
            'date': self.date,
            'overwrite': self.overwrite,
        }
        return BlocksPerCourseTask(**kwargs)

    def run(self):
        mapper = CourseBlockRecordMapper()
        with self.input().open('r') as input_file:
            course_blocks_info = json.load(input_file)
            with self.output().open('w') as output_file:
                for record in mapper.generate_records_from_course_blocks(course_blocks_info):
                    record_string = record.to_separated_values()
                    output_file.write(record_string)
                    output_file.write('\n')


class AllCourseBlockRecordsTask(CourseBlocksDownstreamMixin, luigi.Task):
    """Outputs CourseBlockRecord strings for all courses as a single TSV file for all blocks in each course."""

    record_mapper = CourseBlockRecordMapper()

    @property
    def input_path(self):
        raw_table_name = 'course_block_raw'
        _input_path = self.hive_partition_path(raw_table_name, self.date)
        return _input_path

    @property
    def output_path(self):
        record_table_name = 'course_block_records'
        _output_path = self.hive_partition_path(record_table_name, self.date)
        return _output_path

    def get_output_target_for_course(self, course_id):
        suffix = 'tsv'
        safe_course_id = get_filename_safe_course_id(course_id)
        output_pathname = "{}_{}.{}".format(safe_course_id, 'records', suffix)
        return get_target_from_url(url_path_join(self.output_path, output_pathname))

    def requires(self):
        kwargs = {
            'warehouse_path': self.warehouse_path,
            'course_blocks_api_root_url': self.course_blocks_api_root_url,
            # 'api_access_token': self.api_access_token,
            'output_root': self.input_path,
            'date': self.date,
            'overwrite': self.overwrite,
        }
        return AllCourseBlocksTask(**kwargs)

    def generate_input_file_list(self):
        # Need to do a directory on self.input_path to get all .json
        # files.  At the moment, return a string, not a target.  We
        # convert it before opening.
        file_pattern = "*.json"
        path_set_task = PathSetTask([self.input_path], [file_pattern], include_zero_length=False)
        for input_task in path_set_task.generate_file_list():
            yield input_task.output().path

    def do_action_per_course(self, input_filepath):
        input_target = get_target_from_url(input_filepath)
        with input_target.open('r') as input_file:
            course_blocks_info = json.load(input_file)
            course_id = course_blocks_info.get('course_id')
            output_target_for_course = self.get_output_target_for_course(course_id)
            with output_target_for_course.open('w') as output_file:
                for record in self.record_mapper.generate_records_from_course_blocks(course_blocks_info):
                    record_string = record.to_separated_values()
                    output_file.write(record_string)
                    output_file.write('\n')

    def run(self):
        self.remove_output_on_overwrite()
        if self.overwrite:
            # Also delete all the output files, not just the marker.
            target = get_target_from_url(self.output_path)
            if target.exists():
                target.remove()

        for input_file in self.generate_input_file_list():
            log.info("Processing blocks from %s", input_file)
            self.do_action_per_course(input_file)
            log.info("Processed blocks from %s", input_file)

    def output(self):
        return get_target_from_url(self.output_path, marker=True)

    def on_success(self):  # pragma: no cover
        """Overload the success method to touch the _SUCCESS file.  Any class that uses a separate Marker file from the
        data file will need to override the base on_success() call to create this marker."""
        self.output().touch_marker()


class LoadCourseBlockRecordToVertica(CourseBlocksDownstreamMixin, VerticaCopyTask):

    def requires(self):
        if self.required_tasks is None:
            self.required_tasks = {
                'credentials': ExternalURL(url=self.credentials),
                'insert_source': self.insert_source_task,
            }
        return self.required_tasks

    @property
    def partition(self):
        """The table is partitioned by date."""
        return HivePartition('dt', self.date.isoformat())  # pylint: disable=no-member

    @property
    def insert_source_task(self):
        return AllCourseBlockRecordsTask(
            date=self.date,
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )

    @property
    def table(self):
        return 'course_block_records'

    @property
    def auto_primary_key(self):
        return None

    @property
    def columns(self):
        return CourseBlockRecord.get_sql_schema()


class LoadInternalReportingCourseStructureToWarehouse(WarehouseMixin, VerticaCopyTask):
    """
    Loads the most recent course_block_records table from intermediate storage into the Vertica data warehouse.
    """
    date = luigi.DateParameter()

    def __init__(self, *args, **kwargs):
        super(LoadInternalReportingCourseStructureToWarehouse, self).__init__(*args, **kwargs)
        path = url_path_join(self.warehouse_path, 'course_block_records')
        path_targets = PathSetTask([path]).output()
        paths = list(set([os.path.dirname(target.path) for target in path_targets]))
        dates = [path.rsplit('/', 2)[-1] for path in paths]
        latest_date = sorted(dates)[-1]

        self.load_date = datetime.datetime.strptime(latest_date, "dt=%Y-%m-%d").date()

    @property
    def insert_source_task(self):
        record_table_name = 'course_block_records'
        partition_location = self.hive_partition_path(record_table_name, self.load_date)
        return ExternalURL(url=partition_location)

    @property
    def table(self):
        return 'course_structure'

    @property
    def default_columns(self):
        """List of tuples defining name and definition of automatically-filled columns."""
        # Define a column to list when the original data was exported.
        return [('export_date', "DATE DEFAULT '{}'".format(self.load_date.isoformat()))]

    @property
    def auto_primary_key(self):
        return None

    @property
    def columns(self):
        return CourseBlockRecord.get_sql_schema()
