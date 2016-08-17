"""Collect info from the course blocks API for processing of course structure for subsequent query."""
from collections import defaultdict
import datetime
import json
import logging
from urllib import quote

import luigi
import requests

from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.util.opaque_key_util import get_filename_safe_course_id
from edx.analytics.tasks.load_internal_reporting_course import LoadInternalReportingCourseMixin, PullCourseStructureAPIData
from edx.analytics.tasks.util.hive import (
    WarehouseMixin, BareHiveTableTask, HivePartitionTask, HivePartition
)
from edx.analytics.tasks.util.obfuscate_util import backslash_encode_value

from edx.analytics.tasks.util.record import SparseRecord, StringField, DateField, IntegerField, FloatField, BooleanField
from edx.analytics.tasks.vertica_load import VerticaCopyTask, VerticaCopyTaskMixin

VERSION = '0.1.1'

log = logging.getLogger(__name__)


class AllCourseMixin(LoadInternalReportingCourseMixin):

    output_root = luigi.Parameter()

    def get_all_course_requires(self):
        kwargs = {
            'date': self.date,
            'warehouse_path': self.warehouse_path,
            'api_root_url': self.api_root_url,
            'api_access_token': self.api_access_token,
        }
        return {"all_course": PullCourseStructureAPIData(**kwargs)}

    def generate_course_list_from_file(self):
        with self.input()['all_course'].open('r') as input_file:
            course_info = json.load(input_file)
            result_list = course_info.get('results')
            for result in result_list:
                yield result.get('id').strip()

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
        return get_target_from_url(self.output_root, "_SUCCESS")

    def complete(self):
        return get_target_from_url(url_path_join(self.output_root, '_SUCCESS')).exists()


class BlocksPerCourseTask(LoadInternalReportingCourseMixin, luigi.Task):
    """Fetch info provide by course blocks, and output as TSV."""

    output_root = luigi.Parameter()
    course_id = luigi.Parameter()

    def requires(self):
        pass

    def get_api_request_headers(self):
        return {'authorization': ('Bearer ' + self.api_access_token), 'accept': 'application/json'}

    def get_course_block_info(self, course_id):
        # TODO: fix this hack.  It may not be needed now that all_blocks is specified, but it's not clear
        # who the "requesting" user is when calling like this.  (We have an API token, but no user logged in.)
        username = 'brianstaff'
        input_course_id = quote(course_id)
        # TODO: move requested_fields into a parameter, and overridden in a config file.
        # Try adding student_view_data here, to see what we get.  Hopefully we don't have to enumerate types on it.
        requested_fields = 'children,graded,format,student_view_multi_device,student_view_url,lms_web_url,lti_url,student_view_data'
        query_args = "?course_id={}&username={}&depth=all&all_blocks=true&requested_fields={}".format(
            input_course_id, username, requested_fields,
        )
        api_url = url_path_join(self.api_root_url, 'api', 'courses', 'v1', 'blocks', query_args)
        response = requests.get(url=api_url, headers=self.get_api_request_headers(), stream=True)
        if response.status_code != requests.codes.ok:  # pylint: disable=no-member
            msg = "Encountered status {} on request to API for {}".format(response.status_code, api_url)
            raise Exception(msg)
        block_info = json.loads(response.content)
        return block_info

    def get_output_path(self, course_id, output_root):
        suffix = 'json'
        safe_course_id = get_filename_safe_course_id(course_id)
        output_path = url_path_join(output_root, "{}_{}.{}".format(safe_course_id, 'blocks', suffix))
        return output_path

    def output_blocks_for_course(self, course_id, output_root):
        output_path = self.get_output_path(course_id, output_root)
        log.info('Writing output file: %s', output_path)
        output_file_target = get_target_from_url(output_path)
        with output_file_target.open('w') as output_file:
            block_info = self.get_course_block_info(course_id)
            # Add the course_id into the block output, since it's not explicitly there?
            # And convert back to JSON.
            block_info['course_id'] = course_id
            block_info_string = json.dumps(block_info)
            output_file.write(block_info_string)
            output_file.write('\n')

    def run(self):
        self.remove_output_on_overwrite()
        log.info("Fetching blocks for %s", self.course_id)
        self.output_blocks_for_course(self.course_id, self.output_root)
        log.info("Fetched blocks for %s", self.course_id)

    def output(self):
        return get_target_from_url(self.get_output_path(self.course_id, self.output_root))


class AllBlocksTask(AllCourseMixin, BlocksPerCourseTask):

    course_id = None

    def requires(self):
        return_value = {}
        parent = super(AllBlocksTask, self).requires()
        if parent is not None:
            return_value['parent'] = parent
        all_course = self.get_all_course_requires()
        return_value.update(all_course)
        return return_value

    def do_action_per_course(self, course_id, output_root):
        self.output_blocks_for_course(course_id, output_root)


class BlockRecord(SparseRecord):
    """Represents a block in a course."""

    # Metadata:
    version = StringField(length=20, nullable=False, description='blah.')
    course_id = StringField(length=255, nullable=True, description='blah.')

    # Basic information for blocks
    block_id = StringField(length=255, nullable=True, description='blah.')  # id?
    block_type = StringField(length=255, nullable=True, description='blah.')  # type
    display_name = StringField(length=255, nullable=True, description='blah.')
    
    graded = BooleanField(nullable=True, description='blah.')
    block_format = StringField(length=255, nullable=True, description='blah.')  # format
    student_view_multi_device = BooleanField(nullable=True, description='blah.')
    student_view_url = StringField(length=255, nullable=True, description='blah.')
    lms_web_url = StringField(length=255, nullable=True, description='blah.')
    lti_url = StringField(length=255, nullable=True, description='blah.')

    # Calculated values:
    depth = IntegerField(nullable=True, description='blah.')
    order_index = IntegerField(nullable=True, description='blah.')
    parent_id = StringField(length=255, nullable=True, description='blah.')
    ancestor_gen2_id = StringField(length=255, nullable=True, description='blah.')
    ancestor_gen3_id = StringField(length=255, nullable=True, description='blah.')
    ancestor_gen4_id = StringField(length=255, nullable=True, description='blah.')
    ancestor_gen5_id = StringField(length=255, nullable=True, description='blah.')
    ancestor_gen6_id = StringField(length=255, nullable=True, description='blah.')
    
    # Specific information from student_view_data (depending on block type):
    topic_id = StringField(length=255, nullable=True, description='blah.')
    duration = StringField(length=255, nullable=True, description='blah.')
    only_on_web = BooleanField(nullable=True, description='blah.')
    transcript_en_url = StringField(length=255, nullable=True, description='blah.')
    youtube_url = StringField(length=255, nullable=True, description='blah.')
    mobile_low_url = StringField(length=255, nullable=True, description='blah.')


class RecordMapper(object):
    """Load a record from a dictionary object, according to a given mapping."""

    record_mapping = None
    
    def _add_entry(self, record_dict, record_key, record_field, label, obj):
        if isinstance(record_field, StringField):
            if obj is None:
                # TODO: this should really check to see if the record_field is nullable.
                value = None
            else:
                value = backslash_encode_value(unicode(obj))
                # Avoid validation errors later due to length by truncating here.
                field_length = record_field.length
                value_length = len(value)
                # TODO: This implies that field_length is at least 4. 
                if value_length > field_length:
                    log.error("Record value length (%d) exceeds max length (%d) for field %s: %r", value_length, field_length, record_key, value)
                    value = u"{}...".format(value[:field_length - 4])
            record_dict[record_key] = value
        elif isinstance(record_field, IntegerField):
            try:
                record_dict[record_key] = int(obj)
            except ValueError:
                log.error('Unable to cast value to int for %s: %r', label, obj)
        elif isinstance(record_field, BooleanField):
            try:
                record_dict[record_key] = bool(obj)
            except ValueError:
                log.error('Unable to cast value to bool for %s: %r', label, obj)
        elif isinstance(record_field, FloatField):
            try:
                record_dict[record_key] = float(obj)
            except ValueError:
                log.error('Unable to cast value to float for %s: %r', label, obj)
        else:
            record_dict[record_key] = obj

    def _add_info_recurse(self, record_dict, record_mapping, obj, label):
        if obj is None:
            pass
        elif isinstance(obj, dict):
            for key in obj.keys():
                new_value = obj.get(key)
                # Normalize labels to be all lower-case, since all field (column) names are lowercased.
                new_label = u"{}.{}".format(label, key.lower())
                self._add_info_recurse(record_dict, record_mapping, new_value, new_label)
        elif isinstance(obj, list):
            # We will not output any values that are stored in lists.
            pass
        else:
            # We assume it's a single object, and look it up now.
            if label in record_mapping:
                record_key, record_field = record_mapping[label]
                self._add_entry(record_dict, record_key, record_field, label, obj)

    def add_info(self, record_dict, input_dict):
        self._add_info_recurse(record_dict, self._get_record_mapping(), input_dict, 'root')

    def add_calculated_entry(self, record_dict, record_key, obj):
        """Use this to explicitly add calculated entry values."""
        record_field = self.record_class().get_fields()[record_key]
        label = record_key
        self._add_entry(record_dict, record_key, record_field, label, obj)

    def _get_record_mapping(self):
        """Return dictionary of input_dict attributes to the output keys they map to."""
        if self.record_mapping is None:
            self.record_mapping = self.calculate_record_mapping()
        return self.record_mapping

    def record_class(self):
        raise NotImplementedError

    def calculate_record_mapping(self):
        raise NotImplementedError

    
class BlockRecordMapper(RecordMapper):

    def record_class(self):    
        return BlockRecord

    def calculate_record_mapping(self):
        """Return dictionary of block attributes to the output keys they map to."""
        record_mapping = {}
        fields = self.record_class().get_fields()
        field_keys = fields.keys()
        for field_key in field_keys:
            field_tuple = (field_key, fields[field_key])

            def add_event_mapping_entry(source_key):
                record_mapping[source_key] = field_tuple

            # Most common is to map first-level entries in event data directly.
            # Skip values that are explicitly set:
            if field_key in ['version']:
                pass
            # Skip values that are explicitly calculated rather than copied:
            elif field_key.startswith('ancestor_') or field_key in ['depth', 'parent_id', 'order_index']:
                pass
            # Handle special-cases:
            elif field_key.startswith('block_'):
                add_event_mapping_entry(u"root.{}".format(field_key[len('block_'):]))
            
            # Map values that are top-level:
            elif field_key in ['course_id', 'display_name', 'graded', 'student_view_multi_device', 'student_view_url', 'lms_web_url', 'lti_url']:
                add_event_mapping_entry(u"root.{}".format(field_key))
            # Map student_view_data values:
            elif field_key in ['topic_id', 'duration', 'only_on_web']:
                add_event_mapping_entry(u"root.student_view_data.{}".format(field_key))
            elif field_key in ['transcript_en_url']:
                add_event_mapping_entry(u"root.student_view_data.transcripts.en")
            elif field_key in ['youtube_url']:
                add_event_mapping_entry(u"root.student_view_data.encoded_videos.youtube.url")
            elif field_key in ['mobile_low_url']:
                add_event_mapping_entry(u"root.student_view_data.encoded_videos.mobile_low.url")
            else:
                # TODO: figure out which to use this for: top-level or student_view_data
                # (For now, explicitly enumerate everything.)
                pass

        return record_mapping


class BlockRecordsPerCourseTask(LoadInternalReportingCourseMixin, luigi.Task):

    course_id = luigi.Parameter()
    date = luigi.DateParameter(default=None, description='blah')

    def __init__(self, *args, **kwargs):
        super(BlockRecordsPerCourseTask, self).__init__(*args, **kwargs)

        if not self.date:
            self.date = datetime.datetime.utcnow().date()
    
    def output(self):
        record_table_name = 'course-block-records'
        dummy_partition = HivePartition('dt', self.date.isoformat())  # pylint: disable=no-member
        partition_path_spec = dummy_partition.path_spec
        suffix = 'tsv'
        safe_course_id = get_filename_safe_course_id(self.course_id)
        output_pathname = "{}_{}.{}".format(safe_course_id, 'records', suffix)
        output_path = url_path_join(self.warehouse_path, record_table_name, partition_path_spec, output_pathname)
        return get_target_from_url(output_path)

    def requires(self):
        raw_table_name = 'course-block-raw'
        dummy_partition = HivePartition('dt', self.date.isoformat())  # pylint: disable=no-member
        partition_path_spec = dummy_partition.path_spec
        input_path = url_path_join(self.warehouse_path, raw_table_name, partition_path_spec + '/')
        kwargs = {
            'warehouse_path': self.warehouse_path,
            'api_root_url': self.api_root_url,
            'api_access_token': self.api_access_token,
            'course_id': self.course_id,
            'output_root': input_path,
        }
        return BlocksPerCourseTask(**kwargs)

    def run(self):
        mapper = BlockRecordMapper()
        with self.input().open('r') as input_file:
            course_blocks_info = json.load(input_file)
            course_id = course_blocks_info.get('course_id')
            block_dict = course_blocks_info.get('blocks')
            course_root = course_blocks_info.get('root')
            # TODO: make a first pass through blocks to construct child-to-parent
            # mapping to use for calculating depth and ancestors.
            # TODO: what to do if there are multiple parents or ancestors?
            # (Very likely to happen with drafts, anyway. Ugh.  Approximate,
            # for the cases where it does work.)
            child_map = defaultdict(list)
            parent_map = defaultdict(list)
            for block_id in block_dict:
                block = block_dict[block_id]
                children = block.get('children', [])
                for child_id in children:
                    child_map[block_id].append(child_id)
                    parent_map[child_id].append(block_id)

            # Traverse course from root, calculating depth and choosing parents.
            depth_map = {}
            preferred_parent = {}
            depth_map[course_root] = 1  # one-based, for now...
            def find_depth_for_children(block_id, depth):
                # Breadth-first, to make sure depth measurements are minima.
                for child_id in child_map[block_id]:
                    if child_id in depth_map:
                        prev_depth = depth_map[child_id]
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
                        depth_map[child_id] = depth + 1
                        preferred_parent[child_id] = block_id
                for child_id in child_map[block_id]:
                    find_depth_for_children(child_id, depth_map[child_id])

            # Now run the traversal
            find_depth_for_children(course_root, 1)

            # Traverse course again from root, calculating order_index values.
            order_index_map = {}
            order_index_map[course_root] = 1
            def find_order_index_for_children(block_id, current_index):
                """Returns the next index to use for siblings."""
                next_index = current_index + 1
                for child_id in child_map[block_id]:
                    order_index_map[child_id] = next_index
                    next_index = find_order_index_for_children(child_id, next_index)
                return next_index
            # Now run the traversal
            find_order_index_for_children(course_root, 1)
            
            with self.output().open('w') as output_file:
                for block_id in block_dict:
                    block = block_dict[block_id]
                    
                    record_dict = {'version': VERSION}
                    mapper.add_calculated_entry(record_dict, 'course_id', course_id)
                    depth = depth_map.get(block_id)
                    if depth:
                        mapper.add_calculated_entry(record_dict, 'depth', depth)
                    order_index = order_index_map.get(block_id)
                    if order_index:
                        mapper.add_calculated_entry(record_dict, 'order_index', order_index)
                    if block_id in preferred_parent:
                        parent_id = preferred_parent[block_id]
                        mapper.add_calculated_entry(record_dict, 'parent_id', parent_id)
                        if parent_id in preferred_parent:
                            ancestor_gen2_id = preferred_parent[parent_id]
                            mapper.add_calculated_entry(record_dict, 'ancestor_gen2_id', ancestor_gen2_id)
                            if ancestor_gen2_id in preferred_parent:
                                ancestor_gen3_id = preferred_parent[ancestor_gen2_id]
                                mapper.add_calculated_entry(record_dict, 'ancestor_gen3_id', ancestor_gen3_id)
                                if ancestor_gen3_id in preferred_parent:
                                    ancestor_gen4_id = preferred_parent[ancestor_gen3_id]
                                    mapper.add_calculated_entry(record_dict, 'ancestor_gen4_id', ancestor_gen4_id)
                                    if ancestor_gen4_id in preferred_parent:
                                        ancestor_gen5_id = preferred_parent[ancestor_gen4_id]
                                        mapper.add_calculated_entry(record_dict, 'ancestor_gen5_id', ancestor_gen5_id)
                                        if ancestor_gen5_id in preferred_parent:
                                            ancestor_gen6_id = preferred_parent[ancestor_gen5_id]
                                            mapper.add_calculated_entry(record_dict, 'ancestor_gen6_id', ancestor_gen6_id)
                        
                    mapper.add_info(record_dict, block)

                    record = mapper.record_class()(**record_dict)
                    record_string = record.to_separated_values()
                    output_file.write(record_string)
                    output_file.write('\n')


# Note that this class doesn't work as written, because we need a dynamic
# dependency.  That is, we need to run the base course task in order to know
# what requirements to output.

# But maybe we can stack it, by requiring that the course json dumps
# are done for all courses, and then if it were successful (by the presence
# of the marker), then we would run the output, which would similarly
# generate a marker when done.  So it's really two very different modes:
# either per-course or all-courses.
class AllBlockRecordsTask(AllCourseMixin, BlockRecordsPerCourseTask):

    course_id = None

    def requires(self):
        return_value = {}
        parent = super(AllBlockRecordsTask, self).requires()
        if parent is not None:
            return_value['parent'] = parent
        all_course = self.get_all_course_requires()
        return_value.update(all_course)
        return return_value

    def do_action_per_course(self, course_id, output_root):
        self.output_blocks_for_course(course_id, output_root)
    
