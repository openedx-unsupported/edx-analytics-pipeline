"""
Store course block details sourced from the Course Blocks API into a hive table.

See the CourseBlocksApiDataTask and CourseBlocksPartitionTask for details.
"""

import json
import logging

import luigi
from requests.exceptions import HTTPError

from edx.analytics.tasks.common.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.insights.course_list import CourseListApiDataTask, CourseRecord, TimestampPartitionMixin
from edx.analytics.tasks.util.edx_api_client import EdxApiClient
from edx.analytics.tasks.util.hive import BareHiveTableTask, HivePartitionTask, WarehouseMixin
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.record import BooleanField, IntegerField, Record, StringField
from edx.analytics.tasks.util.url import get_target_from_url, url_path_join

log = logging.getLogger(__name__)


class CourseBlockRecord(Record):
    """
    Represents a course block as fetched from the edX Course Blocks REST API, augmented with details about its
    position in the course.
    """
    block_id = StringField(length=564, nullable=False, description='Block identifier.')
    course_id = StringField(length=255, nullable=False, description='Identifier for the course containing the block.')
    block_type = StringField(length=255, nullable=False,
                             description='Block type, e.g. `video`, `chapter`, `problem`, `vectordraw`.')
    display_name = StringField(length=255, nullable=False, truncate=True, normalize_whitespace=True,
                               description='User-facing title of the block. Will be truncated to 255 characters.')
    is_root = BooleanField(default=False, nullable=False,
                           description='True if the block is the course\'s root node.')
    is_orphan = BooleanField(default=False, nullable=False,
                             description='True if the block has no parent nodes, but is not a root node.')
    is_dag = BooleanField(default=False, nullable=False,
                          description='True if the block has more than one parent, making the course a Directed '
                                      'Acyclic Graph.  If True, parent_block_id, course_path, and sort_idx will be set '
                                      'to the first place the block is found when traversing the course blocks tree.')
    parent_block_id = StringField(length=255, nullable=True,
                                  description='Block identifier for the block\'s parent.')
    course_path = StringField(nullable=True, normalize_whitespace=True,
                              description='Concatenated string of parent block display_name values, from '
                                          'the root node to the parent_block_id.')
    sort_idx = IntegerField(nullable=True,
                            description='Number indicating the position that this block holds in a course-outline '
                                        'sorted list of blocks. See `CourseBlocksApiDataTask.sort_orphan_blocks_up`.')


class CourseBlocksDownstreamMixin(TimestampPartitionMixin, WarehouseMixin, OverwriteOutputMixin):
    """Common parameters used by the Course Blocks Data and Partition tasks."""

    input_root = luigi.Parameter(
        description='URL pointing to the course_list partition data, containing the list of courses whose blocks will '
                    'be loaded.  Note that if this location does not already exist, it will be created by the '
                    'CourseListPartitionTask.'
    )
    partition_format = luigi.Parameter(
        config_path={'section': 'course-blocks', 'name': 'partition_format'},
        default='%Y-%m-%d',
        description='Format string for the course blocks table partition\'s `datetime` parameter. '
                    'Must result in a filename-safe string, or your partitions will fail to be created.\n'
                    'The default value of "%Y-%m-%d" changes daily, and so causes a new course partition to to be '
                    'created once a day.  For example, use "%Y-%m-%dT%H" to update hourly, though beware of load on '
                    'the edX REST API.  See strftime for options.',
    )


class PullCourseBlocksApiData(CourseBlocksDownstreamMixin, luigi.Task):
    """
    This task fetches the blocks from the Course Blocks edX REST API, and stores each course's result on a separate line
    of JSON.  Each line contains the "root" block ID, and a "blocks" dict of blocks.

    See the EdxApiClient to configure the REST API connection parameters.
    """
    api_root_url = luigi.Parameter(
        config_path={'section': 'course-blocks', 'name': 'api_root_url'},
        description="The base URL for the course blocks API. This URL should look like"
                    "https://lms.example.com/api/v1/blocks/"
    )
    api_token_type = luigi.Parameter(
        config_path={'section': 'course-blocks', 'name': 'api_token_type'},
        default='bearer',
        description="Type of authentication required for the API call, e.g. jwt or bearer."
    )

    def requires(self):
        return CourseListApiDataTask(
            datetime=self.datetime,
            output_root=self.input_root,
            overwrite=self.overwrite,
        )

    def run(self):
        self.remove_output_on_overwrite()

        courses = []
        with self.input().open('r') as course_list_file:
            for line in course_list_file:
                course = CourseRecord.from_tsv(line)
                courses.append(course.course_id)

        client = EdxApiClient(token_type=self.api_token_type)
        params = dict(depth="all", requested_fields="children", all_blocks="true")
        counter = 0
        with self.output().open('w') as output_file:
            for course_id in courses:  # pylint: disable=not-an-iterable
                params['course_id'] = course_id
                try:
                    # Course Blocks are returned on one page
                    response = client.get(self.api_root_url, params=params)
                except HTTPError as error:
                    # 404 errors may occur if we try to fetch the course blocks for a deleted course.
                    # So we just log and ignore them.
                    if error.response.status_code == 404:
                        log.error('Error fetching API resource %s: %s', params, error)
                    else:
                        raise error
                else:
                    parsed_response = response.json()
                    parsed_response['course_id'] = course_id
                    output_file.write(json.dumps(parsed_response))
                    output_file.write('\n')
                    counter += 1

        log.info('Wrote %d records to output file', counter)

    def output(self):
        return get_target_from_url(
            url_path_join(
                self.hive_partition_path('course_blocks_raw', partition_value=self.partition_value),
                'course_blocks.json'
            )
        )


class CourseBlocksApiDataTask(CourseBlocksDownstreamMixin, MapReduceJobTask):
    """
    This task processes the data returned by the Course Blocks API into CourseBlock string tuples.

    There are 4 types of blocks:
    * root: The root block is flagged by the Course Blocks REST API.  They are marked by `is_root=True`.  Will
      also have `parent_block_id=None`, `course_path=''`, and `sort_idx=0`.
    * child: Blocks which have a single parent will have `parent_block_id not None`, and a `course_path` string which
      concatenates the parent block's display_name values.
    * DAG: Blocks which have more than one parent (and thus create directed acyclic graphs, aka DAGs) are marked with
      `is_dag=True`.  Will have `parent_block_id` and `course_path` determined by the first place the block is found
       when traversing the course blocks tree.
    * Orphan: Blocks with no parents are marked with `is_orphan=True`.  Will have a `course_path` value configured by
      the `deleted_blocks_path` parameter.

    Blocks are sorted depth-first, using pre-order traversal.
    Orphan blocks can be sorted to the top or bottom of the list by adjusting the `sort_orphan_blocks_up` parameter.

    """
    output_root = luigi.Parameter(
        description='URL where the map reduce data should be stored.',
    )
    path_delimiter = luigi.Parameter(
        config_path={'section': 'course-blocks', 'name': 'path_delimiter'},
        default=' / ',
        description='String used to delimit the course path sections when assembling the full block location.',
    )
    deleted_blocks_path = luigi.Parameter(
        config_path={'section': 'course-blocks', 'name': 'deleted_blocks_path'},
        default='(Deleted block :)',
        description='Mark deleted (orphan) blocks with this string in course_path.',
    )
    sort_orphan_blocks_up = luigi.BoolParameter(
        config_path={'section': 'course-blocks', 'name': 'sort_orphan_blocks_up'},
        default=False,
        description='If True, any deleted (orphan) blocks will be pushed to the top of the list '
                    '(in an indeterminate order).  If False, orphan blocks will be pushed to the bottom.'
    )

    # Write the output directly to the final destination and rely on the _SUCCESS file to indicate
    # whether or not it is complete. Note that this is a custom extension to luigi.
    enable_direct_output = True

    def __init__(self, *args, **kwargs):
        super(CourseBlocksApiDataTask, self).__init__(*args, **kwargs)

    def requires(self):
        return PullCourseBlocksApiData(
            datetime=self.datetime,
            input_root=self.input_root,
            overwrite=self.overwrite,
        )

    def mapper(self, line):
        """
        Load each line of course blocks data, and ensure they have the `blocks` and `root` fields.
        Discard any invalid lines.

        Input is JSON-formatted data returned from the Course Blocks API.

        Yields a 2-element tuple containing the course_id and the parsed JSON data as a dict.
        """
        if line is None:
            return

        data = json.loads(line)
        root = data.get('root')
        blocks = data.get('blocks', {})
        course_id = data.get('course_id')
        if course_id is not None and root is not None and root in blocks:
            yield (course_id, data)
        else:
            log.error('Unable to read course blocks data from "%s"', line)

    def reducer(self, key, values):
        """
        Creates a CourseBlock record for each block, including:
        * `course_path` field concatenated from the block's parents' display_name values
        * `sort_idx` for the block, indicating where the block fits into the course.

        Input is the course block values as a list of dicts, keyed by course_id.

        Yields each CourseBlock record as a tuple, sorted in course pre-order tree traversal order.
        """
        course_id = key

        for course_data in values:
            root_id = course_data.get('root')
            blocks = course_data.get('blocks', {})
            self._index_children(root_id, blocks)

            # Sort unplaced (orphan, multi-parent) blocks towards the top or bottom, as configured
            if self.sort_orphan_blocks_up:
                no_sort_idx = -1
            else:
                no_sort_idx = len(blocks.keys())

            def order_by_sort_idx(key, blocks=blocks, default_sort_idx=no_sort_idx):
                """Function to sort the blocks on sort_idx"""
                return blocks[key].get('sort_idx', default_sort_idx)

            for block_id in sorted(blocks.keys(), key=order_by_sort_idx):
                block = blocks[block_id]
                is_root = (block_id == root_id)
                parents = block.get('parents', [])
                is_dag = block.get('is_dag', False)
                is_orphan = False
                course_path = u''
                sort_idx = block.get('sort_idx', no_sort_idx)

                if not is_root:
                    if len(parents) == 0:
                        is_orphan = True
                        course_path = unicode(self.deleted_blocks_path)
                    else:
                        for parent_id in parents:
                            parent = blocks[parent_id]
                            if len(course_path) > 0:
                                course_path += unicode(self.path_delimiter)
                            course_path += parent['display_name']

                record = CourseBlockRecord(
                    course_id=course_id,
                    block_id=block.get('id'),
                    block_type=block.get('type'),
                    display_name=block.get('display_name'),
                    parent_block_id=block.get('parent_block_id'),
                    is_root=is_root,
                    is_orphan=is_orphan,
                    is_dag=is_dag,
                    course_path=course_path,
                    sort_idx=sort_idx,
                )

                yield record.to_string_tuple()

    def output(self):
        return get_target_from_url(self.output_root)

    def complete(self):
        """
        The task is complete if the output_root/_SUCCESS file is present.
        """
        return get_target_from_url(url_path_join(self.output_root, '_SUCCESS')).exists()

    def run(self):
        """
        Clear out output if data is incomplete, or if overwrite requested.
        """
        if not self.complete():
            self.remove_output_on_overwrite()
        super(CourseBlocksApiDataTask, self).run()

    def _index_children(self, block_id, blocks, parent_block_id=None, sort_idx=0):
        """
        Applies a sort_idx and parents list to all the blocks in the list.
        Blocks are sorted in "pre-order tree traversal" order:
            https://en.wikipedia.org/wiki/Tree_traversal#Pre-order
        """
        if block_id in blocks:
            block = blocks[block_id]

            # If the block already has a sort_idx, then we've seen it before, and so it's a child of multiple parents.
            if 'sort_idx' in block:
                block['is_dag'] = True

            else:
                block['sort_idx'] = sort_idx
                sort_idx += 1

                if parent_block_id is not None:
                    parent = blocks[parent_block_id]
                    block['parents'] = parent.get('parents', [])[:]
                    block['parents'].append(parent_block_id)
                    block['parent_block_id'] = parent_block_id

                # Recurse on children
                for child_id in block.get('children', []):
                    sort_idx = self._index_children(child_id, blocks, block_id, sort_idx)

        return sort_idx


class CourseBlocksTableTask(BareHiveTableTask):
    """Hive table containing the sorted course block data, partitioned on formatted datetime."""

    @property
    def partition_by(self):
        return 'dt'

    @property
    def table(self):
        return 'course_blocks'

    @property
    def columns(self):
        return CourseBlockRecord.get_hive_schema()

    @property
    def output_root(self):
        """Use the table location path for the output root."""
        return self.table_location


class CourseBlocksPartitionTask(CourseBlocksDownstreamMixin, MapReduceJobTaskMixin, HivePartitionTask):
    """
    A single hive partition of course block data, for all courses returned by CourseListApiDataTask.
    """

    def __init__(self, *args, **kwargs):
        super(CourseBlocksPartitionTask, self).__init__(*args, **kwargs)

    @property
    def hive_table_task(self):
        return CourseBlocksTableTask(
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )

    @property
    def data_task(self):
        return CourseBlocksApiDataTask(
            datetime=self.datetime,
            input_root=self.input_root,
            output_root=self.output_root,
            overwrite=self.overwrite,
            mapreduce_engine=self.mapreduce_engine,
            input_format=self.input_format,
            lib_jar=self.lib_jar,
            n_reduce_tasks=self.n_reduce_tasks,
            remote_log_level=self.remote_log_level,
        )

    @property
    def output_root(self):
        """Expose the partition location path as the output root."""
        return self.partition_location
