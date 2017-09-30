"""Collect the course catalog from the course catalog API for processing of course metadata like subjects or types."""
import datetime
import json
import logging
from urllib import quote

import luigi
import requests

from edx.analytics.tasks.util.url import get_target_from_url, url_path_join, ExternalURL
from edx.analytics.tasks.util.opaque_key_util import get_filename_safe_course_id
from edx.analytics.tasks.insights.course_list import CourseRecord
from edx.analytics.tasks.insights.course_blocks import PullCourseBlocksApiData, CourseBlocksDownstreamMixin
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.hive import WarehouseMixin
from edx.analytics.tasks.util.record import Record, StringField

from edx.analytics.tasks.util.edx_api_client import EdxApiClient
from requests.exceptions import HTTPError


log = logging.getLogger(__name__)


class CourseVideoTranscriptInfoRecord(Record):
    """
    Represents a course block as fetched from the edX Course Blocks REST API, augmented with details about its
    position in the course.
    """
    course_id = StringField(length=255, nullable=False, description='Identifier for the course containing the block.')
    block_id = StringField(length=564, nullable=False, description='Block identifier.')
    display_name = StringField(length=255, nullable=False, truncate=True, normalize_whitespace=True,
                               description='User-facing title of the block. Will be truncated to 255 characters.')
    duration = StringField(length=255, nullable=False, description='Video duration.')
    lang = StringField(length=12, nullable=False, description='Language of video transcript.')
    transcript_url = StringField(length=1024, nullable=False, description='URL for fetching video transcript.')


class GetCourseVideoTranscriptTask(OverwriteOutputMixin, WarehouseMixin, luigi.Task):

    BLOCK_TYPE = 'video'

    output_root = luigi.Parameter(default=None)

    input_root = luigi.Parameter(default=None,
        description='URL pointing to the course_list partition data, containing the list of courses whose blocks will '
                    'be loaded.  Note that if this location does not already exist, it will be created by the '
                    'CourseListPartitionTask.'
    )
    date = luigi.DateParameter(default=datetime.datetime.utcnow().date(),)

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

    def __init__(self, *args, **kwargs):
        super(GetCourseVideoTranscriptTask, self).__init__(*args, **kwargs)
        # Provide default for output_root at this level.
        if self.output_root is None:
            self.output_root = self.get_video_transcript_text_directory()

        if self.input_root is None:
            self.input_root = self.hive_partition_path('course_list_raw', partition_value=self.date)

        self.client = EdxApiClient(token_type=self.api_token_type)
        self.courses = None

    def output(self):
        return get_target_from_url(url_path_join(self.output_root, "_SUCCESS"))

    def complete(self):
        return self.output().exists()

    def requires(self):
        return ExternalURL(url=self.input_root)

    def get_input_course_list(self):
        # Get list of all courses from map-reduced directory.
        if self.courses is None:
            self.courses = []
            with self.input().open('r') as course_list_file:
                for line in course_list_file:
                    course = CourseRecord.from_tsv(line)
                    self.courses.append(course.course_id)
        return self.courses
                
    def get_video_listing_params(self):
        return dict(
            # course_id is added by the main routine.  Add the other values here.
            depth="all",
            student_view_data=self.BLOCK_TYPE,
            block_types_filter=self.BLOCK_TYPE,

            # assume we no longer need a username?  Maybe with current token, it's no longer needed.
            # But according to API documentation, it is.
            # See http://edx.readthedocs.io/projects/edx-platform-api/en/latest/courses/blocks.html. 
            username='brianstaff',
        )

    def get_filepath_for_course(self, directory, course_id, filetype):
        # Create an output file for each course.
        suffix = 'tsv'
        safe_course_id = get_filename_safe_course_id(course_id)
        output_path = url_path_join(directory, "{}_{}.{}".format(safe_course_id, filetype, suffix))
        return output_path

    def get_client_response(self, params):
        try:
            # Course Blocks are returned on one page
            response = self.client.get(self.api_root_url, params=params)
        except HTTPError as error:
            # 404 errors may occur if we try to fetch the course blocks for a deleted course.
            # So we just log and ignore them.
            if error.response.status_code == 404:
                log.error('Error %s fetching API resource  %s: %s', error.response.status_code, params, error)
            else:
                log.error('Error %s fetching API resource  %s: %s', error.response.status_code, params, error)                
                # raise error
        else:
            parsed_response = response.json()
            return parsed_response

    def generate_course_video_info(self, block_info):
        course_id = block_info['course_id']
        blocks = block_info['blocks']

        for block in blocks:
            block_data = blocks[block]
            block_id = block.decode('utf8')     # or block.get('id')?
            display_name = block_data.get('display_name')
            student_view_data = block_data.get('student_view_data')
            duration = student_view_data.get('duration')
            transcripts = student_view_data.get('transcripts', {})
            # transcript_url = transcripts.get('en')
            for lang in transcripts:
                transcript_url = transcripts.get(lang)
                if transcript_url is None:
                    log.error("Missing URL:  transcript not found for block %s.  Available transcripts %s", block_id, transcripts.keys())
                    transcript_url = 'NO_URL'
                record = CourseVideoTranscriptInfoRecord(
                    course_id=course_id,
                    block_id=block_id,
                    display_name=display_name,
                    duration=unicode(duration),
                    lang=lang,
                    transcript_url=transcript_url,
                )
                yield record.to_separated_values()

    def get_video_transcript_info_directory(self):
        return self.hive_partition_path('course_transcript_info', partition_value=self.date)

    def get_video_transcript_text_directory(self):
        return self.hive_partition_path('course_transcript_text', partition_value=self.date)

    def get_video_transcript_info(self):
        directory = self.get_video_transcript_info_directory()

        marker_target = get_target_from_url(url_path_join(directory, "_SUCCESS"))
        if marker_target.exists():
            log.info('Skipping video_transcript_info -- marker already exists: %s', marker_target.path)
            return
        
        course_counter = 0
        transcript_counter = 0
        params = self.get_video_listing_params()
        for course_id in self.get_input_course_list():  # pylint: disable=not-an-iterable
            output_path = self.get_filepath_for_course(directory, course_id, 'video_info')
            output_target = get_target_from_url(output_path)
            course_counter += 1

            with output_target.open('w') as output_file:
                # overwrite the course_id parameter each time.
                params['course_id'] = course_id
                parsed_response = self.get_client_response(params)
                if not parsed_response:
                    continue
                parsed_response['course_id'] = course_id

                for row in self.generate_course_video_info(parsed_response):
                    output_file.write(row)  # assume this does not need row.encode('utf8'))
                    output_file.write('\n')
                    transcript_counter += 1

        log.info('Wrote %d info records from %d courses to output file', transcript_counter, course_counter)
        with marker_target.open('w') as marker_file:
            marker_file.write("Done.")

    def get_video_transcript_text(self):
        text_directory = self.get_video_transcript_text_directory()
        log.info('Calling video_transcript_text -- checking if marker already exists in %s', text_directory)        
        marker_target = get_target_from_url(url_path_join(text_directory, "_SUCCESS"))
        if marker_target.exists():
            log.info('Skipping video_transcript_text -- marker already exists in %s', text_directory)
            return

        course_counter = 0
        transcript_counter = 0
        info_directory = self.get_video_transcript_info_directory()

        for course_id in self.get_input_course_list():  # pylint: disable=not-an-iterable
            course_counter += 1

            input_path = self.get_filepath_for_course(info_directory, course_id, 'video_info')
            input_target = get_target_from_url(input_path)
            if not input_target.exists():
                continue

            output_path = self.get_filepath_for_course(text_directory, course_id, 'video_text')
            output_target = get_target_from_url(output_path)

            counter = 0
            with output_target.open('w') as video_text_file:
                with input_target.open('r') as video_info_file:
                    for line in video_info_file:
                        record = CourseVideoTranscriptInfoRecord.from_tsv(line)
                        transcript_url = record.transcript_url
                        text = self.get_transcript_text(transcript_url)
                        output = "{}\t{}".format(record.to_separated_values(), text.encode('utf8'))
                        video_text_file.write(output)
                        video_text_file.write('\n')
                        counter += 1

            log.info('Wrote %d text records for course %d - %s ', counter, course_counter, course_id)
            transcript_counter += counter

        log.info('Wrote %d text records from %d courses to output file', transcript_counter, course_counter)
        with marker_target.open('w') as marker_file:
            marker_file.write("Done.")

    def get_transcript_text(self, transcript_url):

        try:
            response = requests.get(url=transcript_url, stream=True)
        except Exception as exc:
            log.error("Failed request:  transcript %s returned exception %s", transcript_url, exc)
            return 'FAILED_FETCH'

        if response.status_code == requests.codes.not_found:
            log.error("Bad URL:  transcript not found at %s", transcript_url)
            return 'NOT_FOUND'
        elif response.status_code != requests.codes.ok: # pylint: disable=no-member
            msg = "Failed fetch: Encountered status {} on request to API for {}".format(response.status_code, transcript_url)
            log.error(msg)
            return 'NOT_FETCHED'

        textlines = []
        for index, line in enumerate(response.iter_lines()):
            line = line.strip().decode('utf8')
            # This is not always being met. Let us bail out when it isn't.
            if (index % 4) == 3 and len(line) > 0:
                log.error("Bad format: expected blank line on line %d of transcript %s: %s", index, transcript_url, line)
                return 'BAD_FORMAT'

            if (index % 4) == 2:
                textlines.append(line.replace('\t', ' '))

        return u' '.join(textlines)

    def run(self):
        # self.remove_output_on_overwrite()

        self.get_video_transcript_info()

        self.get_video_transcript_text()

        # with self.output().open('w') as marker_file:
        # marker_file.write("Done.")

