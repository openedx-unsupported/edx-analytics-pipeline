"""Collect the course catalog from the course catalog API for processing of course metadata like subjects or types."""
import json
import logging
from urllib import quote

import luigi
import requests

from edx.analytics.tasks.util.url import get_target_from_url, url_path_join
from edx.analytics.tasks.util.opaque_key_util import get_filename_safe_course_id
# This doesn't exist:
# from edx.analytics.tasks.warehouse.load_internal_reporting_course import LoadInternalReportingCourseMixin, PullCourseStructureAPIData
# from edx.analytics.tasks.insights.course_list import TimestampPartitionMixin, CourseRecord, CourseListApiDataTask
from edx.analytics.tasks.insights.course_blocks import PullCourseBlocksApiData, CourseBlocksDownstreamMixin

from edx.analytics.tasks.util.edx_api_client import EdxApiClient
from requests.exceptions import HTTPError


log = logging.getLogger(__name__)


class PullVideoCourseBlocksApiData(PullCourseBlocksApiData):

    def get_api_params(self):
        block_type = 'video'

        # return dict(depth="all", requested_fields="children", all_blocks="true")
        # username = 'brianstaff'
        # input_course_id = quote(course_id)
        # query_args = "?course_id={}&username={}&depth=all&student_view_data={}&block_types_filter={}".format(
        #     input_course_id, username, block_type, block_type
        # )
        params = dict(
            depth="all",
            # requested_fields="children",
            # all_blocks="true",

            #student_view_data=block_type,
            #block_types_filter=block_type,
            # assume we no longer need a username?  Maybe with current token, it's no longer needed.
            # But according to API documentation, it is.
            # See http://edx.readthedocs.io/projects/edx-platform-api/en/latest/courses/blocks.html. 
            username='brianstaff',
            # requested_fields="graded,forat,student_view_multi_devices",
            requested_fields="graded,student_view_multi_devices",
            nav_depth=3,
            student_view_data="video",
            block_counts="video"
            # "GET /api/courses/v1/blocks/?depth=all&requested_fields=graded,format,student_view_multi_device&student_view_data=video,discussion&block_counts=video&nav_depth=3&u            
        )
        return params

    def output(self):
        return get_target_from_url(
            url_path_join(
                self.hive_partition_path('course_blocks_raw', partition_value=self.partition_value),
                'course_video_blocks.json'
            )
        )

class ExtractParticularTranscript(luigi.Task):
    output_root = luigi.Parameter()

    def output(self):
        return get_target_from_url(self.output_root, "particular.trans")

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

    def run(self):
        # self.remove_output_on_overwrite()
        client = EdxApiClient(token_type=self.api_token_type)
        params = {} # self.get_api_params()
        counter = 0
        # https://courses.edx.org/api/mobile/v0.5/video_outlines/transcripts/course-v1:BerkeleyX+GG101x-2+1T2015/88b183fd7e9d4f1ea91c60333c4b1f21/en
        # course_id = quote('course-v1:BerkeleyX+GG101x-2+1T2015')
        # block_id = '88b183fd7e9d4f1ea91c60333c4b1f21'
        # This worked in Splunk, but gives a 404 locally:
        # /api/mobile/v0.5/video_outlines/transcripts/course-v1:HarvardX+MCB64.1x+2T2017/a1e7a715a1b04321afaecaf099011346/en
        # => https://courses.edx.org/api/mobile/v0.5/video_outlines/transcripts/course-v1:HarvardX+MCB64.1x+2T2017/a1e7a715a1b04321afaecaf099011346/en
        # course_id = quote('course-v1:HarvardX+MCB64.1x+2T2017')
        course_id = 'course-v1:HarvardX+MCB64.1x+2T2017'
        block_id = 'a1e7a715a1b04321afaecaf099011346'
        courses = [course_id,]
        api_root_url = 'https://courses.edx.org/api/mobile/v0.5/video_outlines/transcripts/{}/{}/en'.format(course_id, block_id)
        with self.output().open('w') as output_file:
            for course_id in courses:  # pylint: disable=not-an-iterable
                # params['course_id'] = course_id
                try:
                    # Course Blocks are returned on one page
                    # response = client.get(self.api_root_url, params=params)
                    response = client.get(api_root_url)
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


class ExtractVideoTranscriptionURLsTask(CourseBlocksDownstreamMixin, luigi.Task):
    """Fetch video transcription URLs from course blocks, and output as TSV."""
    """
    This task fetches video blocks from the Course Blocks edX REST API, and 
    extracts transcription URLs, and stores each video's result on a separate line
    of JSON.

    See the EdxApiClient to configure the REST API connection parameters.
    """

    output_root = luigi.Parameter()

    def requires(self):
        return PullVideoCourseBlocksApiData(
            date=self.date,
            input_root=self.input_root,
            overwrite=self.overwrite,
        )

    def output(self):
        return get_target_from_url(
            url_path_join(
                self.hive_partition_path('course_transcriptions', partition_value=self.partition_value),
                'course_video_transcription_urls.tsv'
            )
        )

    def run(self):
        self.remove_output_on_overwrite()
        with self.input().open('r') as video_info_file:
            for line in video_info_file:
                video_info = json.loads(line)
                root = video_info.get('root')
                blocks = video_info.get('blocks', {})
                course_id = video_info.get('course_id')
                if course_id is None or root is None or root not in blocks:
                    log.error('Unable to read course blocks data from "%s"', line)
                    continue
                
                for block in blocks:
                    block_data = blocks[block]
                    block_id = block.decode('utf8')
                    display_name = block_data.get('display_name')
                    student_view_data = block_data.get('student_view_data')
                    duration = student_view_data.get('duration')
                    transcripts = student_view_data.get('transcripts', {})
                    en_transcript_url = transcripts.get('en')
                    if en_transcript_url is None:
                        log.error("Missing URL:  transcript not found for block %s.  Available transcripts %s", block_id, transcripts.keys())
                        # transcript_text = 'NO_URL'
                    # else:
                    # transcript_text = self.get_transcript_text(en_transcript_url)

                    row = u'\t'.join([course_id, block_id, display_name, unicode(duration), en_transcript_url])
                    # TODO: write to output
        




    


    

    def run(self):
        self.remove_output_on_overwrite()
        client = EdxApiClient(token_type=self.api_token_type)
        # username = 'brianstaff'
        block_type = 'video'
        # input_course_id = quote(course_id)
        # query_args = "?course_id={}&username={}&depth=all&student_view_data={}&block_types_filter={}".format(
        #     input_course_id, username, block_type, block_type
        # )
        
        params = dict(
            depth="all",
            # requested_fields="children",
            # all_blocks="true",
            student_view_data=block_type,
            block_types_filter=block_type,
            # assume we no longer need a username?  Maybe with current token, it's no longer needed.
        )
        counter = 0
        with self.output().open('w') as output_file:
            for course_id in self.generate_course_list_from_file():  # pylint: disable=not-an-iterable
                # We assume that the API code will properly quote() this parameter when inserting into
                # the query args.
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
                    # What to do with this video_info?  Dump the raw file?
                    # Or extract the transcript URL from it and output that?
                    output_file.write(json.dumps(parsed_response))
                    output_file.write('\n')
                    counter += 1

        log.info('Wrote %d records to output file', counter)

    def output(self):
        return get_target_from_url(
            url_path_join(
                self.hive_partition_path('course_blocks_raw', partition_value=self.partition_value),
                'course_video_blocks.json'
            )
        )

        
        for course_id in self.generate_course_list_from_file():
            log.info("Fetching transcripts for %s", course_id)
            self.output_video_data_for_course(course_id)
            log.info("Fetched transcripts for %s", course_id)

        with self.output().open('w') as output_file:
            output_file.write("DONE.")

    def output(self):
        return get_target_from_url(self.output_root, "_SUCCESS")

    def complete(self):
        return get_target_from_url(url_path_join(self.output_root, '_SUCCESS')).exists()


class FetchVideoTranscriptionsTask(CourseBlocksDownstreamMixin, luigi.Task):
    """Fetch video transcription text from URLs provided by course blocks, and output as TSV."""

    def requires(self):
        return FetchVideoTranscriptionURLsTask(
            date=self.date,
            output_root=self.input_root,
            overwrite=self.overwrite,
        )

    def get_transcript_text(self, transcript_url):

        response = requests.get(url=transcript_url, stream=True)

        if response.status_code == requests.codes.not_found:
            log.error("Bad URL:  transcript not found at %s", transcript_url)
            return 'NOT_FOUND'
        elif response.status_code != requests.codes.ok: # pylint: disable=no-member
            msg = "Encountered status {} on request to API for {}".format(response.status_code, transcript_url)
            raise Exception(msg)

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

    def generate_course_video_data(self, course_id):
        block_info = self.get_course_video_block_info(course_id)

        blocks = block_info['blocks']

        for block in blocks:
            block_data = blocks[block]
            block_id = block.decode('utf8')
            display_name = block_data.get('display_name')
            student_view_data = block_data.get('student_view_data')
            duration = student_view_data.get('duration')
            transcripts = student_view_data.get('transcripts', {})
            transcript_url = transcripts.get('en')
            if transcript_url is None:
                log.error("Missing URL:  transcript not found for block %s.  Available transcripts %s", block_id, transcripts.keys())
                transcript_text = 'NO_URL'
            else:
                transcript_text = self.get_transcript_text(transcript_url)

            row = u'\t'.join([course_id, block_id, display_name, unicode(duration), transcript_text])
            yield row

    def output_video_data_for_course(self, course_id):
        suffix = 'tsv'
        block_type = 'video'
        safe_course_id = get_filename_safe_course_id(course_id)
        output_path = url_path_join(self.output_root, "{}_{}.{}".format(safe_course_id, block_type, suffix))
        log.info('Writing output file: %s', output_path)
        output_file_target = get_target_from_url(output_path)
        with output_file_target.open('w') as output_file:
            for row in self.generate_course_video_data(course_id):
                output_file.write(row.encode('utf8'))
                output_file.write('\n')

    def get_transcription_urls_from_file(self):
        with self.input().open('r') as input_file:
            course_info = json.load(input_file)
            result_list = course_info.get('results')
            for result in result_list:
                yield result.get('id').strip()
                
    def run(self):
        self.remove_output_on_overwrite()
        
        for course_id in self.generate_course_list_from_file():
            log.info("Fetching transcripts for %s", course_id)
            self.output_video_data_for_course(course_id)
            log.info("Fetched transcripts for %s", course_id)

        with self.output().open('w') as output_file:
            output_file.write("DONE.")

    def output(self):
        return get_target_from_url(self.output_root, "_SUCCESS")

    def complete(self):
        return get_target_from_url(url_path_join(self.output_root, '_SUCCESS')).exists()
                
