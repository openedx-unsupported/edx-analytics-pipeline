"""Collect the course catalog from the course catalog API for processing of course metadata like subjects or types."""
import json
import logging
from urllib import quote

import luigi
import requests

from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.util.opaque_key_util import get_filename_safe_course_id
from edx.analytics.tasks.load_internal_reporting_course import LoadInternalReportingCourseMixin, PullCourseStructureAPIData

log = logging.getLogger(__name__)


class FetchVideoTranscriptionsTask(LoadInternalReportingCourseMixin, luigi.Task):
    """Fetch video transcription text from URLs provide by course blocks, and output as TSV."""

    output_root = luigi.Parameter()

    def requires(self):
        kwargs = {
            'run_date': self.run_date,
            'warehouse_path': self.warehouse_path,
            'api_root_url': self.api_root_url,
            'api_access_token': self.api_access_token
        }
        return PullCourseStructureAPIData(**kwargs)

    def get_api_request_headers(self):
        return {'authorization': ('Bearer ' + self.api_access_token), 'accept': 'application/json'}

    def get_course_video_block_info(self, course_id):
        username = 'brianstaff'
        block_type = 'video'
        input_course_id = quote(course_id)
        query_args = "?course_id={}&username={}&depth=all&student_view_data={}&block_types_filter={}".format(
            input_course_id, username, block_type, block_type
        )
        api_url = url_path_join(self.api_root_url, 'api', 'courses', 'v1', 'blocks', query_args)
        response = requests.get(url=api_url, headers=self.get_api_request_headers(), stream=True)
        if response.status_code != requests.codes.ok:  # pylint: disable=no-member
            msg = "Encountered status {} on request to API for {}".format(response.status_code, api_url)
            raise Exception(msg)
        block_info = json.loads(response.content)
        return block_info

    def get_transcript_text(self, transcript_url):

        response = requests.get(url=transcript_url, stream=True)

        if response.status_code == requests.codes.not_found:
            log.error("Bad URL:  transcript not found at %s", transcript_url)
            return 'NOT_FOUND'
        elif response.status_code != requests.codes.ok:  # pylint: disable=no-member
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

    def generate_course_list_from_file(self):
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

