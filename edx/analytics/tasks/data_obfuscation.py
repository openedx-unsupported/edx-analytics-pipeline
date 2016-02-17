"""Obfuscate course state files by removing/stubbing user information."""

import csv
import json
import logging
import os

import cjson
import luigi

from edx.analytics.tasks.pathutil import PathSetTask
from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.util.obfuscate_util import (
    ObfuscatorMixin, backslash_encode_value, backslash_decode_value, ObfuscatorDownstreamMixin
)
from edx.analytics.tasks.util.file_util import FileCopyMixin
import edx.analytics.tasks.util.opaque_key_util as opaque_key_util


log = logging.getLogger(__name__)


class BaseObfuscateDumpTask(ObfuscatorMixin, luigi.Task):
    """
    Base class for obfuscation of state files.
    """

    course = luigi.Parameter()
    output_directory = luigi.Parameter()
    data_directory = luigi.Parameter()

    def output(self):
        if len(self.input()['data']) == 0:
            raise IOError("Course File '{filename}' not found for course '{course}'".format(
                filename=self.file_pattern, course=self.course
            ))
        output_filename = os.path.basename(self.input()['data'][0].path)
        return get_target_from_url(url_path_join(self.output_directory, output_filename))

    def requires(self):
        base_reqs = {
            # We want to process files that are zero-length.
            'data': PathSetTask([self.data_directory], [self.file_pattern], include_zero_length=True)
        }
        base_reqs.update(self.user_info_requirements())
        return base_reqs

    @property
    def file_pattern(self):
        """Provides the file pattern for input file."""
        raise NotImplementedError

    @property
    def file_input_target(self):
        """Get filename of source for copying."""
        return self.input()['data'][0]


class ObfuscateSqlDumpTask(BaseObfuscateDumpTask):
    """Task to obfuscate an sql file."""

    def run(self):
        with self.output().open('w') as output_file:
            with self.input()['data'][0].open('r') as input_file:
                writer = csv.writer(output_file, dialect='mysqlexport')
                reader = csv.reader(input_file, dialect='mysqlexport')

                headers = next(reader, None)

                if headers:
                    writer.writerow(headers)

                for row in reader:
                    # Found an empty line at the bottom of test file
                    if row:
                        filtered_row = self.filter_row(row)
                        writer.writerow(filtered_row)

    def filter_row(self, row):
        """Function to deitentify a particular row."""
        raise NotImplementedError


class ObfuscateAuthUserProfileTask(ObfuscateSqlDumpTask):
    """Task to obfuscate auth_userprofile dump file."""

    @property
    def file_pattern(self):
        return '*-auth_userprofile-*'

    def filter_row(self, row):
        row[1] = self.remap_id(row[1])  # user_id
        row[2] = ''  # name
        row[3] = ''  # language
        row[4] = ''  # location
        row[5] = ''  # meta
        row[6] = ''  # courseware
        row[8] = ''  # mailing_address
        row[12] = '1'  # allow_certificate
        row[14] = ''  # city
        row[15] = ''  # bio

        return row


class ObfuscateAuthUserTask(ObfuscateSqlDumpTask):
    """Task to obfuscate auth_user dump file."""

    @property
    def file_pattern(self):
        return '*-auth_user-*'

    def filter_row(self, row):
        user_id = row[0]
        row[0] = self.remap_id(user_id)  # id
        row[1] = self.generate_obfuscated_username_from_user_id(user_id)
        row[2] = ''  # first_name
        row[3] = ''  # last_name
        row[4] = ''  # email
        row[5] = ''  # passwords

        for i in xrange(11, len(row)):
            row[i] = ''

        return row


class ObfuscateStudentCourseEnrollmentTask(ObfuscateSqlDumpTask):
    """Task to obfuscate student_courseenrollment dump file."""

    @property
    def file_pattern(self):
        return '*-student_courseenrollment-*'

    def filter_row(self, row):
        row[1] = self.remap_id(row[1])  # user_id
        return row


class ObfuscateUserApiUserCourseTagTask(ObfuscateSqlDumpTask):
    """Task to obfuscate user_api_usercoursetag dump file."""

    @property
    def file_pattern(self):
        return '*-user_api_usercoursetag-*'

    def filter_row(self, row):
        row[1] = self.remap_id(row[1])  # user_id
        return row


class ObfuscateStudentLanguageProficiencyTask(ObfuscateSqlDumpTask):
    """Task to obfuscate student_languageproficiency dump file."""

    @property
    def file_pattern(self):
        return '*-student_languageproficiency-*'

    def filter_row(self, row):
        return row


class ObfuscateCoursewareStudentModule(ObfuscateSqlDumpTask):
    """Task to obfuscate courseware_studentmodule dump file."""

    @property
    def file_pattern(self):
        return '*-courseware_studentmodule-*'

    def filter_row(self, row):
        user_id = row[3]
        user_info = {'user_id': [user_id, ]}
        try:
            user_id = int(user_id)
            entry = self.user_by_id[user_id]
            if 'username' in entry:
                user_info['username'] = [entry['username'], ]
            if 'name' in entry:
                user_info['name'] = [entry['name'], ]
        except KeyError:
            log.error("Unable to find CWSM user_id: %r in the user_by_id map of size %s", user_id, len(self.user_by_id))

        row[3] = self.remap_id(user_id)  # student_id

        # Courseware_studentmodule is not processed with the other SQL tables, so it
        # is not escaped in the same way.  In particular, we will not decode and encode it,
        # but merely transform double backslashes.
        state_str = row[4].replace('\\\\', '\\')
        try:
            state_dict = cjson.decode(state_str, all_unicode=True)
            # Traverse the dictionary, looking for entries that need to be scrubbed.
            updated_state_dict = self.obfuscator.obfuscate_structure(state_dict, u"state", user_info)
        except Exception:   # pylint:  disable=broad-except
            log.exception(u"Unable to parse state as JSON for record %s: type = %s, state = %r",
                          row[0], type(state_str), state_str)
            updated_state_dict = {}

        if updated_state_dict is not None:
            # Can't reset values, so update original fields.
            updated_state = cjson.encode(updated_state_dict).replace('\\', '\\\\')
            row[4] = updated_state
            if self.obfuscator.is_logging_enabled():
                log.info(u"Obfuscated state for user_id '%s' module_id '%s'", user_id, row[2])

        return row


class ObfuscateCertificatesGeneratedCertificate(ObfuscateSqlDumpTask):
    """Task to obfuscate certificates_generatedcertificate dump file."""

    @property
    def file_pattern(self):
        return '*-certificates_generatedcertificate-*'

    def filter_row(self, row):
        row[1] = self.remap_id(row[1])  # user_id
        row[2] = ''  # download_url
        row[5] = ''  # key
        row[8] = ''  # verify_uuid
        row[9] = ''  # download_uuid
        row[10] = ''  # name
        row[13] = ''  # error_reason
        return row


class ObfuscateTeamsTask(ObfuscateSqlDumpTask):
    """Task to obfuscate teams dump file."""

    @property
    def file_pattern(self):
        return '*-teams-*'

    def filter_row(self, row):
        return row


class ObfuscateTeamsMembershipTask(ObfuscateSqlDumpTask):
    """Task to obfuscate teams_membership dump file."""

    @property
    def file_pattern(self):
        return '*-teams_membership-*'

    def filter_row(self, row):
        row[1] = self.remap_id(row[1])  # user_id
        return row


class ObfuscateVerificationStatusTask(ObfuscateSqlDumpTask):
    """Task to obfuscate verify_student_verificationstatus dump file."""

    @property
    def file_pattern(self):
        return '*-verify_student_verificationstatus-*'

    def filter_row(self, row):
        row[4] = self.remap_id(row[4])  # user_id
        return row


class ObfuscateWikiArticleTask(ObfuscateSqlDumpTask):
    """Task to obfuscate wiki_article dump file."""

    @property
    def file_pattern(self):
        return '*-wiki_article-*'

    def filter_row(self, row):
        # Removing these just to be safe.
        row[4] = ''  # owner_id
        row[5] = ''  # group_id
        return row


class ObfuscateWikiArticleRevisionTask(ObfuscateSqlDumpTask):
    """Task to obfuscate wiki_articlerevision dump file."""

    @property
    def file_pattern(self):
        return '*-wiki_articlerevision-*'

    def filter_row(self, row):
        user_id = row[5]
        user_info = {}
        if user_id != 'NULL':
            user_id = int(user_id)
            user_info['user_id'] = [user_id, ]
            try:
                entry = self.user_by_id[user_id]
                if 'username' in entry:
                    user_info['username'] = [entry['username'], ]
                if 'name' in entry:
                    user_info['name'] = [entry['name'], ]
            except KeyError:
                log.error("Unable to find wiki user_id: %s in the user_by_id map", user_id)

        row[2] = ''  # user_message
        row[3] = ''  # automatic_log
        row[4] = ''  # ip_address
        # For user_id, preserve 'NULL' value if present.
        if user_id != 'NULL':
            row[5] = self.remap_id(user_id)

        wiki_content = backslash_decode_value(row[12].decode('utf8'))
        cleaned_content = self.obfuscator.obfuscate_text(wiki_content, user_info)
        row[12] = backslash_encode_value(cleaned_content).encode('utf8')

        return row


class CourseStructureTask(FileCopyMixin, BaseObfuscateDumpTask):
    """Task to copy course_structure dump file, no obfuscation needed."""

    @property
    def file_pattern(self):
        return '*-course_structure-*'


class CourseContentTask(FileCopyMixin, BaseObfuscateDumpTask):
    """Task to copy course dump file, no obfuscation needed."""

    @property
    def file_pattern(self):
        return '*-course-*'


class ObfuscateMongoDumpsTask(BaseObfuscateDumpTask):
    """Task to obfuscate mongo dump file."""

    @property
    def file_pattern(self):
        return '*mongo*'

    def run(self):
        with self.output().open('w') as output_file:
            with self.input()['data'][0].open('r') as input_file:
                for line in input_file:
                    row = json.loads(line)
                    filtered_row = self.filter_row(row)
                    output_file.write(json.dumps(filtered_row, ensure_ascii=False).encode('utf-8'))
                    output_file.write('\n')

    def filter_row(self, row):
        """Replace/remove sensitive information."""
        try:
            author_id = int(row['author_id'])
        except ValueError:
            log.error("Encountered non-integer value for author_id (%s) in forums data.  Username = '%s'",
                      row.get('author_id'), row.get('author_username'))
            author_id = None

        user_info = {}
        if author_id is not None:
            # Gather user_info.
            user_info['user_id'] = [author_id, ]
            user_info['username'] = [row.get('author_username'), ]
            try:
                entry = self.user_by_id[author_id]
                if 'name' in entry:
                    user_info['name'] = [entry['name'], ]
                # While we're at it, perform a sanity check on username.
                decoded_username = row.get('author_username', '').decode('utf8')
                if decoded_username != entry['username']:
                    log.error("author_username '%s' for author_id: %s does not match cached value '%s'",
                              decoded_username, author_id, entry['username'])
            except KeyError:
                log.error("Unable to find author_id: %s in the user_by_id map", author_id)

            # Remap author values, if possible.
            row['author_id'] = str(self.remap_id(author_id))
            row['author_username'] = self.generate_obfuscated_username_from_user_id(author_id)

        # Clean the body of the forum post.
        body = row['body']
        row['body'] = self.obfuscator.obfuscate_text(body, user_info)

        # Also clean the title, if present, since it also contains username and fullname matches.
        if 'title' in row:
            title = row['title']
            row['title'] = self.obfuscator.obfuscate_text(title, user_info)

        # Remap user_id values that are stored in lists.
        if 'votes' in row:
            votes = row['votes']
            if 'down' in votes and len(votes['down']) > 0:
                votes['down'] = [str(self.remap_id(user_id)) for user_id in votes['down']]
            if 'up' in votes and len(votes['up']) > 0:
                votes['up'] = [str(self.remap_id(user_id)) for user_id in votes['up']]

        if 'abuse_flaggers' in row and len(row['abuse_flaggers']) > 0:
            row['abuse_flaggers'] = [str(self.remap_id(user_id)) for user_id in row['abuse_flaggers']]
        if 'historical_abuse_flaggers' in row and len(row['historical_abuse_flaggers']) > 0:
            row['historical_abuse_flaggers'] = [
                str(self.remap_id(user_id)) for user_id in row['historical_abuse_flaggers']
            ]
        if 'endorsement' in row and row['endorsement'] and 'user_id' in row['endorsement']:
            user_id = row['endorsement']['user_id']
            if user_id is not None:
                row['endorsement']['user_id'] = str(self.remap_id(user_id))

        return row


class ObfuscatedCourseDumpTask(ObfuscatorDownstreamMixin, luigi.WrapperTask):
    """Wrapper task to obfuscate data for a particular course."""

    course = luigi.Parameter()
    dump_root = luigi.Parameter()
    output_root = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super(ObfuscatedCourseDumpTask, self).__init__(*args, **kwargs)

        filename_safe_course_id = opaque_key_util.get_filename_safe_course_id(self.course)
        dump_path = url_path_join(self.dump_root, filename_safe_course_id, 'state')
        auth_userprofile_targets = PathSetTask([dump_path], ['*auth_userprofile*']).output()
        # TODO: Refactor out this logic of getting latest file. Right now we expect a date, so we use that
        dates = [target.path.rsplit('/', 2)[-2] for target in auth_userprofile_targets]
        # TODO: Make the date a parameter that defaults to the most recent, but allows the user to override?
        # This should return an error if no data is found, rather than getting a cryptic 'index out of range' error.
        if len(dates) == 0:
            raise Exception('Missing auth_userprofile data file in {}'.format(dump_path))
        latest_date = sorted(dates)[-1]
        self.data_directory = url_path_join(self.dump_root, filename_safe_course_id, 'state', latest_date)
        self.output_directory = url_path_join(self.output_root, filename_safe_course_id, 'state', latest_date)

    def requires(self):
        kwargs = {
            'course': self.course,
            'output_directory': self.output_directory,
            'data_directory': self.data_directory,
            'entities': self.entities,
            'log_context': self.log_context,
            'auth_user_path': self.auth_user_path,
            'auth_userprofile_path': self.auth_userprofile_path,
        }
        yield (
            ObfuscateAuthUserTask(**kwargs),
            ObfuscateAuthUserProfileTask(**kwargs),
            ObfuscateStudentCourseEnrollmentTask(**kwargs),
            ObfuscateUserApiUserCourseTagTask(**kwargs),
            ObfuscateStudentLanguageProficiencyTask(**kwargs),
            # TODO: decide if CWSM should be optional, and if so, how to implement that.
            ObfuscateCoursewareStudentModule(**kwargs),
            ObfuscateCertificatesGeneratedCertificate(**kwargs),
            ObfuscateTeamsTask(**kwargs),
            ObfuscateTeamsMembershipTask(**kwargs),
            ObfuscateVerificationStatusTask(**kwargs),
            ObfuscateWikiArticleTask(**kwargs),
            ObfuscateWikiArticleRevisionTask(**kwargs),
            CourseContentTask(**kwargs),
            CourseStructureTask(**kwargs),
            ObfuscateMongoDumpsTask(**kwargs),
        )


class DataObfuscationTask(ObfuscatorDownstreamMixin, luigi.WrapperTask):
    """Wrapper task for data obfuscation development."""

    course = luigi.Parameter(is_list=True)
    dump_root = luigi.Parameter()
    output_root = luigi.Parameter(
        config_path={'section': 'obfuscation', 'name': 'output_root'}
    )

    def requires(self):
        for course in self.course:   # pylint: disable=not-an-iterable
            kwargs = {
                'dump_root': self.dump_root,
                'course': course,
                'output_root': self.output_root,
                'entities': self.entities,
                'log_context': self.log_context,
                'auth_user_path': self.auth_user_path,
                'auth_userprofile_path': self.auth_userprofile_path,
            }
            yield ObfuscatedCourseDumpTask(**kwargs)
