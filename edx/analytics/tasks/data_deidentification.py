"""Deidentify course state files by removing/stubbing user information."""

import luigi
import luigi.s3
import csv
import json
import os
import re

from edx.analytics.tasks.pathutil import PathSetTask
from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.util.id_codec import UserIdRemapperMixin
import edx.analytics.tasks.util.opaque_key_util as opaque_key_util
from edx.analytics.tasks.util.file_util import FileCopyMixin
import edx.analytics.tasks.util.csv_util

import logging

log = logging.getLogger(__name__)


class BaseDeidentifyDumpTask(UserIdRemapperMixin, luigi.Task):
    """
    Base class for deidentification of state files.
    """

    course = luigi.Parameter()
    output_directory = luigi.Parameter()
    data_directory = luigi.Parameter()

    def output(self):
        if len(self.input()) == 0:
            raise IOError("Course File '{filename}' not found for course '{course}'".format(
                filename=self.file_pattern, course=self.course
            ))

        output_filename = os.path.basename(self.input()[0].path)
        return get_target_from_url(url_path_join(self.output_directory, output_filename))

    def requires(self):
        return PathSetTask([self.data_directory], [self.file_pattern])

    @property
    def file_pattern(self):
        """Provides the file pattern for input file."""
        raise NotImplementedError


class DeidentifySqlDumpTask(BaseDeidentifyDumpTask):
    """Task to deidentify an sql file."""

    def run(self):
        with self.output().open('w') as output_file:
            with self.input()[0].open('r') as input_file:
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


class DeidentifyAuthUserProfileTask(DeidentifySqlDumpTask):
    """Task to deidentify auth_userprofile dump file."""

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


class DeidentifyAuthUserTask(DeidentifySqlDumpTask):
    """Task to deidentify auth_user dump file."""

    @property
    def file_pattern(self):
        return '*-auth_user-*'

    def filter_row(self, row):
        row[0] = self.remap_id(row[0])  # id
        row[1] = "username_{id}".format(id=row[0])
        row[2] = ''  # first_name
        row[3] = ''  # last_name
        row[4] = ''  # email
        row[5] = ''  # passwords

        for i in xrange(11, len(row)):
            row[i] = ''

        return row


class DeidentifyStudentCourseEnrollmentTask(DeidentifySqlDumpTask):
    """Task to deidentify student_courseenrollment dump file."""

    @property
    def file_pattern(self):
        return '*-student_courseenrollment-*'

    def filter_row(self, row):
        row[1] = self.remap_id(row[1])  # user_id
        return row


class DeidentifyUserApiUserCourseTagTask(DeidentifySqlDumpTask):
    """Task to deidentify user_api_usercoursetag dump file."""

    @property
    def file_pattern(self):
        return '*-user_api_usercoursetag-*'

    def filter_row(self, row):
        row[1] = self.remap_id(row[1])  # user_id
        return row


class DeidentifyStudentLanguageProficiencyTask(DeidentifySqlDumpTask):
    """Task to deidentify student_languageproficiency dump file."""

    @property
    def file_pattern(self):
        return '*-student_languageproficiency-*'

    def filter_row(self, row):
        return row


class DeidentifyCoursewareStudentModule(DeidentifySqlDumpTask):
    """Task to deidentify courseware_studentmodule dump file."""

    @property
    def file_pattern(self):
        return '*-courseware_studentmodule-*'

    def filter_row(self, row):
        row[3] = self.remap_id(row[3])  # student_id
        #row[4] = ?  # state
        return row


class DeidentifyCertificatesGeneratedCertificate(DeidentifySqlDumpTask):
    """Task to deidentify certificates_generatedcertificate dump file."""

    @property
    def file_pattern(self):
        return '*-certificates_generatedcertificate-*'

    def filter_row(self, row):
        row[1] = self.remap_id(row[1])  # user_id
        row[2] = ''  # download_url
        row[8] = ''  # verify_uuid
        row[9] = ''  # download_uuid
        row[10] = ''  # name
        row[13] = ''  # error_reason
        return row


class DeidentifyTeamsTask(DeidentifySqlDumpTask):
    """Task to deidentify teams dump file."""

    @property
    def file_pattern(self):
        return '*-teams-*'

    def filter_row(self, row):
        return row


class DeidentifyTeamsMembershipTask(DeidentifySqlDumpTask):
    """Task to deidentify teams_membership dump file."""

    @property
    def file_pattern(self):
        return '*-teams_membership-*'

    def filter_row(self, row):
        row[1] = self.remap_id(row[1])  # user_id
        return row


class DeidentifyVerificationStatusTask(DeidentifySqlDumpTask):
    """Task to deidentify verify_student_verificationstatus dump file."""

    @property
    def file_pattern(self):
        return '*-verify_student_verificationstatus-*'

    def filter_row(self, row):
        row[4] = self.remap_id(row[4])  # user_id
        return row


class DeidentifyWikiArticleTask(DeidentifySqlDumpTask):
    """Task to deidentify wiki_article dump file."""

    @property
    def file_pattern(self):
        return '*-wiki_article-*'

    def filter_row(self, row):
        # row[4] = ? # owner_id
        # row[5] = ? # group_id
        return row


class DeidentifyWikiArticleRevisionTask(DeidentifySqlDumpTask):
    """Task to deidentify wiki_articlerevision dump file."""

    @property
    def file_pattern(self):
        return '*-wiki_articlerevision-*'

    def filter_row(self, row):
        row[2] = ''  # user_message
        row[3] = ''  # automatic_log
        row[4] = ''  # ip_address
        row[5] = self.remap_id(row[5])  # user_id
        return row


class CourseStructureTask(FileCopyMixin, BaseDeidentifyDumpTask):
    """Task to copy course_structure dump file, no deidentification needed."""

    @property
    def file_pattern(self):
        return '*-course_structure-*'


class CourseContentTask(FileCopyMixin, BaseDeidentifyDumpTask):
    """Task to copy course dump file, no deidentification needed."""

    @property
    def file_pattern(self):
        return '*-course-*'


class DeidentifyMongoDumpsTask(BaseDeidentifyDumpTask):
    """Task to deidentify mongo dump file."""

    @property
    def file_pattern(self):
        return '*mongo*'

    def run(self):
        with self.output().open('w') as output_file:
            with self.input()[0].open('r') as input_file:
                for line in input_file:
                    row = json.loads(line)
                    filtered_row = self.filter_row(row)
                    output_file.write(json.dumps(filtered_row, ensure_ascii=False).encode('utf-8'))
                    output_file.write('\n')

    def filter_row(self, row):
        """Replace/remove sensitive information."""
        row['author_id'] = str(self.remap_id(row['author_id']))
        row['author_username'] = "username_{id}".format(id=row['author_id'])
        # TODO: scrub body
        # TODO: encrypt votes { up: [ user_ids.... ], down: [ user_ids...] }
        return row


class DeidentifyCourseDumpTask(luigi.WrapperTask):
    """Wrapper task to deidentify data for a particular course."""

    course = luigi.Parameter()
    dump_root = luigi.Parameter()
    output_root = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super(DeidentifyCourseDumpTask, self).__init__(*args, **kwargs)

        filename_safe_course_id = opaque_key_util.get_filename_safe_course_id(self.course)
        auth_userprofile_targets = PathSetTask([url_path_join(self.dump_root, filename_safe_course_id, 'state')],
                                               ['*auth_userprofile*']).output()
        # TODO: Refactor out this logic of getting latest file. Right now we expect a date, so we use that
        dates = [re.search(r"\d{4}-\d{2}-\d{2}", target.path).group() for target in auth_userprofile_targets]
        latest_date = sorted(dates)[-1]
        self.data_directory = url_path_join(self.dump_root, filename_safe_course_id, 'state', latest_date)
        self.output_directory = url_path_join(self.output_root, filename_safe_course_id, 'state', latest_date)

    def requires(self):
        kwargs = {
            'course': self.course,
            'output_directory': self.output_directory,
            'data_directory': self.data_directory
        }
        yield (
            DeidentifyAuthUserTask(**kwargs),
            DeidentifyAuthUserProfileTask(**kwargs),
            DeidentifyStudentCourseEnrollmentTask(**kwargs),
            DeidentifyUserApiUserCourseTagTask(**kwargs),
            DeidentifyStudentLanguageProficiencyTask(**kwargs),
            DeidentifyCoursewareStudentModule(**kwargs),
            DeidentifyCertificatesGeneratedCertificate(**kwargs),
            DeidentifyTeamsTask(**kwargs),
            DeidentifyTeamsMembershipTask(**kwargs),
            DeidentifyVerificationStatusTask(**kwargs),
            DeidentifyWikiArticleTask(**kwargs),
            DeidentifyWikiArticleRevisionTask(**kwargs),
            CourseContentTask(**kwargs),
            CourseStructureTask(**kwargs),
            DeidentifyMongoDumpsTask(**kwargs),
        )


class DataDeidentificationTask(luigi.WrapperTask):
    """Wrapper task for data deidentification."""

    course = luigi.Parameter(is_list=True)
    dump_root = luigi.Parameter()
    output_root = luigi.Parameter(
        config_path={'section': 'data-deidentification', 'name': 'output_root'}
    )

    def requires(self):
        for course in self.course:
            kwargs = {
                'dump_root': self.dump_root,
                'course': course,
                'output_root': self.output_root,
            }
            yield DeidentifyCourseDumpTask(**kwargs)
