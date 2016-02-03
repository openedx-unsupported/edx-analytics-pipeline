"""Tasks to deidentify course data for RDX."""

import json
import os
import errno
import logging
import tarfile
import urlparse

import luigi

from edx.analytics.tasks.encrypt import make_encrypted_file
from edx.analytics.tasks.mapreduce import MapReduceJobTaskMixin
from edx.analytics.tasks.util.deid_util import DeidentifierDownstreamMixin
from edx.analytics.tasks.util.file_util import copy_file_to_file

from edx.analytics.tasks.data_deidentification import DeidentifiedCourseDumpTask
from edx.analytics.tasks.events_deidentification import DeidentifyCourseEventsTask
from edx.analytics.tasks.pathutil import PathSetTask
from edx.analytics.tasks.url import url_path_join, get_target_from_url
from edx.analytics.tasks.url import ExternalURL
from edx.analytics.tasks.util import opaque_key_util
from edx.analytics.tasks.util.tempdir import make_temp_directory


log = logging.getLogger(__name__)


class DeidentifiedCourseTaskMixin(DeidentifierDownstreamMixin, MapReduceJobTaskMixin):
    """Parameters used by DeidentifiedCourseTask."""

    deidentified_output_root = luigi.Parameter(
        config_path={'section': 'deidentification', 'name': 'deidentified_output_root'}
    )
    dump_root = luigi.Parameter(
        config_path={'section': 'deidentification', 'name': 'dump_root'}
    )
    format_version = luigi.Parameter()
    pipeline_version = luigi.Parameter()


class DeidentifiedPackageTaskMixin(object):
    """Parameters used by DeidentifiedPackageTask."""

    deidentified_output_root = luigi.Parameter(
        config_path={'section': 'deidentification', 'name': 'deidentified_output_root'}
    )
    gpg_key_dir = luigi.Parameter(
        config_path={'section': 'deidentification', 'name': 'gpg_key_dir'}
    )
    gpg_master_key = luigi.Parameter(
        config_path={'section': 'deidentification', 'name': 'gpg_master_key'}
    )
    output_root = luigi.Parameter(
        config_path={'section': 'deidentification', 'name': 'output_root'}
    )
    recipient = luigi.Parameter(is_list=True)
    format_version = luigi.Parameter()


class DeidentifiedCourseTask(DeidentifiedCourseTaskMixin, luigi.Task):
    """Depends on DeidentifiedCourseDumpTask & DeidentifyCourseEventsTask, writes metadata file to the output."""

    course = luigi.Parameter()

    def requires(self):
        output_root_with_version = url_path_join(self.deidentified_output_root, self.format_version)
        yield DeidentifiedCourseDumpTask(
            dump_root=self.dump_root,
            course=self.course,
            output_root=output_root_with_version,
            entities=self.entities,
            log_context=self.log_context,
            auth_user_path=self.auth_user_path,
            auth_userprofile_path=self.auth_userprofile_path,
        )
        yield DeidentifyCourseEventsTask(
            dump_root=self.dump_root,
            course=self.course,
            output_root=output_root_with_version,
            entities=self.entities,
            log_context=self.log_context,
            auth_user_path=self.auth_user_path,
            auth_userprofile_path=self.auth_userprofile_path,
            n_reduce_tasks=self.n_reduce_tasks,
        )

    def run(self):
        with self.output().open('w') as metadata_file:
            json.dump({
                'format_version': self.format_version,
                'pipeline_version': self.pipeline_version
            }, metadata_file)

    def output(self):
        filename_safe_course_id = opaque_key_util.get_filename_safe_course_id(self.course)
        return get_target_from_url(url_path_join(self.deidentified_output_root, self.format_version, filename_safe_course_id, 'metadata_file.json'))


class DeidentifiedPackageTask(DeidentifiedPackageTaskMixin, luigi.Task):
    """Task that packages deidentified course data."""

    course = luigi.Parameter()
    temporary_dir = luigi.Parameter(default=None)

    def requires(self):
        return ExternalURL(url_path_join(self.course_files_url, 'metadata_file.json'))

    def __init__(self, *args, **kwargs):
        super(DeidentifiedPackageTask, self).__init__(*args, **kwargs)
        self.filename_safe_course_id = opaque_key_util.get_filename_safe_course_id(self.course)
        self.course_files_url = url_path_join(self.deidentified_output_root, self.format_version, self.filename_safe_course_id)

    def run(self):
        recipients = set(self.recipient)
        if self.gpg_master_key is not None:
            recipients.add(self.gpg_master_key)
        key_file_targets = [
            get_target_from_url(url_path_join(self.gpg_key_dir, recipient))
            for recipient in recipients
        ]

        path_task = PathSetTask([self.course_files_url], ['*.*'])
        with make_temp_directory(prefix='deid-archive.', dir=self.temporary_dir) as tmp_directory:
            for target in path_task.output():
                with target.open('r') as input_file:
                    # Get path without urlscheme.
                    course_files_path = urlparse.urlparse(self.course_files_url).path
                    # Calculates target's relative path to course_files_path by getting the substring that
                    # occurs after course_files_path substring in target's path.
                    # Needed as target.path returns path with urlscheme for s3target & without for hdfstarget.
                    # Examples:
                    # target.path: /edx-analytics-pipeline/output/edX_Demo_Course/events/edX_Demo_Course-events-2015-08-30.log.gz
                    # relative_path: events/edX_Demo_Course-events-2015-08-30.log.gz
                    # target.path: s3://some_bucket/output/edX_Demo_Course/state/2015-11-25/edX-Demo-Course-auth_user-prod-analytics.sql
                    # relative_path: state/2015-11-25/edX-Demo-Course-auth_user-prod-analytics.sql
                    r_index = target.path.find(course_files_path) + len(course_files_path)
                    relative_path = target.path[r_index:].lstrip('/')

                    local_file_path = os.path.join(tmp_directory, relative_path)
                    try:
                        os.makedirs(os.path.dirname(local_file_path))
                    except OSError as exc:
                        if exc.errno != errno.EEXIST:
                            raise
                    with open(local_file_path, 'w') as temp_file:
                        copy_file_to_file(input_file, temp_file)

            def report_encrypt_progress(num_bytes):
                """Log encryption progress."""
                log.info('Encrypted %d bytes', num_bytes)

            with self.output().open('w') as output_file:
                with make_encrypted_file(output_file, key_file_targets, progress=report_encrypt_progress, dir=self.temporary_dir) as encrypted_output_file:
                    with tarfile.open(mode='w:gz', fileobj=encrypted_output_file) as output_archive_file:
                        output_archive_file.add(tmp_directory, arcname='')

    def output(self):
        return get_target_from_url(url_path_join(self.output_root, self.filename_safe_course_id + '.tar.gz.gpg'))


class MultiCourseDeidentifiedCourseTask(DeidentifiedCourseTaskMixin, luigi.WrapperTask):
    """Task to deid multiple courses at once."""

    course = luigi.Parameter(is_list=True)

    def requires(self):
        for course in self.course:
            yield DeidentifiedCourseTask(
                course=course,
                dump_root=self.dump_root,
                deidentified_output_root=self.deidentified_output_root,
                format_version=self.format_version,
                pipeline_version=self.pipeline_version,
                n_reduce_tasks=self.n_reduce_tasks,
                entities=self.entities,
                log_context=self.log_context,
                auth_user_path=self.auth_user_path,
                auth_userprofile_path=self.auth_userprofile_path,
            )


class MultiCourseDeidentifiedPackageTask(DeidentifiedPackageTaskMixin, luigi.WrapperTask):
    """Task to package multiple courses at once."""

    course = luigi.Parameter(is_list=True)
    temporary_dir = luigi.Parameter(default=None)

    def requires(self):
        for course in self.course:
            yield DeidentifiedPackageTask(
                course=course,
                deidentified_output_root=self.deidentified_output_root,
                output_root=self.output_root,
                recipient=self.recipient,
                gpg_key_dir=self.gpg_key_dir,
                gpg_master_key=self.gpg_master_key,
                format_version=self.format_version,
                temporary_dir=self.temporary_dir,
            )
