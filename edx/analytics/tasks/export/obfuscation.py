"""Tasks to obfuscate course data for RDX."""

import errno
import json
import logging
import os
import tarfile
import urlparse

import luigi

from edx.analytics.tasks.common.mapreduce import MapReduceJobTaskMixin
from edx.analytics.tasks.common.pathutil import PathSetTask
from edx.analytics.tasks.export.data_obfuscation import ObfuscatedCourseDumpTask
from edx.analytics.tasks.export.events_obfuscation import ObfuscateCourseEventsTask
from edx.analytics.tasks.util import opaque_key_util
from edx.analytics.tasks.util.encrypt import make_encrypted_file
from edx.analytics.tasks.util.file_util import copy_file_to_file
from edx.analytics.tasks.util.obfuscate_util import ObfuscatorDownstreamMixin
from edx.analytics.tasks.util.tempdir import make_temp_directory
from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join

log = logging.getLogger(__name__)


class ObfuscatedCourseTaskMixin(ObfuscatorDownstreamMixin, MapReduceJobTaskMixin):
    """Parameters used by ObfuscatedCourseTask."""

    obfuscated_output_root = luigi.Parameter(
        config_path={'section': 'obfuscation', 'name': 'obfuscated_output_root'}
    )
    dump_root = luigi.Parameter(
        config_path={'section': 'obfuscation', 'name': 'dump_root'}
    )
    format_version = luigi.Parameter()
    pipeline_version = luigi.Parameter()


class ObfuscatedPackageTaskMixin(object):
    """Parameters used by ObfuscatedPackageTask."""

    obfuscated_output_root = luigi.Parameter(
        config_path={'section': 'obfuscation', 'name': 'obfuscated_output_root'}
    )
    gpg_key_dir = luigi.Parameter(
        config_path={'section': 'obfuscation', 'name': 'gpg_key_dir'}
    )
    gpg_master_key = luigi.Parameter(
        config_path={'section': 'obfuscation', 'name': 'gpg_master_key'}
    )
    output_root = luigi.Parameter(
        config_path={'section': 'obfuscation', 'name': 'output_root'}
    )
    recipient = luigi.ListParameter()
    format_version = luigi.Parameter()


class ObfuscatedCourseTask(ObfuscatedCourseTaskMixin, luigi.Task):
    """Depends on ObfuscatedCourseDumpTask & ObfuscateCourseEventsTask, writes metadata file to the output."""

    course = luigi.Parameter()

    def requires(self):
        output_root_with_version = url_path_join(self.obfuscated_output_root, self.format_version)
        yield ObfuscatedCourseDumpTask(
            dump_root=self.dump_root,
            course=self.course,
            output_root=output_root_with_version,
            entities=self.entities,
            log_context=self.log_context,
            auth_user_path=self.auth_user_path,
            auth_userprofile_path=self.auth_userprofile_path,
        )
        yield ObfuscateCourseEventsTask(
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
        return get_target_from_url(url_path_join(
            self.obfuscated_output_root, self.format_version, filename_safe_course_id, 'metadata_file.json'
        ))


class ObfuscatedPackageTask(ObfuscatedPackageTaskMixin, luigi.Task):
    """Task that packages obfuscated course data."""

    course = luigi.Parameter()
    temporary_dir = luigi.Parameter(default=None)

    def requires(self):
        return ExternalURL(url_path_join(self.course_files_url, 'metadata_file.json'))

    def __init__(self, *args, **kwargs):
        super(ObfuscatedPackageTask, self).__init__(*args, **kwargs)
        self.filename_safe_course_id = opaque_key_util.get_filename_safe_course_id(self.course)
        self.course_files_url = url_path_join(
            self.obfuscated_output_root, self.format_version, self.filename_safe_course_id
        )

    def run(self):
        recipients = set(self.recipient)
        if self.gpg_master_key is not None:
            recipients.add(self.gpg_master_key)
        key_file_targets = [
            get_target_from_url(url_path_join(self.gpg_key_dir, recipient))
            for recipient in recipients
        ]

        path_task = PathSetTask([self.course_files_url], ['*.*'])
        with make_temp_directory(prefix='obfuscate-archive.', dir=self.temporary_dir) as tmp_directory:
            for target in path_task.output():
                with target.open('r') as input_file:
                    # Get path without urlscheme.
                    course_files_path = urlparse.urlparse(self.course_files_url).path
                    # Calculates target's relative path to course_files_path by getting the substring that
                    # occurs after course_files_path substring in target's path.
                    # Needed as target.path returns path with urlscheme for s3target & without for hdfstarget.
                    # Examples:
                    # target.path: /pipeline/output/edX_Demo_Course/events/edX_Demo_Course-events-2015-08-30.log.gz
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
                with make_encrypted_file(
                    output_file, key_file_targets, progress=report_encrypt_progress, dir=self.temporary_dir
                ) as encrypted_output_file:
                    with tarfile.open(mode='w:gz', fileobj=encrypted_output_file) as output_archive_file:
                        output_archive_file.add(tmp_directory, arcname='')

    def output(self):
        return get_target_from_url(url_path_join(self.output_root, self.filename_safe_course_id + '.tar.gz.gpg'))


class MultiCourseObfuscatedCourseTask(ObfuscatedCourseTaskMixin, luigi.WrapperTask):
    """Task to obfuscate multiple courses at once."""

    course = luigi.ListParameter()

    def requires(self):
        for course in self.course:   # pylint: disable=not-an-iterable
            yield ObfuscatedCourseTask(
                course=course,
                dump_root=self.dump_root,
                obfuscated_output_root=self.obfuscated_output_root,
                format_version=self.format_version,
                pipeline_version=self.pipeline_version,
                n_reduce_tasks=self.n_reduce_tasks,
                entities=self.entities,
                log_context=self.log_context,
                auth_user_path=self.auth_user_path,
                auth_userprofile_path=self.auth_userprofile_path,
            )


class MultiCourseObfuscatedPackageTask(ObfuscatedPackageTaskMixin, luigi.WrapperTask):
    """Task to package multiple courses at once."""

    course = luigi.ListParameter()
    temporary_dir = luigi.Parameter(default=None)

    def requires(self):
        for course in self.course:   # pylint: disable=not-an-iterable
            yield ObfuscatedPackageTask(
                course=course,
                obfuscated_output_root=self.obfuscated_output_root,
                output_root=self.output_root,
                recipient=self.recipient,
                gpg_key_dir=self.gpg_key_dir,
                gpg_master_key=self.gpg_master_key,
                format_version=self.format_version,
                temporary_dir=self.temporary_dir,
            )
