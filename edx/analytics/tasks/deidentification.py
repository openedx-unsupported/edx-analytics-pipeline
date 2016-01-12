import os
import errno
import logging
import tarfile
import urlparse

import luigi

from edx.analytics.tasks.encrypt import make_encrypted_file
from edx.analytics.tasks.util.file_util import copy_file_to_file

from edx.analytics.tasks.data_deidentification import DeidentifyCourseDumpTask
from edx.analytics.tasks.events_deidentification import DeidentifyCourseEventsTask
from edx.analytics.tasks.pathutil import PathSetTask
from edx.analytics.tasks.url import url_path_join, get_target_from_url
from edx.analytics.tasks.util import opaque_key_util
from edx.analytics.tasks.util.tempdir import make_temp_directory


log = logging.getLogger(__name__)


class DeidentificationTaskMixin(object):
    dump_root = luigi.Parameter(
        config_path={'section': 'deidentification', 'name': 'dump_root'}
    )
    intermediate_output_root = luigi.Parameter(
        config_path={'section': 'deidentification', 'name': 'intermediate_output_root'}
    )
    event_log_interval = luigi.DateIntervalParameter(
        config_path={'section': 'deidentification', 'name': 'event_log_interval'}
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


class DeidentificationTask(DeidentificationTaskMixin, luigi.Task):

    course = luigi.Parameter()

    def requires(self):
        yield DeidentifyCourseDumpTask(
            dump_root=self.dump_root,
            course=self.course,
            output_root=self.intermediate_output_root,
        )

        filename_safe_course_id = opaque_key_util.get_filename_safe_course_id(self.course)
        source = [url_path_join(self.dump_root, filename_safe_course_id, 'events')]
        yield DeidentifyCourseEventsTask(
            dump_root=self.dump_root,
            course=self.course,
            output_root=self.intermediate_output_root,
            source=source,
            interval=self.event_log_interval,
            pattern='.*.gz',
        )

    def run(self):
        recipients = set(self.recipient)
        if self.gpg_master_key is not None:
            recipients.add(self.gpg_master_key)
        key_file_targets = [
            get_target_from_url(url_path_join(self.gpg_key_dir, recipient))
            for recipient in recipients
        ]

        filename_safe_course_id = opaque_key_util.get_filename_safe_course_id(self.course)
        course_files_url = url_path_join(self.intermediate_output_root, filename_safe_course_id)
        path_task = PathSetTask(
                [course_files_url],
                ['*.*']
        )

        with make_temp_directory(prefix='deid-archive.') as tmp_directory:
            for target in path_task.output():
                log.info(target.path)
                with target.open('r') as input_file:
                    relative_url_path = urlparse.urlparse(course_files_url).path
                    r_index = target.path.find(relative_url_path) + len(relative_url_path)
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
                """Update hadoop counters as the file is encrypted"""
                log.info('Encrypted %d bytes', num_bytes)

            with self.output().open('w') as output_file:
                with make_encrypted_file(output_file, key_file_targets, progress=report_encrypt_progress) as encrypted_output_file:
                    with tarfile.open(mode='w:gz', fileobj=encrypted_output_file) as output_archive_file:
                        output_archive_file.add(tmp_directory, arcname='')

    def output(self):
        filename_safe_course_id = opaque_key_util.get_filename_safe_course_id(self.course)
        return get_target_from_url(url_path_join(self.output_root, filename_safe_course_id + '.tar.gz.gpg'))


class MultiCourseDeidentificationTask(DeidentificationTaskMixin, luigi.WrapperTask):

    course = luigi.Parameter(is_list=True)
    request_id = luigi.Parameter()

    def requires(self):
        for course in self.course:
            yield DeidentificationTask(
                course=course,
                dump_root=self.dump_root,
                intermediate_output_root=self.intermediate_output_root,
                event_log_interval=self.event_log_interval,
                output_root=url_path_join(self.output_root, self.request_id),
                recipient=self.recipient,
                gpg_key_dir=self.gpg_key_dir,
                gpg_master_key=self.gpg_master_key,
            )
