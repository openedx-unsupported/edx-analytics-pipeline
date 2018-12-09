"""Support running map reduce jobs using a manifest file to store the input paths."""

import logging

import luigi.task
from luigi import configuration

from edx.analytics.tasks.util.url import get_target_class_from_url, get_target_from_url, url_path_join

CONFIG_SECTION = 'manifest'

log = logging.getLogger(__name__)


def convert_to_manifest_input_if_necessary(manifest_id, targets):
    targets = luigi.task.flatten(targets)
    threshold = configuration.get_config().getint(CONFIG_SECTION, 'threshold', -1)
    if threshold > 0 and len(targets) >= threshold:
        log.debug(
            'Using manifest since %d inputs are greater than or equal to the threshold %d', len(targets), threshold
        )
        return [create_manifest_target(manifest_id, targets)]
    else:
        log.debug(
            'Directly processing files since %d inputs are less than the threshold %d', len(targets), threshold
        )
        return targets


def get_manifest_file_path(manifest_id):
    # Construct the manifest file URL from the manifest_id and the configuration
    base_url = configuration.get_config().get(CONFIG_SECTION, 'path')
    manifest_file_path = url_path_join(base_url, manifest_id + '.manifest')
    return manifest_file_path


def create_manifest_target(manifest_id, targets):
    # If we are running locally, we need our manifest file to be a local file target, however, if we are running on
    # a real Hadoop cluster, it has to be an HDFS file so that the input format can read it. Luigi makes it a little
    # difficult for us to construct a target that can be one or the other of those types of targets at runtime since
    # it relies on inheritance to signify the difference. We hack the inheritance here, by dynamically choosing the
    # base class at runtime based on the URL of the manifest file.

    # Construct the manifest file URL from the manifest_id and the configuration
    manifest_file_path = get_manifest_file_path(manifest_id)

    # Figure out the type of target that should be used to write/read the file.
    manifest_file_target_class, init_args, init_kwargs = get_target_class_from_url(manifest_file_path)

    # Ensure our constructed target inherits from the appropriate type of file target.
    class ManifestInputTarget(ManifestInputTargetMixin, manifest_file_target_class):
        pass

    # This functionality is inherited from the Mixin which contains all of the substantial logic
    return ManifestInputTarget.from_existing_targets(targets, *init_args, **init_kwargs)


def remove_manifest_target_if_exists(manifest_id):
    """Given an id and configuration, construct a target that can check and remove a manifest file."""
    manifest_file_path = get_manifest_file_path(manifest_id)
    # we don't need the mixin in order to check for existence or to remove the manifest file.
    manifest_target = get_target_from_url(manifest_file_path)
    if manifest_target.exists():
        log.info('Removing existing manifest found at %s', manifest_target.path)
        manifest_target.remove()


class ManifestInputTargetMixin(object):

    def __init__(self, *args, **kwargs):
        super(ManifestInputTargetMixin, self).__init__(*args, **kwargs)

        config = configuration.get_config()
        lib_jar = config.get(CONFIG_SECTION, 'lib_jar', None)
        if lib_jar:
            self.lib_jar = [lib_jar]
        input_format = config.get(CONFIG_SECTION, 'input_format', None)
        if input_format:
            self.input_format = input_format

    @classmethod
    def from_existing_targets(cls, other_targets, *init_args, **init_kwargs):
        manifest_target = cls(*init_args, **init_kwargs)
        if not manifest_target.exists():
            log.info('Writing manifest file %s', manifest_target.path)
            with manifest_target.open('w') as manifest_file:
                for target in other_targets:
                    manifest_file.write(target.path)
                    manifest_file.write('\n')
        else:
            log.debug('Reusing existing manifest found at %s', manifest_target.path)

        return manifest_target
