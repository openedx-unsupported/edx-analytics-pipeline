"""Support running map reduce jobs using a manifest file to store the input paths."""

import logging

import luigi
from luigi import configuration
from luigi import task

from edx.analytics.tasks.url import url_path_join, get_target_from_url, UncheckedExternalURL


CONFIG_SECTION = 'manifest'

log = logging.getLogger(__name__)


class URLManifestTask(luigi.Task):
    """
    Support running map reduce jobs using a manifest file to store the input paths.

    The operating system has a hard limit on the number and length of arguments passed to a process. When presented with
    a very large number of input paths, it is fairly easy to exceed the limit, which prevents the hadoop streaming
    system from being able to launch the python subprocess it uses to actually process the data.

    The workaround for this problem is to instead provide hadoop with a single input path which is actually a file
    containing the list of actual input paths. A custom input format is used to parse this file and blow out the list of
    input paths in this file into first class hadoop job input paths. This file is called a "manifest" file.

    This task requires some presumably large set of input paths and provides a manifest for those paths. The output
    from this task can be used as input for a hadoop job as long as the custom input format is used.
    """

    urls = luigi.Parameter(is_list=True, default=[])

    def requires(self):
        return [UncheckedExternalURL(url) for url in self.urls]

    def output(self):
        config = configuration.get_config()
        base_url = config.get(CONFIG_SECTION, 'path')
        target = get_target_from_url(url_path_join(base_url, str(hash(self))) + '.manifest')
        lib_jar = config.get(CONFIG_SECTION, 'lib_jar', None)
        if lib_jar:
            target.lib_jar = [lib_jar]
        input_format = config.get(CONFIG_SECTION, 'input_format', None)
        if input_format:
            target.input_format = input_format
        return target

    def run(self):
        with self.output().open('w') as manifest_file:
            for url in self.urls:
                manifest_file.write(url)
                manifest_file.write('\n')


def convert_tasks_to_manifest_if_necessary(input_tasks):  # pylint: disable=invalid-name
    """
    Provide a manifest for the input paths if there are too many of them.

    The configuration section "manifest" can contain a "threshold" option which, when exceeded, causes this function
    to return a URLManifestTask instead of the original input_tasks.
    """
    all_input_tasks = task.flatten(input_tasks)
    targets = task.flatten(task.getpaths(all_input_tasks))
    threshold = configuration.get_config().getint(CONFIG_SECTION, 'threshold', -1)
    if threshold > 0 and len(targets) >= threshold:
        log.debug(
            'Using manifest since %d inputs are greater than or equal to the threshold %d', len(targets), threshold
        )
        return [URLManifestTask(urls=[target.path for target in targets])]
    else:
        log.debug(
            'Directly processing files since %d inputs are less than the threshold %d', len(targets), threshold
        )
        return all_input_tasks
