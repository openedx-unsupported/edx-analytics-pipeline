"""
Support executing map reduce tasks.
"""
from __future__ import absolute_import

import gzip
import logging
import logging.config
import os
import StringIO
from hashlib import md5

import luigi
import luigi.contrib.hadoop
import luigi.task
from luigi import configuration

from edx.analytics.tasks.util.manifest import convert_to_manifest_input_if_necessary, remove_manifest_target_if_exists
from edx.analytics.tasks.util.url import get_target_from_url, url_path_join

log = logging.getLogger(__name__)


class MapReduceJobTaskMixin(object):
    """Defines arguments used by downstream tasks to pass to upstream MapReduceJobTask."""

    mapreduce_engine = luigi.Parameter(
        config_path={'section': 'map-reduce', 'name': 'engine'},
        significant=False,
        description='Name of the map reduce job engine to use.  Use `hadoop` (the default) or `local`.',
    )
    # TODO: remove these parameters
    input_format = luigi.Parameter(
        default=None,
        significant=False,
        description='The input_format for Hadoop job to use. For example, when '
        'running with manifest file, specify "oddjob.ManifestTextInputFormat" for input_format.',
    )
    lib_jar = luigi.ListParameter(
        default=[],
        significant=False,
        description='A list of library jars that the Hadoop job can make use of.',
    )
    n_reduce_tasks = luigi.Parameter(
        default=25,
        significant=False,
        description='Number of reducer tasks to use in upstream tasks.  Scale this to your cluster size.',
    )
    remote_log_level = luigi.Parameter(
        config_path={'section': 'map-reduce', 'name': 'remote_log_level'},
        significant=False,
        description='Level of logging for the map reduce tasks.',
    )


class MapReduceJobTask(MapReduceJobTaskMixin, luigi.contrib.hadoop.JobTask):
    """
    Execute a map reduce job.  Typically using Hadoop, but can execute the
    job in process as well.
    """

    def init_hadoop(self):
        log_format = '%(asctime)s %(levelname)s %(process)d [%(name)s] %(filename)s:%(lineno)d - %(message)s'
        logging.config.dictConfig(
            {
                'version': 1,
                'disable_existing_loggers': False,
                'formatters': {
                    'default': {
                        'format': log_format,
                    },
                },
                'handlers': {
                    'stderr': {
                        'formatter': 'default',
                        'class': 'logging.StreamHandler',
                    },
                },
                'root': {
                    'handlers': ['stderr'],
                    'level': self.remote_log_level.upper(),  # pylint: disable=no-member
                },
            }
        )
        return super(MapReduceJobTask, self).init_hadoop()

    def job_runner(self):
        # Lazily import this since this module will be loaded on hadoop worker nodes however stevedore will not be
        # available in that environment.
        from stevedore import ExtensionManager

        extension_manager = ExtensionManager('mapreduce.engine')
        try:
            engine_class = extension_manager[self.mapreduce_engine].plugin
        except KeyError:
            raise KeyError('A map reduce engine must be specified in order to run MapReduceJobTasks')

        if issubclass(engine_class, MapReduceJobRunner):
            engine_kwargs = self._get_engine_parameters_from_targets()
            return engine_class(**engine_kwargs)
        else:
            return engine_class()

    def _get_engine_parameters_from_targets(self):
        """
        Determine the set of job parameters that should be used to process the input.

        Some types of input may not be simple files that Hadoop can process natively out of the box, they might require
        special handling by custom input formats. Allow dynamic loading of input formats and the jars that contain them
        by setting attributes on the input target.
        """
        lib_jar = list(self.lib_jar)
        input_format = self.input_format

        for input_target in luigi.task.flatten(self.input_hadoop()):
            if hasattr(input_target, 'lib_jar'):
                lib_jar.extend(input_target.lib_jar)
            if hasattr(input_target, 'input_format') and input_target.input_format is not None:
                if input_format is not None and input_target.input_format != input_format:
                    raise RuntimeError('Multiple distinct input formats specified on input targets.')

                input_format = input_target.input_format

        return {
            'libjars_in_hdfs': lib_jar,
            'input_format': input_format,
        }

    @property
    def manifest_id(self):
        return str(hash(self)).replace('-', 'n')

    def input_hadoop(self):
        return convert_to_manifest_input_if_necessary(self.manifest_id, super(MapReduceJobTask, self).input_hadoop())

    def remove_manifest_target_if_exists(self):
        return remove_manifest_target_if_exists(self.manifest_id)


class MapReduceJobRunner(luigi.contrib.hadoop.HadoopJobRunner):
    """
    Support more customization of the streaming command.

    Args:
        libjars_in_hdfs (list): An optional list of library jars that the hadoop job can make use of.
        input_format (str): An optional full class name of a hadoop input format to use.
    """

    def __init__(self, libjars_in_hdfs=None, input_format=None):
        libjars_in_hdfs = libjars_in_hdfs or []
        config = configuration.get_config()
        streaming_jar = config.get('hadoop', 'streaming-jar', '/tmp/hadoop-streaming.jar')

        if config.has_section('job-conf'):
            job_confs = dict(config.items('job-conf'))
        else:
            job_confs = {}

        super(MapReduceJobRunner, self).__init__(
            streaming_jar,
            input_format=input_format,
            libjars_in_hdfs=libjars_in_hdfs,
            jobconfs=job_confs,
        )


class EmulatedMapReduceJobRunner(luigi.contrib.hadoop.JobRunner):
    """
    Execute map reduce tasks in process on the machine that is running luigi.

    This is a modified version of luigi.contrib.hadoop.LocalJobRunner. The key differences are:

    * It gracefully handles .gz input files, decompressing them and streaming them directly to the mapper. This mirrors
      the behavior of hadoop's default file input format. Note this only works for files that support `tell()` and
      `seek()` since those methods are used by the gzip decompression library.
    * It detects ".manifest" files and assumes that they are in fact just a file that contains paths to the real files
      that should be processed by the task. It makes use of this information to "do the right thing". This mirrors the
      behavior of a manifest input format in hadoop.
    * It sets the "map_input_file" environment variable when running the mapper just like the hadoop streaming library.

    Other than that it should behave identically to LocalJobRunner.

    """

    def group(self, input):
        output = StringIO.StringIO()
        lines = []
        for i, line in enumerate(input):
            parts = line.rstrip('\n').split('\t')
            blob = md5(str(i)).hexdigest()  # pseudo-random blob to make sure the input isn't sorted
            lines.append((parts[:-1], blob, line))
        for _, _, line in sorted(lines):
            output.write(line)
        output.seek(0)
        return output

    def run_job(self, job):
        job.init_hadoop()
        job.init_mapper()
        map_output = StringIO.StringIO()
        input_targets = luigi.task.flatten(job.input_hadoop())
        for input_target in input_targets:
            # if file is a directory, then assume that it's Hadoop output,
            # and actually loop through its contents:
            if os.path.isdir(input_target.path):
                filenames = os.listdir(input_target.path)
                for filename in filenames:
                    url = url_path_join(input_target.path, filename)
                    input_targets.append(get_target_from_url(url.strip()))
                continue

            with input_target.open('r') as input_file:

                # S3 files not yet supported since they don't support tell() and seek()
                if input_target.path.endswith('.gz'):
                    input_file = gzip.GzipFile(fileobj=input_file)
                elif input_target.path.endswith('.manifest'):
                    for url in input_file:
                        input_targets.append(get_target_from_url(url.strip()))
                    continue

                os.environ['map_input_file'] = input_target.path
                try:
                    outputs = job._map_input((line[:-1] for line in input_file))
                    job.internal_writer(outputs, map_output)
                finally:
                    del os.environ['map_input_file']

        map_output.seek(0)

        reduce_input = self.group(map_output)
        try:
            reduce_output = job.output().open('w')
        except Exception:
            reduce_output = StringIO.StringIO()

        try:
            job._run_reducer(reduce_input, reduce_output)
        finally:
            try:
                reduce_output.close()
            except Exception:
                pass


class MultiOutputMapReduceJobTask(MapReduceJobTask):
    """
    Produces multiple output files from a map reduce job.

    The mapper output tuple key is used to determine the name of the file that reducer results are written to. Different
    reduce tasks must not write to the same file.  Since all values for a given mapper output key are guaranteed to be
    processed by the same reduce task, we only allow a single file to be output per key for safety.  In the future, the
    reducer output key could be used to determine the output file name, however.

    """
    output_root = luigi.Parameter(
        description='A URL location where the split files will be stored.',
    )
    delete_output_root = luigi.BoolParameter(
        default=False,
        significant=False,
        description='If True, recursively deletes the `output_root` at task creation.',
    )
    marker = luigi.Parameter(
        config_path={'section': 'map-reduce', 'name': 'marker'},
        significant=False,
        description='A URL location to a directory where a marker file will be written on task completion.',
    )

    def output(self):
        marker_url = url_path_join(self.marker, str(hash(self)))
        return get_target_from_url(marker_url)

    def reducer(self, key, values):
        """
        Write out values from each key into different output files.
        """
        output_path = self.output_path_for_key(key)
        if output_path:
            log.info('Writing output file: %s', output_path)
            output_file_target = get_target_from_url(output_path)
            with output_file_target.open('w') as output_file:
                self.multi_output_reducer(key, values, output_file)

        # Luigi requires the reducer to return an iterable
        return iter(tuple())

    def multi_output_reducer(self, _key, _values, _output_file):
        """Returns an iterable of strings that are written out to the appropriate output file for this key."""
        return iter(tuple())

    def output_path_for_key(self, _key):
        """
        Returns a URL that is unique to the given key.

        All values returned from the reducer for the given key will be output to the file specified by the URL returned
        from this function.
        """
        return None

    def __init__(self, *args, **kwargs):
        super(MultiOutputMapReduceJobTask, self).__init__(*args, **kwargs)
        if self.delete_output_root:
            # If requested, make sure that the output directory is empty.  This gets rid
            # of any generated data files from a previous run (that might not get
            # regenerated in this run).  It also makes sure that the marker file
            # (i.e. the output target) will be removed, so that external functionality
            # will know that the generation of data files is not complete.
            output_dir_target = get_target_from_url(self.output_root)
            for target in [self.output(), output_dir_target]:
                if target.exists():
                    target.remove()
