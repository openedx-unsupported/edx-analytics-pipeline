import ast
import json
import logging
import os
import tempfile
import zipfile

import luigi
import luigi.configuration
from luigi.contrib.spark import PySparkTask

from edx.analytics.tasks.common.pathutil import EventLogSelectionDownstreamMixin, PathSelectionByDateIntervalTask
from edx.analytics.tasks.util.manifest import (
    ManifestInputTargetMixin, convert_to_manifest_input_if_necessary, remove_manifest_target_if_exists
)
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.url import get_target_from_url

_file_path_to_package_meta_path = {}


def get_package_metadata_paths():
    """
    List of package metadata to be loaded on EMR cluster
    """
    from distlib.database import DistributionPath

    if len(_file_path_to_package_meta_path) > 0:
        return _file_path_to_package_meta_path

    dist_path = DistributionPath(include_egg=True)
    for distribution in dist_path.get_distributions():
        metadata_path = distribution.path
        for installed_file_path, _hash, _size in distribution.list_installed_files():
            absolute_installed_file_path = installed_file_path
            if not os.path.isabs(installed_file_path):
                absolute_installed_file_path = os.path.join(os.path.dirname(metadata_path), installed_file_path)
            normalized_file_path = os.path.realpath(absolute_installed_file_path)
            _file_path_to_package_meta_path[normalized_file_path] = metadata_path

    return _file_path_to_package_meta_path


def dereference(f):
    if os.path.islink(f):
        # by joining with the dirname we are certain to get the absolute path
        return dereference(os.path.join(os.path.dirname(f), os.readlink(f)))
    else:
        return f


def create_packages_archive(packages, archive_dir_path):
    """
    Create a zip archive for all the packages listed in packages and returns the list of zip file location.
    """
    import zipfile
    archives_list = []
    package_metadata_paths = get_package_metadata_paths()
    metadata_to_add = dict()

    package_zip_path = os.path.join(archive_dir_path, 'packages.zip')
    package_zip = zipfile.ZipFile(package_zip_path, "w", compression=zipfile.ZIP_DEFLATED)
    archives_list.append(package_zip_path)

    def add(src, dst, package_name):
        # Ensure any entry points and other egg-info metadata is also transmitted along with
        # this file. If it is associated with any egg-info directories, ship them too.
        metadata_path = package_metadata_paths.get(os.path.realpath(src))
        if metadata_path:
            metadata_to_add[package_name] = metadata_path

        package_zip.write(src, dst)

    def add_files_for_package(sub_package_path, root_package_path, root_package_name, package_name):
        for root, dirs, files in os.walk(sub_package_path):
            if '.svn' in dirs:
                dirs.remove('.svn')
            for f in files:
                if not f.endswith(".pyc") and not f.startswith("."):
                    add(dereference(root + "/" + f),
                        root.replace(root_package_path, root_package_name) + "/" + f,
                        package_name)

    for package in packages:
        # Archive each package
        if not getattr(package, "__path__", None) and '.' in package.__name__:
            package = __import__(package.__name__.rpartition('.')[0], None, None, 'non_empty')

        n = package.__name__.replace(".", "/")

        # Check length of path, because the attribute may exist and be an empty list.
        if len(getattr(package, "__path__", [])) > 0:
            # TODO: (BUG) picking only the first path does not
            # properly deal with namespaced packages in different
            # directories
            p = package.__path__[0]

            if p.endswith('.egg') and os.path.isfile(p):
                raise 'Not going to archive egg files!!!'
                # Add the entire egg file
                # p = p[:p.find('.egg') + 4]
                # add(dereference(p), os.path.basename(p))

            else:
                # include __init__ files from parent projects
                root = []
                for parent in package.__name__.split('.')[0:-1]:
                    root.append(parent)
                    module_name = '.'.join(root)
                    directory = '/'.join(root)

                    add(dereference(__import__(module_name, None, None, 'non_empty').__path__[0] + "/__init__.py"),
                        directory + "/__init__.py",
                        package.__name__)

                add_files_for_package(p, p, n, package.__name__)

        else:
            f = package.__file__
            if f.endswith("pyc"):
                f = f[:-3] + "py"
            if n.find(".") == -1:
                add(dereference(f), os.path.basename(f), package.__name__)
            else:
                add(dereference(f), n + ".py", package.__name__)

        # include metadata in the same zip file
        metadata_path = metadata_to_add.get(package.__name__)
        if metadata_path is not None:
            add_files_for_package(metadata_path, metadata_path, os.path.basename(metadata_path), package.__name__)

    return archives_list


class PathSelectionTaskSpark(EventLogSelectionDownstreamMixin, luigi.WrapperTask):
    """
    Path selection task with manifest feature for spark
    """
    requirements = None

    def requires(self):
        yield PathSelectionByDateIntervalTask(
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            date_pattern=self.date_pattern,
        )

    @property
    def manifest_id(self):
        return str(hash(self)).replace('-', 'n')

    def get_target_paths(self):
        if not self.requirements:
            targets = luigi.task.flatten(
                convert_to_manifest_input_if_necessary(self.manifest_id, self.input())
            )
            if len(targets) and isinstance(targets[0], ManifestInputTargetMixin):
                # spark ( on yarn ) has issues while reading ManifestInputTarget due to missing luigi configuration,
                # so we explicitly convert it to file target to fix it.
                targets = [get_target_from_url(targets[0].path)]
            self.requirements = targets
        return self.requirements

    def output(self):
        return self.get_target_paths()


class EventLogSelectionMixinSpark(EventLogSelectionDownstreamMixin):
    """
    Extract events corresponding to a specified time interval.
    """

    def __init__(self, *args, **kwargs):
        """
        Call path selection task to get list of log files matching the pattern
        """
        super(EventLogSelectionDownstreamMixin, self).__init__(*args, **kwargs)
        self.lower_bound_date_string = self.interval.date_a.strftime('%Y-%m-%d')  # pylint: disable=no-member
        self.upper_bound_date_string = self.interval.date_b.strftime('%Y-%m-%d')  # pylint: disable=no-member

    def get_log_schema(self):
        """
        Get spark based schema for processing event logs
        :return: Spark schema
        """
        from pyspark.sql.types import StructType, StringType
        event_schema = StructType().add("POST", StringType(), True).add("GET", StringType(), True)
        module_schema = StructType().add("display_name", StringType(), True) \
            .add("original_usage_key", StringType(), True) \
            .add("original_usage_version", StringType(), True) \
            .add("usage_key", StringType(), True)
        context_schema = StructType().add("command", StringType(), True) \
            .add("course_id", StringType(), True) \
            .add("module", module_schema) \
            .add("org_id", StringType(), True) \
            .add("path", StringType(), True) \
            .add("user_id", StringType(), True)

        event_log_schema = StructType() \
            .add("username", StringType(), True) \
            .add("event_type", StringType(), True) \
            .add("ip", StringType(), True) \
            .add("agent", StringType(), True) \
            .add("host", StringType(), True) \
            .add("referer", StringType(), True) \
            .add("accept_language", StringType(), True) \
            .add("event", event_schema) \
            .add("event_source", StringType(), True) \
            .add("context", context_schema) \
            .add("time", StringType(), True) \
            .add("name", StringType(), True) \
            .add("page", StringType(), True) \
            .add("session", StringType(), True)

        return event_log_schema

    def get_input_rdd(self):
        source_targets = luigi.task.flatten(self.input())
        if len(source_targets) > 0 and 'manifest' in source_targets[0].path:
            # Reading manifest as rdd with spark is alot faster as compared to hadoop.
            # Currently, we're getting only 1 manifest file per request, so we will create a single rdd from it.
            # If there are multiple manifest files, each file can be read as rdd and then union it with other manifest rdds
            self.log.warn("\nSPARK: Reading manifest file :: {} \n".format(source_targets[0].path))
            source_rdd = self._spark.sparkContext.textFile(source_targets[0].path)
        else:
            # maybe we only need to broadcast it ( on cluster ) and not create rdd. lets see
            self.log.warn("\nSPARK: Reading normal targets \n")
            source_rdd = self._spark.sparkContext.parallelize([target.path for target in source_targets])
        return source_rdd

    def get_event_log_dataframe(self, spark, *args, **kwargs):
        from pyspark.sql.functions import to_date, udf, struct, date_format
        dataframe = spark.read.format('json').load(self.get_input_rdd().collect(), schema=self.get_log_schema())
        dataframe = dataframe.filter(dataframe['time'].isNotNull()) \
            .withColumn('event_date', date_format(to_date(dataframe['time']), 'yyyy-MM-dd'))
        dataframe = dataframe.filter(
            (dataframe['event_date'] >= self.lower_bound_date_string) &
            (dataframe['event_date'] < self.upper_bound_date_string)
        )
        return dataframe


class SparkJobTask(OverwriteOutputMixin, EventLogSelectionDownstreamMixin, PySparkTask):
    """
    Wrapper for spark task
    """

    _spark = None
    _spark_context = None
    _sql_context = None
    _hive_context = None
    _tmp_dir = None
    log = None

    driver_memory = luigi.Parameter(
        config_path={'section': 'spark', 'name': 'driver-memory'},
        description='Memory for spark driver',
        significant=False,
    )
    executor_memory = luigi.Parameter(
        config_path={'section': 'spark', 'name': 'executor-memory'},
        description='Memory for each executor',
        significant=False,
    )
    executor_cores = luigi.Parameter(
        config_path={'section': 'spark', 'name': 'executor-cores'},
        description='No. of cores for each executor',
        significant=False,
    )
    spark_conf = luigi.Parameter(
        config_path={'section': 'spark', 'name': 'conf'},
        description='Spark configuration',
        significant=False,
        default=None
    )
    always_log_stderr = False  # log stderr if spark fails, True for verbose log

    def init_spark(self, sc):
        """
        Initialize spark, sql and hive context
        :param sc: Spark context
        """
        from pyspark.sql import SparkSession, SQLContext, HiveContext
        self._sql_context = SQLContext(sc)
        self._spark_context = sc
        self._spark = SparkSession.builder.getOrCreate()
        self._hive_context = HiveContext(sc)
        log4jLogger = sc._jvm.org.apache.log4j  # using spark logger
        self.log = log4jLogger.LogManager.getLogger(__name__)

    @property
    def conf(self):
        """
        Adds spark configuration to spark-submit task
        """
        return self._dict_config(self.spark_conf)

    def requires(self):
        yield PathSelectionTaskSpark(
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            date_pattern=self.date_pattern,
        )

    def spark_job(self):
        """
        Spark code for the job
        """
        raise NotImplementedError

    def get_config_from_args(self, key, *args, **kwargs):
        """
        Returns `value` of `key` after parsing string argument
        """
        default_value = kwargs.get('default_value', None)
        str_arg = args[0]
        config_dict = ast.literal_eval(str_arg)
        value = config_dict.get(key, default_value)
        return value

    def _load_internal_dependency_on_cluster(self, *args):
        """
        creates a zip of package and loads it on spark worker nodes

        Loading via luigi configuration does not work as it creates a tar file whereas spark does not load tar files
        """

        # import packages to be loaded on cluster
        import edx
        import luigi
        import opaque_keys
        import stevedore
        import bson
        import ccx_keys
        import cjson
        import boto
        import filechunkio
        import ciso8601
        import chardet
        import urllib3
        import certifi
        import idna
        import requests
        import six

        dependencies_list = []
        # get cluster dependencies from *args
        cluster_dependencies = self.get_config_from_args('cluster_dependencies', *args, default_value=None)
        if cluster_dependencies is not None:
            cluster_dependencies = json.loads(cluster_dependencies)
        if isinstance(cluster_dependencies, list):
            dependencies_list += cluster_dependencies

        packages = [edx, luigi, opaque_keys, stevedore, bson, ccx_keys, cjson, boto, filechunkio, ciso8601, chardet,
                    urllib3, certifi, idna, requests, six]
        self._tmp_dir = tempfile.mkdtemp()
        dependencies_list += create_packages_archive(packages, self._tmp_dir)
        if len(dependencies_list) > 0:
            for file in dependencies_list:
                self._spark_context.addPyFile(file)

    def get_luigi_configuration(self):
        """
        Return luigi configuration as dict for spark task

        luigi configuration cannot be retrieved directly from luigi's get_config method inside spark task
        """

        return None

    def app_options(self):
        """
        List of options that needs to be passed to spark task
        """
        options = {}
        task_config = self.get_luigi_configuration()  # load task dependencies first if any
        if isinstance(task_config, dict):
            options = task_config
        configuration = luigi.configuration.get_config()
        cluster_dependencies = configuration.get('spark', 'edx_egg_files', None)  # spark worker nodes dependency
        if cluster_dependencies is not None:
            options['cluster_dependencies'] = cluster_dependencies
        return [options]

    def _clean(self):
        """Do any cleanup after job here"""
        import shutil
        shutil.rmtree(self._tmp_dir)

    def main(self, sc, *args):
        self.init_spark(sc)  # initialize spark contexts
        self._load_internal_dependency_on_cluster(*args)  # load packages on EMR cluster for spark worker nodes
        self.spark_job(*args)  # execute spark job
        self._clean()  # cleanup after spark job
