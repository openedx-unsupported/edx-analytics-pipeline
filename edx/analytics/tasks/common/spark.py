import ast
import datetime
import importlib
import json
import logging
import os
import shutil
import tempfile
import zipfile
from collections import defaultdict

import luigi
import luigi.configuration
from edx.analytics.tasks.util.hive import WarehouseMixin
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.url import get_target_from_url, url_path_join
from luigi.contrib.spark import PySparkTask

_file_path_to_package_meta_path = {}

log = logging.getLogger(__name__)


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


class SparkMixin():
    """
    Provides `spark-submit` parameters that can be set on the command-line, as well as a configuration file.

    Most `spark-submit` parameters defined in SparkSubmitTask are only pulled from config files.

    See documentation for `spark-submit` for more details on the particular parameters.

    """
    # Note that all of these are hard-coded to pull from the 'spark' section of the configuration file.
    # In SparkSubmitTask, the 'spark-version' that defines the section is a property that can be overridden
    # in derived classes.  Because this is a standalone mixin, it doesn't have access to that property.

    driver_memory = luigi.Parameter(
        config_path={'section': 'spark', 'name': 'driver-memory'},
        description='Memory for spark driver.',
        significant=False,
    )
    executor_memory = luigi.Parameter(
        config_path={'section': 'spark', 'name': 'executor-memory'},
        description='Memory for each executor.',
        significant=False,
    )
    executor_cores = luigi.Parameter(
        config_path={'section': 'spark', 'name': 'executor-cores'},
        description='Number of cores for each executor.',
        significant=False,
    )
    spark_conf = luigi.Parameter(
        config_path={'section': 'spark', 'name': 'conf'},
        description="Spark configuration, specified with '|' as delimiter of key=value pairs.",
        significant=False,
        default=None
    )
    always_log_stderr = False  # log stderr if spark fails, True for verbose log


class SparkJobTask(SparkMixin, PySparkTask):
    """
    Base class for running a launchable Spark task.

    This adds functionality to PySparkTask for loading Python packages onto Spark workers to support
    Python user-defined functions (UDFs) running there.  It also provides a mechanism for passing
    configuration information from Luigi into the Spark 'spark-submit' subprocess.

    Note that methods that begin with 'spark_' or '_spark_' are executed within the 'spark-submit'
    subprocess, while others will be executed in the main process (that contains Luigi).
    """

    _spark = None
    _spark_context = None
    _tmp_dir = None
    log = None
    _requested_config = defaultdict(list)

    def spark_job(self):
        """
        Spark code for the job.
        """
        raise NotImplementedError

    @property
    def spark_remote_package_names(self):
        """
        List of package names that should be loaded onto remote Spark workers in a cluster.

        Redefine this list at the task level, depending on the UDF functionality the task requires.

        For example, to make use of opaque_key utility methods in a UDF, you would want to specify
        the following:

                return ['edx', 'opaque_keys', 'stevedore', 'bson', 'six']

        If you want to support parsing of ccx-type identifiers as well, also include 'ccx_keys' in the list.

        """
        return []

    def request_configuration_from_luigi(self, section, parameter):
        """
        Registers configuration values needed for this Spark task, to be pulled from Luigi configuration.

        Luigi configuration cannot be retrieved directly from Luigi's get_config() method inside a Spark task,
        so configuration is passed to spark-submit on the command-line, and makes its way
        to the args parameter passed to spark_job().

        Note that Luigi parameters defined explicitly on a task or mixin are available to the Spark side.

        The requests are stored in a dictionary with keys being the section names, and the values a list of parameter names to
        include from the particular section.
        """
        self._requested_config[section].append(parameter)

    def spark_get_config_from_args(self, section, key, default_value=None):
        """
        Allows parameters to be fetched from the arguments parameter passed to spark_job().

        This is the mirror of request_configuration_from_luigi.

        Returns `value` of `key` in given `section`, if any were requested and found in Luigi configuration.
        """
        # Return the default value if either the section or the key are missing.
        return self._spark_task_config.get(section, {key: default_value}).get(key, default_value)

    @property
    def spark_logger_name(self):
        """
        Define class path to display in Spark logging.

        By default, this uses the Python module name for the task, which is probably adequate most of the time.
        """
        return "{}.{}".format(self.__class__.__module__, self.__class__.__name__)

    def _spark_initialize(self, sc, *args):
        """
        Initialize Spark, SQL and Hive context.
        :param sc: Spark context
        """
        from pyspark.sql import SparkSession
        self._spark_context = sc
        # Note that this doesn't actually use sc.  It just gets
        # the currently-existing Spark session.
        self._spark = SparkSession.builder.getOrCreate()

        self._tmp_dir = tempfile.mkdtemp()

        # Convert args to task configuration:
        self._spark_task_config = self._spark_get_config_from_app_options(*args)

        log4j_logger = sc._jvm.org.apache.log4j  # using spark logger
        self.log = log4j_logger.LogManager.getLogger(self.spark_logger_name)

    @property
    def conf(self):
        """
        Adds spark configuration to spark-submit task.

        This overrides the `conf` parameter defined in SparkSubmitTask, which reads a dict of Spark configuration parameters
        from Luigi configuration files only. The override here permits configuration to be specified on the command line
        (using the `spark_conf` Luigi parameter), and only defaults to reading from configuration if the command line is not used.
        """
        return self._dict_config(self.spark_conf)

    def app_options(self):
        """
        Dictionary of options that needs to be passed to Spark task.

        This overrides the empty SparkSubmitTask default.  It should not be necessary for tasks to redefine this.
        """
        output_config = defaultdict(dict)

        # Add information about prebuilt modules here, to make sure that we grab that information even
        # if the client didn't explicitly request it for their task.
        self.request_configuration_from_luigi('spark', 'prebuilt_python_modules')

        luigi_config = luigi.configuration.get_config()
        for requested_section in self._requested_config:
            for requested_parameter in self._requested_config[requested_section]:
                value = luigi_config.get(requested_section, requested_parameter, None)
                if value:
                    output_config[requested_section][requested_parameter] = value

        # Cast away the defaultdict-ness before putting into the final list form,
        # so that ast.literal_eval can interpret it as a plain dict.
        options_list = [dict(output_config)]
        return options_list

    def _spark_get_config_from_app_options(self, *args):
        """
        Pull configuration out of args, mirroring the actions of app_options to put configuration in.
        """
        str_arg = str(args[0])
        return ast.literal_eval(str_arg)

    def _spark_load_internal_dependency_on_cluster(self):
        """
        Loads .egg and/or .zip files into Spark context for use on remote Spark workers.

        Uses the `spark_remote_package_names` property to indicate the packages that should be loaded.  It
        looks first for prebuilt .egg or .zip files that were defined in Luigi configuration as 'prebuilt_python_modules'.
        Any packages that it doesn't find already prebuilt, it will add to a single zipfile, and this will also be loaded
        onto the Spark worker nodes.

        Loading of packages via a single zipfile is adequate for most packages.  However, it is not adequate for
        packages like opaque_keys or ccx_keys, which must provide extension point metadata information in order to
        work properly as Stevedore-based plugins.

        Loading via PySparkTask.setup_remote() does not work, as it passes a tar file to addPyFile().
        Spark's addPyFile() does not load tar files, only .zip or .egg files.   Go figure.
        """
        # Get dictionary of prebuilt python modules from *args.
        prebuilt_python_modules = self.spark_get_config_from_args('spark', 'prebuilt_python_modules', default_value=None)
        if prebuilt_python_modules is not None:
            prebuilt_python_modules = json.loads(prebuilt_python_modules)

        dependencies_list = []
        packages_to_archive = []
        for package in self.spark_remote_package_names:
            if prebuilt_python_modules is not None and package in prebuilt_python_modules:
                dependencies_list.append(prebuilt_python_modules[package])
            else:
                mod = importlib.import_module(package)
                packages_to_archive.append(mod)

        if packages_to_archive:
            dependencies_list += create_packages_archive(packages_to_archive, self._tmp_dir)

        self.log.warn("List of dependencies to load into Spark context: {}".format(dependencies_list))
        if len(dependencies_list) > 0:
            for file in dependencies_list:
                self._spark_context.addPyFile(file)

    def _spark_clean(self):
        """
        Do any cleanup after job here.

        """
        if self._tmp_dir:
            shutil.rmtree(self._tmp_dir)

    def main(self, sc, *args):
        """
        Defines the main extension point for PySparkTask, providing its own 'spark_job' extension point.
        """
        try:
            self._spark_initialize(sc, *args)  # initialize spark contexts
            self._spark_load_internal_dependency_on_cluster()  # load packages into context for Spark worker nodes.
            self.spark_job(*args)  # execute spark job
        finally:
            self._spark_clean()  # cleanup after spark job


class SparkMysqlMixin(object):
    import_date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        description='Date to assign to partition.  Default is today\'s date, UTC.',
    )
    destination = luigi.Parameter(
        config_path={'section': 'database-import', 'name': 'destination'},
        description='The directory to write the output.',
    )
    num_partitions = luigi.Parameter(
        default=2,
        description='Number of partitions in dataframe while reading/writing via jdbc '
                    '( this also sets the no. of connections to the database).'
    )
    mysql_driver_class = "com.mysql.jdbc.Driver"
    credentials = None
    database = None

    def get_credentials(self):
        """
        Gathers the secure connection parameters from an external file
        and uses them to establish a connection to the database
        specified in the secure parameters.

        """
        cred = {}
        credentials_source = get_target_from_url(url=self.credentials_file)
        with credentials_source.open('r') as credentials_file:
            cred = json.load(credentials_file)
        return cred

    @property
    def spark_remote_package_names(self):
        return ['edx', 'luigi', 'stevedore', 'bson', 'six']

    def get_jdbc_url(self):
        if self.credentials:
            return "jdbc:mysql://{host}:{port}/{database}".format(
                host=self.credentials.get('host', ''),
                port=self.credentials.get('port', ''),
                database=self.database
            )
        return ''


class SparkMysqlExportMixin(SparkMysqlMixin, OverwriteOutputMixin):
    """Defines arguments used by spark mysql export tasks to pass to SparkExportFromMysqlTaskMixin."""
    credentials_file = luigi.Parameter(
        config_path={'section': 'database-import', 'name': 'credentials'},
        description='Path to the external access credentials file.',
    )
    database = luigi.Parameter(
        config_path={'section': 'database-import', 'name': 'database'},
        description='The name of the database to read the data.',
    )


class SparkMysqlImportMixin(SparkMysqlMixin):
    """Defines arguments used for importing data into mysql using spark."""
    credentials_file = luigi.Parameter(
        config_path={'section': 'database-export', 'name': 'credentials'},
        description='Path to the external access credentials file.',
    )
    database = luigi.Parameter(
        config_path={'section': 'database-export', 'name': 'database'},
        description='The name of the database to write the data.',
    )
    marker = luigi.Parameter(
        config_path={'section': 'map-reduce', 'name': 'marker'},
        significant=False,
        description='A URL location to a directory where a marker file will be written on task completion.',
    )


class SparkExportFromMysqlTaskMixin(SparkMysqlExportMixin, SparkJobTask):
    """Spark task mixin for exporting from mysql to an external directory path."""

    def run(self):
        self.remove_output_on_overwrite()
        self.credentials = self.get_credentials()
        super(SparkExportFromMysqlTaskMixin, self).run()

    @property
    def table_name(self):
        """Provides name of mysql table."""
        raise NotImplementedError

    @property
    def table_location(self):
        return url_path_join(self.destination, self.table_name)

    @property
    def partition_date(self):
        """Provides value to use in constructing the partition name of Hive database table."""
        return str(self.import_date)

    @property
    def partition(self):
        """Provides name of Hive database table partition."""
        # The Luigi hive code expects partitions to be defined by dictionaries.
        return {'dt': self.partition_date}

    @property
    def partition_location(self):
        """Provides location of table's partition data."""
        # The actual folder name where the data is stored is expected to be in the format <key>=<value>
        partition_name = '='.join(self.partition.items()[0])
        # Make sure that input path ends with a slash, to indicate a directory.
        return url_path_join(self.table_location, partition_name + '/')

    @property
    def output_delimiter(self):
        """Field delimiter for output columns"""
        return '\x01'

    # TODO: Add option to either export the results or create global temporary views
    def spark_job(self, *args):
        df = self._spark.read.format("jdbc").options(
            url=self.get_jdbc_url(),
            dbtable=self.table_name,
            user=self.credentials.get("username", ''),
            password=self.credentials.get("password", ''),
            driver=self.mysql_driver_class,
            numPartitions=self.num_partitions
        ).load()
        # We cannot impose schema on jdbc source, so we explicitly cast them to string types.
        # We need to cast columns if we want to replace null values with some placeholder, but for discovery purpose
        # i've commented this out.
        # from pyspark.sql.functions import col
        # from pyspark.sql.types import StringType
        # exprs = [col(c).cast(StringType()) for c in df.columns]
        # df = df.select(*exprs)       # select cast columns
        # df = df.fillna('\N')         # replace nulls with placeholder ( for hive compatability )
        df.select(self.columns) \
            .write \
            .csv(self.output().path, mode='overwrite', sep=self.output_delimiter)

    def output(self):
        return get_target_from_url(self.partition_location)


class ImportAuthUserSparkTask(SparkExportFromMysqlTaskMixin):
    """Spark equivalent of ImportAuthUserTask."""

    @property
    def table_name(self):
        return "auth_user"

    @property
    def columns(self):
        # Fields not included are 'password', 'first_name' and 'last_name'.
        return ['id', 'username', 'last_login', 'date_joined', 'is_active', 'is_superuser', 'is_staff', 'email']


class ImportAuthUserProfileSparkTask(SparkExportFromMysqlTaskMixin):
    """Spark equivalent of ImportAuthUserProfileTask."""

    @property
    def table_name(self):
        return "auth_userprofile"

    @property
    def columns(self):
        return ['user_id', 'name', 'gender', 'year_of_birth', 'level_of_education', 'language', 'location',
                'mailing_address', 'city', 'country', 'goals']


class SparkTest(WarehouseMixin, SparkJobTask):
    input_source = luigi.Parameter(
        default=None
    )
    column_delimiter = luigi.Parameter(
        default='\x01'
    )

    def _spark_clean(self):
        # not deleting temporary files for debugging
        pass

    @property
    def spark_remote_package_names(self):
        return ['edx', 'luigi', 'stevedore', 'bson', 'six']

    def spark_job(self, *args):
        df = self._spark.read.format('csv').options(delimiter=self.column_delimiter).load(self.input_source)
        df = df.groupBy('_c3').count()
        df.coalesce(1).write.csv(self.output().path, mode='overwrite')

    def output(self):
        return get_target_from_url(
            url_path_join(
                self.warehouse_path,
                'spark_test_job'
            )
        )
