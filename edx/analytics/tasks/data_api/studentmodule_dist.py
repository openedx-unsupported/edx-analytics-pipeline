"""
Tasks to compute histogram distribution from the courseware_studentmodule table in edx-platform's mysql database,
either by producing a new dump with sqoop or using existing dumps, and export them into analytics-data-api's
mysql database.
"""

import csv
import logging
from collections import defaultdict

import luigi

from edx.analytics.tasks.common.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.common.mysql_load import MysqlInsertTask
from edx.analytics.tasks.common.sqoop import SqoopImportFromMysql
from edx.analytics.tasks.export.database_exports import FIELD_SIZE_LIMIT, StudentModuleRecord
from edx.analytics.tasks.util import csv_util
from edx.analytics.tasks.util.url import get_target_from_url, url_path_join

log = logging.getLogger(__name__)


# Increase maximum number of characters per field since we have
# entries that easily exceed the default value of 124 KB.
csv.field_size_limit(FIELD_SIZE_LIMIT)


######################################################################
#       Abstract Import and Histogram Calculation Section            #
######################################################################
class HistogramTaskFromSqoopParamsMixin(object):
    """
    Mixin the parameters for HistogramsFromStudentModule that involve Sqoop

    """
    name = luigi.Parameter(
        description='Name of this run',
    )
    dest = luigi.Parameter(
        description='URL of S3 location/directory where the task outputs',
    )
    credentials = luigi.Parameter(
        config_path={'section': 'database-export', 'name': 'credentials'},
        description='Credentials for the analytics db',
    )
    sqoop_overwrite = luigi.BoolParameter(  # prefixed with sqoop for disambiguation
        default=False,
        description='Overwrite any existing imports.',
    )
    num_mappers = luigi.Parameter(   # TODO: move to config
        default=None,
        significant=False,
        description='Number of mappers for Sqoop to use',
    )


class HistogramFromStudentModuleSqoopWorkflowBase(
    HistogramTaskFromSqoopParamsMixin,
    MapReduceJobTask
):
    """
    Abstract base class for taking a Sqoop dump of a courseware_studentmodule table and calculating histogram data

    Implementers must override mapper_yield and reducer_yield
    """
    def mapper(self, line):
        """
        Yields the correct values by consulting mapper_yield
        """
        values = csv_util.parse_line(line, dialect='mysqldump')
        record = StudentModuleRecord(*values)
        return self.mapper_yield(record)  # return generator we got from yield

    def mapper_yield(self, record):
        """
        Takes a record:StudentModuleRecord and decides how the mapper should yield the data.

        Subclasses need to implement this.  Must "return" a generator (via yield)
        """
        raise NotImplementedError

    def reducer(self, key, values):
        """
        Collates values and produces histogram data
        """
        histogram = defaultdict(int)  # int() returns 0
        for v in values:
            histogram[v] += 1
        return self.reducer_yield(key, histogram)  # return generator we got from yield

    def reducer_yield(self, reducer_key, histogram):
        """
        Formats the yielded output from the histogram.  Subclasses need to implement this.

        Must "return" a generator (via yield)

        Parameters:
            * reducer_key: key fed into the reducer
            * histogram: the computed histogram
        """
        raise NotImplementedError

    def requires(self):
        table_name = 'courseware_studentmodule'
        return SqoopImportFromMysql(
            credentials=self.credentials,
            destination=url_path_join(self.dest, table_name),
            table_name=table_name,
            num_mappers=self.num_mappers,
            overwrite=self.sqoop_overwrite,
        )


######################################################################
#             Abstract Export to Mysql Workflow Section              #
######################################################################
class HistogramFromSqoopToMySQLWorkflowBase(
    HistogramTaskFromSqoopParamsMixin,
    MapReduceJobTaskMixin,
    MysqlInsertTask
):
    """
    An abstract base class that formulates the workflow that runs sqoop to produce a sql dump, generates
    a histogram, and then exports to the analytics database.

    Clears the target mysql table before writing.

    Implementers must inherit from this base class and define the properties specifying of the mysql export
    i.e. table, columns, indexes, etc, (or mix it in, e.g. InsertToMysqlGradeDistTableMixin).
    They must also override sqoop_histogram_task

    One notable subtlety is that this needs 2 sets of mysql credentials,
    Parameter "--import-credentials" is the location of the readonly edx-platform db creds
    Parameter "--credentials" is the location of the analytics-db creds
    """
    sqoop_histogram_task = None  # implementers must override default

    import_credentials = luigi.Parameter(
        config_path={'section': 'database-import', 'name': 'credentials'},
        description='Credentials for the readonly edx-platform db',
    )

    overwrite = True  # always overwrite the exported MySQL table.  Independent of sqoop_overwrite, which is for import

    @property
    def insert_source_task(self):
        return self.sqoop_histogram_task(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            name=self.name,
            dest=self.dest,
            credentials=self.import_credentials,
            num_mappers=self.num_mappers,
            sqoop_overwrite=self.sqoop_overwrite,
        )


######################################################################
#                 Grade Distribution Section                         #
######################################################################
class GradeDistFromStudentModuleMixin(object):
    """
    Takes a SQL dump of a courseware_studentmodule table and calculates the grade distribution per module
    """
    def mapper_yield(self, record):
        if record.grade != "NULL":
            yield (record.module_id, record.course_id), (record.grade, record.max_grade)

    def reducer_yield(self, reducer_key, histogram):
        for k in histogram:
            yield [
                reducer_key[0],  # module_id
                reducer_key[1],  # course_id
                k[0] if k[0] != "NULL" else None,  # grade, converting "NULL" -> None
                k[1] if k[1] != "NULL" else None,  # max_grade, converting "NULL" -> None
                histogram[k],  # count
            ]

    def output(self):
        output_name = u'grade_dist_{name}/'.format(name=self.name)
        return get_target_from_url(url_path_join(self.dest, output_name))


class GradeDistFromSqoopToTSVWorkflow(
    GradeDistFromStudentModuleMixin,
    HistogramFromStudentModuleSqoopWorkflowBase,
):
    """
    Task that sqoops a SQL dump of a courseware_studentmodule table, calculates the grade distribution per module,
    and outputs a TSV

    This is a concrete class that does work, but it's simply a composition that needs no code.  Instead of
    defining mapper_yield and reducer_yield in this class, we get it from a mixin so as to not copy-paste code
    """
    pass


class InsertToMysqlGradeDistTableMixin(object):
    """
    Define grade_distribution table.
    """
    @property
    def table(self):
        return "grade_distribution"

    @property
    def columns(self):
        return [
            ('module_id', 'VARCHAR(255) NOT NULL'),
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('grade', 'DOUBLE'),
            ('max_grade', 'DOUBLE'),
            ('count', 'INT(11) NOT NULL'),
        ]

    @property
    def indexes(self):
        return [
            ('module_id',),
            ('course_id',),
        ]


class GradeDistFromSqoopToMySQLWorkflow(
    InsertToMysqlGradeDistTableMixin,
    HistogramFromSqoopToMySQLWorkflowBase,
):
    """
    Task class that writes to grade_distribution table from GradeDistFromStudentModuleSqoopWorkflow.

    Gets most of its functionality from inheritance.  E.g. uses InsertToMysqlGradeDistTableMixin
    instead of copy-paste
    """
    sqoop_histogram_task = GradeDistFromSqoopToTSVWorkflow


######################################################################
#                  Open Distribution Section                         #
######################################################################
class SeqOpenDistFromStudentModuleMixin(object):
    """
    Takes a SQL dump of a courseware_studentmodule table and calculates the "open" count per subsection/sequential

    The open count is just the number of existent rows for that subsection/sequential, which indicates that a student
    has instantiated that subsection/sequential ("opened").
    """
    def mapper_yield(self, record):
        if record.module_type == "sequential":
            yield (record.module_id, record.course_id), record.module_id  # values should all be identical per key

    def reducer_yield(self, reducer_key, histogram):
        for k in histogram:
            yield [
                reducer_key[0],  # module_id
                reducer_key[1],  # course_id
                histogram[k],  # count
            ]

    def output(self):
        output_name = u'seq_open_dist_{name}/'.format(name=self.name)
        return get_target_from_url(url_path_join(self.dest, output_name))


class SeqOpenDistFromSqoopToTSVWorkflow(
    SeqOpenDistFromStudentModuleMixin,
    HistogramFromStudentModuleSqoopWorkflowBase,
):
    """
    Sqoops a SQL dump of courseware_studentmodule and calculates the "open" distribution per subsection/sequential

    This is a concrete class that does work, but it's simply a composition that needs no code
    e.g. it gets the redefinition of mapper_yield and reducer_yield from SeqOpenDistFromStudentModuleMixin
    instead of copy-pasting code
    """
    pass


class InsertToMysqlSeqOpenDistTableMixin(object):
    """
    Define sequential_open_distribution table.
    """
    @property
    def table(self):
        return "sequential_open_distribution"

    @property
    def columns(self):
        return [
            ('module_id', 'VARCHAR(255) NOT NULL'),
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('count', 'INT(11) NOT NULL'),
        ]

    @property
    def indexes(self):
        return [
            ('module_id',),
            ('course_id',),
        ]


class SeqOpenDistFromSqoopToMySQLWorkflow(
    InsertToMysqlSeqOpenDistTableMixin,
    HistogramFromSqoopToMySQLWorkflowBase,
):
    """
    Task class that writes to sequential_open_distribution table from SeqOpenDistFromStudentModuleSqoopWorkflow.

    Gets most of its functionality from inheritance
    E.g. gets the table definition from InsertToMysqlSeqOpenDistTableMixin instead of copy-paste code.
    """
    sqoop_histogram_task = SeqOpenDistFromSqoopToTSVWorkflow
