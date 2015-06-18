import luigi
import luigi.hdfs

from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.database_imports import (DatabaseImportMixin,ImportCourseModeTask)

class CourseModeTableTask(DatabaseImportMixin, OverwriteOutputMixin, luigi.WrapperTask):
    """
    Imports the course mode table from an external LMS RDBMS into a both destination directory and HIVE metastore.

    """
    def requires(self):
        kwargs = {
            'num_mappers': self.num_mappers,
            'verbose': self.verbose,
            'import_date': self.import_date,
            'overwrite': self.overwrite,
        }
        yield (
            ImportCourseModeTask(
                destination=self.destination,
                credentials=self.credentials,
                database=self.database,
                **kwargs
            )
        )

    def output(self):
        return [task.output() for task in self.requires()]
