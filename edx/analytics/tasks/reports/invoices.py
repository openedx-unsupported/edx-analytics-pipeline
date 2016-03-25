import luigi

from edx.analytics.tasks.mapreduce import MapReduceJobTaskMixin
from edx.analytics.tasks.util.hive import HiveTableFromQueryTask


class InvoiceTransactionsIntervalTask(luigi.WrapperTask):
    """
    Creates TSV files with information about invoice transactions for a specified interval.
    """
    pass

class ShoppingCartInvoiceTransactionsPerDayTask(MapReduceJobTaskMixin):
    """
    Creates TSV file with information about ShoppingCart invoices.
    """
    pass

class OttoInvoiceTransactionsPerDayTask(MapReduceJobTaskMixin):
    """
    Creates TSV file with information about Otto invoices.
    """
    pass

class OttoInvoiceTransactionsTask(HiveTableFromQueryTask):
    """
    Adds information about currency and amount charged to Otto invoices.
    """
    pass
