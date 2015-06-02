
"""Import Shopping Cart Tables from the LMS."""

import luigi
import luigi.hdfs

from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.database_imports import DatabaseImportMixin,\
    ImportShoppingCartCertificateItem,\
    ImportShoppingCartCourseRegistrationCodeItem,\
    ImportShoppingCartDonation,\
    ImportShoppingCartOrder,\
    ImportShoppingCartOrderItem,\
    ImportShoppingCartPaidCourseRegistration


class ShoppingCartTables(DatabaseImportMixin, OverwriteOutputMixin, luigi.WrapperTask):
    """Imports a set of shopping cart database tables from an external LMS RDBMS into a destination directory."""

    def requires(self):
        kwargs = {
            'destination': self.destination,
            'credentials': self.credentials
        }
        yield (
            ImportShoppingCartOrder(**kwargs),
            ImportShoppingCartOrderItem(**kwargs),
            ImportShoppingCartCertificateItem(**kwargs),
            ImportShoppingCartPaidCourseRegistration(**kwargs),
            ImportShoppingCartDonation(**kwargs),
            ImportShoppingCartCourseRegistrationCodeItem(**kwargs),
        )

    def output(self):
        return [task.output() for task in self.requires()]