
"""Import Orders: Shopping Cart Tables from the LMS, Orders from Otto."""

import luigi
import luigi.hdfs

from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.database_imports import (
    DatabaseImportMixin,
    ImportShoppingCartCertificateItem,
    ImportShoppingCartCourseRegistrationCodeItem,
    ImportShoppingCartDonation,
    ImportShoppingCartOrder,
    ImportShoppingCartOrderItem,
    ImportShoppingCartPaidCourseRegistration,
    ImportProductCatalog,
    ImportProductCatalogAttributes,
    ImportProductCatalogAttributeValues,
    ImportOrderOrderHistory,
    ImportOrderHistoricalLine,
    ImportCurrentBasketState,
    ImportCurrentOrderState,
    ImportCurrentOrderLineState,
    ImportCurrentOrderLineAttributeState,
    ImportCurrentOrderLinePriceState,
    ImportOrderPaymentEvent,
    ImportPaymentSource,
    ImportPaymentTransactions,
    ImportPaymentProcessorResponse,
    ImportCurrentRefundRefundState,
    ImportCurrentRefundRefundLineState,
    ImportRefundHistoricalRefund,
    ImportRefundHistoricalRefundLine,
)


class PullFromShoppingCartTablesTask(DatabaseImportMixin, OverwriteOutputMixin, luigi.WrapperTask):
    """Imports a set of shopping cart database tables from an external LMS RDBMS into a destination directory."""

    def requires(self):
        kwargs = {
            'destination': self.destination,
            'credentials': self.credentials,
            'num_mappers': self.num_mappers,
            'verbose': self.verbose,
            'import_date': self.import_date,
            'overwrite': self.overwrite,
        }
        yield (
            # Original shopping cart tables.
            ImportShoppingCartOrder(**kwargs),
            ImportShoppingCartOrderItem(**kwargs),
            ImportShoppingCartCertificateItem(**kwargs),
            ImportShoppingCartPaidCourseRegistration(**kwargs),
            ImportShoppingCartDonation(**kwargs),
            ImportShoppingCartCourseRegistrationCodeItem(**kwargs),
        )

    def output(self):
        return [task.output() for task in self.requires()]


class PullFromEcommerceTablesTask(DatabaseImportMixin, OverwriteOutputMixin, luigi.WrapperTask):
    """Imports a set of ecommerce tables from an external database into a destination directory."""

    destination = luigi.Parameter(
        default_from_config={'section': 'otto-database-import', 'name': 'destination'}
    )
    credentials = luigi.Parameter(
        default_from_config={'section': 'otto-database-import', 'name': 'credentials'}
    )
    database = luigi.Parameter(
        default_from_config={'section': 'otto-database-import', 'name': 'database'}
    )

    def requires(self):
        kwargs = {
            'destination': self.destination,
            'credentials': self.credentials,
            'num_mappers': self.num_mappers,
            'verbose': self.verbose,
            'import_date': self.import_date,
            'overwrite': self.overwrite,
            'database': self.database,
        }
        yield (
            # Otto Product Tables.
            ImportProductCatalog(**kwargs),
            ImportProductCatalogAttributes(**kwargs),
            ImportProductCatalogAttributeValues(**kwargs),

            # Otto Order History Tables.
            ImportOrderOrderHistory(**kwargs),
            ImportOrderHistoricalLine(**kwargs),

            # Otto Current State and Line Item Tables.
            ImportCurrentBasketState(**kwargs),
            ImportCurrentOrderState(**kwargs),
            ImportCurrentOrderLineState(**kwargs),
            ImportCurrentOrderLineAttributeState(**kwargs),
            ImportCurrentOrderLinePriceState(**kwargs),

            # Otto Payment Tables.
            ImportOrderPaymentEvent(**kwargs),
            ImportPaymentSource(**kwargs),
            ImportPaymentTransactions(**kwargs),
            ImportPaymentProcessorResponse(**kwargs),

            # Otto Refund Tables.
            ImportCurrentRefundRefundState(**kwargs),
            ImportCurrentRefundRefundLineState(**kwargs),
            ImportRefundHistoricalRefund(**kwargs),
            ImportRefundHistoricalRefundLine(**kwargs),
        )

    def output(self):
        return [task.output() for task in self.requires()]