"""Create history tables from Hive-imported tables."""
import logging
import textwrap

import luigi

from edx.analytics.tasks.common.vertica_load import VerticaCopyTask, VerticaCopyTaskMixin
from edx.analytics.tasks.insights.database_imports import (
    DatabaseImportMixin,
    ImportProductCatalog,
    ImportProductCatalogClass,
    ImportStudentCourseEnrollmentTask,
    ImportAuthUserTask,
    ImportAuthUserProfileTask,
    ImportCouponVoucherIndirectionState,
    ImportCouponVoucherState,
    ImportCourseEntitlementTask,
    ImportCurrentOrderDiscountState,
    ImportCurrentOrderLineState,
    ImportCurrentOrderState,
    ImportCurrentRefundRefundLineState,
    ImportEcommercePartner,
    ImportEcommerceUser,
    ImportProductCatalogAttributes,
    ImportProductCatalogAttributeValues,
    ImportShoppingCartCertificateItem,
    ImportShoppingCartCoupon,
    ImportShoppingCartCouponRedemption,
    ImportShoppingCartCourseRegistrationCodeItem,
    ImportShoppingCartDonation,
    ImportShoppingCartOrder,
    ImportShoppingCartOrderItem,
    ImportShoppingCartPaidCourseRegistration,
)
from edx.analytics.tasks.enterprise.enterprise_database_imports import (
    ImportBenefitTask,
    ImportConditionalOfferTask,
    ImportDataSharingConsentTask,
    ImportEnterpriseCourseEnrollmentUserTask,
    ImportEnterpriseCustomerTask,
    ImportEnterpriseCustomerUserTask,
    ImportStockRecordTask,
    ImportUserSocialAuthTask,
    ImportVoucherTask
)
# Of these, which contain fields that are > 80 characters long?  Cannot tell from
# the column definitions on import, because they are STRINGs.  Could look at outputs on
# join, but probably better to actually look at individual column values, and do
# MAX(LENGTH(a)).  Fortunately, course_id seems okay, on enrollment table.
# Workaround:  change VARCHAR to VARCHAR(255) for all STRINGS.  Done.

from edx.analytics.tasks.util.hive import hive_database_name, HivePartition, HiveTableTask, WarehouseMixin
from edx.analytics.tasks.util.url import get_target_from_url

log = logging.getLogger(__name__)


class BaseHiveHistoryTask(DatabaseImportMixin, HiveTableTask):
    # otto_credentials = luigi.Parameter(
    #     config_path={'section': 'otto-database-import', 'name': 'credentials'}
    # )
    # otto_database = luigi.Parameter(
    #     config_path={'section': 'otto-database-import', 'name': 'database'}
    # )

    source_task = None

    @property
    def source_import_class(self):
        """The class object that loads the source table into Hive."""
        raise NotImplementedError

    def requires(self):
        if self.source_task is None:
            kwargs = {
                'destination': self.destination,
                'num_mappers': self.num_mappers,
                'verbose': self.verbose,
                'import_date': self.import_date,
                'overwrite': self.overwrite,
                # 'credentials': self.otto_credentials,
                # 'database': self.otto_database,
                # Use LMS credentials instead
                'credentials': self.credentials,
                'database': self.database,
            }
            source_task = self.source_import_class(**kwargs)
        return source_task

    def get_source_task(self):
        return self.requires()
    
    @property
    def source_table(self):
        return self.get_source_task().table_name

    @property
    def source_columns(self):
        return self.get_source_task().columns

    @property
    def source_column_names(self):
        return [name for (name, type) in self.source_columns]

    @property
    def table(self):
        """The name of the output table in Hive, based on the source table."""
        return "{name}_history".format(name=self.source_table)

    @property
    def columns(self):
        column_names = self.source_column_names
        if column_names[0] <> 'id':
            raise Exception("first column in source columns is not named 'id'")
        
        # Map everything to a STRING, except the 'id' field, and add standard history fields.
        columns = [('id', 'INT'),]
        columns.extend([(name, 'STRING') for name in column_names[1:]])
        columns.extend([
            ('history_id', 'STRING'),
            ('history_date', 'STRING'),
            ('history_change_reason', 'STRING'),
            ('history_type', 'STRING'),
            ('history_user_id', 'STRING'),
        ])
        return columns
        
    @property
    def partition(self):
        # TODO: import_date is not necessarily the right location for output, given that it can be any date
        # in the past, as long as it exists (so we don't reload it).  Only if it's today or yesterday might
        # this better match the set of dates that are available, by roughly being the latest date.
        # On the other hand, maybe we could calculate that based on files in S3, and set import_date
        # appropriately to whatever exists.
        return HivePartition('dt', self.import_date.isoformat())  # pylint: disable=no-member

    @property
    def load_input_table_query(self):
        # Make sure that all tables are loaded from the input table.
        query = "MSCK REPAIR TABLE {source_table};".format(source_table=self.source_table)
        log.info('load_input_table_query: %s', query)
        return query

    @property
    def create_table_query(self):
        # Ensure there is exactly one available partition in the output table.
        query_format = """
            USE {database_name};
            DROP TABLE IF EXISTS `{table}`;
            CREATE EXTERNAL TABLE `{table}` (
                {col_spec}
            )
            PARTITIONED BY (`{partition.key}` STRING)
            {table_format}
            LOCATION '{location}';
            ALTER TABLE `{table}` ADD PARTITION ({partition.query_spec});
        """
        query = query_format.format(
            database_name=hive_database_name(),
            table=self.table,
            col_spec=','.join(['`{}` {}'.format(name, col_type) for name, col_type in self.columns]),
            location=self.table_location,
            table_format=self.table_format,
            partition=self.partition,
        )
        query = textwrap.dedent(query)
        log.info('create_table_query: %s', query)
        return query
    
    def query(self):
        full_insert_query = """
            INSERT INTO TABLE `{table}`
            PARTITION ({partition.query_spec})
            {insert_query}
        """.format(
            table=self.table,
            partition=self.partition,
            insert_query=self.insert_query.strip(),  # pylint: disable=no-member
        )

        full_query = self.create_table_query + self.load_input_table_query + textwrap.dedent(full_insert_query)
        log.info('History-creating hive query: %s', full_query)
        return full_query

    def output(self):
        # This is magically copied from HiveTableFromQueryTask.
        return get_target_from_url(self.partition_location.rstrip('/') + '/')

    def intermediate_columns(self):
        # BOOLEAN types in Hive have to be converted explicitly, because they cannot be coerced into Strings.  (Same with BINARY, FWIW.)
        coalesce_pairs = list()
        for (name, type) in self.source_columns:
            if type.lower() == 'boolean':
                pair = "COALESCE(IF({name},'1','0'),'NNULLL') AS {name}, COALESCE(IF(LAG({name}) OVER w,'1','0'),'NNULLL') AS lag_{name}".format(name=name)
            else:
                pair = "COALESCE({name},'NNULLL') AS {name}, COALESCE(LAG({name}) OVER w,'NNULLL') AS lag_{name}".format(name=name)
            coalesce_pairs.append(pair)

        return ', '.join(coalesce_pairs)

    @property
    def insert_query(self):
        # Convert back from the NULL marker to return actual null values, but skip the first (id) column.
        input_columns = ', '.join(["IF(t.{name} = 'NNULLL', NULL, t.{name}) AS {name}".format(name=name) for name in self.source_column_names[1:]])
        # For intermediate work, convert NULLs to NULL marker.
        intermediate_columns = self.intermediate_columns()
        where_clause = " OR ".join(["t.{name} <> t.lag_{name}".format(name=name) for name in self.source_column_names[1:]])
        query = """
        SELECT id, {input_columns},
            NULL AS history_id, 
            dt AS history_date, 
            NULL AS history_change_reason,
            IF(t.lag_dt = 'NNULLL', '+', '~') AS history_type, 
            NULL AS history_user_id
        FROM (
            SELECT 
                {intermediate_columns},
                COALESCE(dt,'NNULLL') AS dt, COALESCE(LAG(dt) OVER w,'NNULLL') AS lag_dt
            FROM {source_table}
            WINDOW w AS (PARTITION BY id ORDER BY dt)
        ) t
        WHERE {where_clause}
        """.format(
            input_columns=input_columns,
            intermediate_columns=intermediate_columns,
            source_table=self.source_table,
            where_clause=where_clause,
        )
        return query


class LoadHiveHistoryToWarehouse(WarehouseMixin, VerticaCopyTask):
    
    date = luigi.DateParameter()

    source_task = None

    @property
    def source_import_class(self):
        """The class object that loads the source table into Hive."""
        raise NotImplementedError

    @property
    def table(self):
        return self.insert_source_task.table

    @property
    def default_columns(self):
        """List of tuples defining name and definition of automatically-filled columns."""
        return None

    @property
    def auto_primary_key(self):
        """No automatic primary key here."""
        return None

    @property
    def columns(self):
        # convert Hive columns to Vertica columns.
        source_columns = self.insert_source_task.columns
        columns = [(name, 'INT' if type=='INT' else 'VARCHAR(255)') for (name, type) in source_columns]
        return columns

    @property
    def partition(self):
        # TODO: is this supposed to match the source location?  Then get it from there?
        """The table is partitioned by date."""
        # return HivePartition('dt', self.date.isoformat())  # pylint: disable=no-member
        return self.insert_source_task.partition

    @property
    def insert_source_task(self):
        if self.source_task is None:
            kwargs = {
                # 'schema': self.schema,
                # 'marker_schema': self.marker_schema,
                # 'credentials': self.credentials,
                'destination': self.warehouse_path,
                'overwrite': self.overwrite,
                'warehouse_path': self.warehouse_path,
                'import_date': self.date,
            }
            self.source_task = self.source_import_class(**kwargs)
        return self.source_task        


############################################
# Define Loads for individual source classes
############################################


class ProductCatalogClassHiveHistoryTask(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportProductCatalogClass


class LoadProductCatalogClassHiveHistoryToWarehouse(LoadHiveHistoryToWarehouse):    

    @property
    def source_import_class(self):
        return ProductCatalogClassHiveHistoryTask
    

class ProductCatalogHiveHistoryTask(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportProductCatalog


class LoadProductCatalogHiveHistoryToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return ProductCatalogHiveHistoryTask


class StudentCourseEnrollmentHiveHistoryTask(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportStudentCourseEnrollmentTask


class LoadStudentCourseEnrollmentHiveHistoryToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return StudentCourseEnrollmentHiveHistoryTask


class HiveHistoryAuthUserTask(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportAuthUserTask


class LoadHiveHistoryAuthUserTaskToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryAuthUserTask


class HiveHistoryAuthUserProfileTask(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportAuthUserProfileTask

class LoadHiveHistoryAuthUserProfileTaskToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryAuthUserProfileTask


class HiveHistoryCouponVoucherIndirectionState(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportCouponVoucherIndirectionState

class LoadHiveHistoryCouponVoucherIndirectionStateToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryCouponVoucherIndirectionState


class HiveHistoryCouponVoucherState(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportCouponVoucherState

class LoadHiveHistoryCouponVoucherStateToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryCouponVoucherState


class HiveHistoryCourseEntitlementTask(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportCourseEntitlementTask

class LoadHiveHistoryCourseEntitlementTaskToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryCourseEntitlementTask


class HiveHistoryCurrentOrderDiscountState(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportCurrentOrderDiscountState

class LoadHiveHistoryCurrentOrderDiscountStateToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryCurrentOrderDiscountState


class HiveHistoryCurrentOrderLineState(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportCurrentOrderLineState

class LoadHiveHistoryCurrentOrderLineStateToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryCurrentOrderLineState


class HiveHistoryCurrentOrderState(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportCurrentOrderState

class LoadHiveHistoryCurrentOrderStateToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryCurrentOrderState


class HiveHistoryCurrentRefundRefundLineState(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportCurrentRefundRefundLineState

class LoadHiveHistoryCurrentRefundRefundLineStateToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryCurrentRefundRefundLineState


class HiveHistoryEcommercePartner(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportEcommercePartner


class LoadHiveHistoryEcommercePartnerToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryEcommercePartner


class HiveHistoryEcommerceUser (BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportEcommerceUser


class LoadHiveHistoryEcommerceUserToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryEcommerceUser


class HiveHistoryProductCatalogAttributes(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportProductCatalogAttributes


class LoadHiveHistoryProductCatalogAttributesToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryProductCatalogAttributes


class HiveHistoryProductCatalogAttributeValues(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportProductCatalogAttributeValues

class LoadHiveHistoryProductCatalogAttributeValuesToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryProductCatalogAttributeValues


class HiveHistoryShoppingCartCertificateItem(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportShoppingCartCertificateItem

class LoadHiveHistoryShoppingCartCertificateItemToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryShoppingCartCertificateItem


class HiveHistoryShoppingCartCoupon(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportShoppingCartCoupon

class LoadHiveHistoryShoppingCartCouponToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryShoppingCartCoupon


class HiveHistoryShoppingCartCouponRedemption(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportShoppingCartCouponRedemption

class LoadHiveHistoryShoppingCartCouponRedemptionToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryShoppingCartCouponRedemption


class HiveHistoryShoppingCartCourseRegistrationCodeItem(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportShoppingCartCourseRegistrationCodeItem

class LoadHiveHistoryShoppingCartCourseRegistrationCodeItemToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryShoppingCartCourseRegistrationCodeItem


class HiveHistoryShoppingCartDonation(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportShoppingCartDonation

class LoadHiveHistoryShoppingCartDonationToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryShoppingCartDonation


class HiveHistoryShoppingCartOrder(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportShoppingCartOrder

class LoadHiveHistoryShoppingCartOrderToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryShoppingCartOrder


class HiveHistoryShoppingCartOrderItem(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportShoppingCartOrderItem

class LoadHiveHistoryShoppingCartOrderItemToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryShoppingCartOrderItem


class HiveHistoryShoppingCartPaidCourseRegistration(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportShoppingCartPaidCourseRegistration

class LoadHiveHistoryShoppingCartPaidCourseRegistrationToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryShoppingCartPaidCourseRegistration


class HiveHistoryBenefitTask(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportBenefitTask

class LoadHiveHistoryBenefitTaskToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryBenefitTask


class HiveHistoryConditionalOfferTask(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportConditionalOfferTask

class LoadHiveHistoryConditionalOfferTaskToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryConditionalOfferTask


class HiveHistoryDataSharingConsentTask(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportDataSharingConsentTask

class LoadHiveHistoryDataSharingConsentTaskToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryDataSharingConsentTask


class HiveHistoryEnterpriseCourseEnrollmentUserTask(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportEnterpriseCourseEnrollmentUserTask

class LoadHiveHistoryEnterpriseCourseEnrollmentUserTaskToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryEnterpriseCourseEnrollmentUserTask


class HiveHistoryEnterpriseCustomerTask(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportEnterpriseCustomerTask

class LoadHiveHistoryEnterpriseCustomerTaskToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryEnterpriseCustomerTask


class HiveHistoryEnterpriseCustomerUserTask(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportEnterpriseCustomerUserTask

class LoadHiveHistoryEnterpriseCustomerUserTaskToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryEnterpriseCustomerUserTask


class HiveHistoryStockRecordTask(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportStockRecordTask

class LoadHiveHistoryStockRecordTaskToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryStockRecordTask


class HiveHistoryUserSocialAuthTask(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportUserSocialAuthTask

class LoadHiveHistoryUserSocialAuthTaskToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryUserSocialAuthTask


class HiveHistoryVoucherTask(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportVoucherTask

class LoadHiveHistoryVoucherTaskToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return HiveHistoryVoucherTask


class LoadHistoryWorkflow(WarehouseMixin, luigi.WrapperTask):

    date = luigi.DateParameter()

    # We are not using VerticaCopyTaskMixin as OverwriteOutputMixin changes the complete() method behavior.
    schema = luigi.Parameter(
        config_path={'section': 'vertica-export', 'name': 'schema'},
        description='The schema to which to write.',
    )
    credentials = luigi.Parameter(
        config_path={'section': 'vertica-export', 'name': 'credentials'},
        description='Path to the external access credentials file.',
    )

    """
    Provides entry point for loading data into warehouse.
    """

    def requires(self):
        kwargs = {
            'date': self.date,
            'schema': self.schema,
            'credentials': self.credentials,
            'warehouse_path': self.warehouse_path,
        }
        yield (
            LoadHiveHistoryAuthUserTaskToWarehouse(**kwargs),
            LoadHiveHistoryAuthUserProfileTaskToWarehouse(**kwargs),
            LoadHiveHistoryCouponVoucherIndirectionStateToWarehouse(**kwargs),
            LoadHiveHistoryCouponVoucherStateToWarehouse(**kwargs),
            LoadHiveHistoryCourseEntitlementTaskToWarehouse(**kwargs),
            LoadHiveHistoryCurrentOrderDiscountStateToWarehouse(**kwargs),
            LoadHiveHistoryCurrentOrderLineStateToWarehouse(**kwargs),
            LoadHiveHistoryCurrentOrderStateToWarehouse(**kwargs),
            LoadHiveHistoryCurrentRefundRefundLineStateToWarehouse(**kwargs),
            LoadHiveHistoryEcommercePartnerToWarehouse(**kwargs),
            LoadHiveHistoryEcommerceUserToWarehouse(**kwargs),
            LoadHiveHistoryProductCatalogAttributesToWarehouse(**kwargs),
            LoadHiveHistoryProductCatalogAttributeValuesToWarehouse(**kwargs),
            LoadHiveHistoryShoppingCartCertificateItemToWarehouse(**kwargs),
            LoadHiveHistoryShoppingCartCouponToWarehouse(**kwargs),
            LoadHiveHistoryShoppingCartCouponRedemptionToWarehouse(**kwargs),
            LoadHiveHistoryShoppingCartCourseRegistrationCodeItemToWarehouse(**kwargs),
            LoadHiveHistoryShoppingCartDonationToWarehouse(**kwargs),
            LoadHiveHistoryShoppingCartOrderToWarehouse(**kwargs),
            LoadHiveHistoryShoppingCartOrderItemToWarehouse(**kwargs),
            LoadHiveHistoryShoppingCartPaidCourseRegistrationToWarehouse(**kwargs),
            LoadHiveHistoryBenefitTaskToWarehouse(**kwargs),
            LoadHiveHistoryConditionalOfferTaskToWarehouse(**kwargs),
            LoadHiveHistoryDataSharingConsentTaskToWarehouse(**kwargs),
            LoadHiveHistoryEnterpriseCourseEnrollmentUserTaskToWarehouse(**kwargs),
            LoadHiveHistoryEnterpriseCustomerTaskToWarehouse(**kwargs),
            LoadHiveHistoryEnterpriseCustomerUserTaskToWarehouse(**kwargs),
            LoadHiveHistoryStockRecordTaskToWarehouse(**kwargs),
            LoadHiveHistoryUserSocialAuthTaskToWarehouse(**kwargs),
            LoadHiveHistoryVoucherTaskToWarehouse(**kwargs),
        )

    def output(self):
        return [task.output() for task in self.requires()]
