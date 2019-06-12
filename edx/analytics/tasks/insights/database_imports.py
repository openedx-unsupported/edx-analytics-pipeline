"""
Import data from external RDBMS databases into Hive.
"""
import datetime
import logging
import textwrap

import luigi
from luigi.contrib.hive import HivePartitionTarget, HiveQueryTask

from edx.analytics.tasks.common.sqoop import SqoopImportFromMysql, SqoopImportMixin
from edx.analytics.tasks.util.hive import hive_database_name, hive_decimal_type
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.url import url_path_join

log = logging.getLogger(__name__)


class DatabaseImportMixin(SqoopImportMixin):
    """Provides parameters for accessing RDBMS databases and determining date to assign to Hive partition."""

    import_date = luigi.DateParameter(
        default=None,
        description='Date to assign to Hive partition.  Default is today\'s date, UTC.',
    )

    def __init__(self, *args, **kwargs):
        super(DatabaseImportMixin, self).__init__(*args, **kwargs)

        if not self.import_date:
            self.import_date = datetime.datetime.utcnow().date()


class ImportIntoHiveTableTask(OverwriteOutputMixin, HiveQueryTask):
    """
    Abstract class to import data into a Hive table.

    Requires four properties and a requires() method to be defined.
    """

    def query(self):
        # TODO: Figure out how to clean up old data. This just cleans
        # out old metastore info, and doesn't actually remove the table
        # data.

        # Ensure there is exactly one available partition in the
        # table. Don't keep historical partitions since we don't want
        # to commit to taking snapshots at any regular interval. They
        # will happen when/if they need to happen.  Table snapshots
        # should *not* be used for analyzing trends, instead we should
        # rely on events or database tables that keep historical
        # information.
        query_format = textwrap.dedent("""
            USE {database_name};
            DROP TABLE IF EXISTS `{table_name}`;
            CREATE EXTERNAL TABLE `{table_name}` (
                {col_spec}
            )
            PARTITIONED BY (dt STRING)
            {table_format}
            LOCATION '{location}';
            ALTER TABLE `{table_name}` ADD PARTITION (dt = '{partition_date}');
        """)

        query = query_format.format(
            database_name=hive_database_name(),
            table_name=self.table_name,
            col_spec=','.join(['`{}` {}'.format(name, col_type) for name, col_type in self.columns]),
            location=self.table_location,
            table_format=self.table_format,
            partition_date=self.partition_date,
        )

        log.debug('Executing hive query: %s', query)

        # Mark the output as having been removed, even though
        # that doesn't technically happen until the query has been
        # executed (and in particular that the 'DROP TABLE' is executed).
        log.info("Marking existing output as having been removed for task %s", str(self))
        self.attempted_removal = True

        return query

    @property
    def partition(self):
        """Provides name of Hive database table partition."""
        # The Luigi hive code expects partitions to be defined by dictionaries.
        return {'dt': self.partition_date}

    @property
    def partition_location(self):
        """Provides location of Hive database table's partition data."""
        # The actual folder name where the data is stored is expected to be in the format <key>=<value>
        partition_name = '='.join(self.partition.items()[0])
        # Make sure that input path ends with a slash, to indicate a directory.
        # (This is necessary for S3 paths that are output from Hadoop jobs.)
        return url_path_join(self.table_location, partition_name + '/')

    @property
    def table_name(self):
        """Provides name of Hive database table."""
        raise NotImplementedError

    @property
    def table_format(self):
        """Provides format of Hive database table's data."""
        raise NotImplementedError

    @property
    def table_location(self):
        """Provides root location of Hive database table's data."""
        raise NotImplementedError

    @property
    def partition_date(self):
        """Provides value to use in constructing the partition name of Hive database table."""
        raise NotImplementedError

    @property
    def columns(self):
        """
        Provides definition of columns in Hive.

        This should define a list of (name, definition) tuples, where
        the definition defines the Hive type to use. For example,
        ('first_name', 'STRING').

        """
        raise NotImplementedError

    def output(self):
        return HivePartitionTarget(
            self.table_name, self.partition, database=hive_database_name(), fail_missing_table=False
        )


class ImportMysqlToHiveTableTask(DatabaseImportMixin, ImportIntoHiveTableTask):
    """
    Dumps data from an RDBMS table, and imports into Hive.

    Requires override of `table_name` and `columns` properties.
    """

    @property
    def table_location(self):
        return url_path_join(self.destination, self.table_name)

    @property
    def table_format(self):
        # Use default of hive built-in format.
        return ""

    @property
    def partition_date(self):
        # Partition date is provided by DatabaseImportMixin.
        return self.import_date.isoformat()

    def requires(self):
        return SqoopImportFromMysql(
            table_name=self.table_name,
            # TODO: We may want to make the explicit passing in of columns optional as it prevents a direct transfer.
            # Make sure delimiters and nulls etc. still work after removal.
            columns=[c[0] for c in self.columns],
            destination=self.partition_location,
            credentials=self.credentials,
            num_mappers=self.num_mappers,
            verbose=self.verbose,
            overwrite=self.overwrite,
            database=self.database,
            # Hive expects NULL to be represented by the string "\N" in the data. You have to pass in "\\N" to sqoop
            # since it uses that string directly in the generated Java code, so "\\N" actually looks like "\N" to the
            # Java code. In order to get "\\N" onto the command line we have to use another set of escapes to tell the
            # python code to pass through the "\" character.
            null_string='\\\\N',
            # It's unclear why, but this setting prevents us from correctly substituting nulls with \N.
            mysql_delimiters=False,
            # This is a string that is interpreted as an octal number, so it is equivalent to the character Ctrl-A
            # (0x01). This is the default separator for fields in Hive.
            fields_terminated_by='\x01',
            # Replace delimiters with a single space if they appear in the data. This prevents the import of malformed
            # records. Hive does not support escape characters or other reasonable workarounds to this problem.
            delimiter_replacement=' ',
            where=self.where,
        )


class ImportStudentCourseEnrollmentTask(ImportMysqlToHiveTableTask):
    """Imports course enrollment information from an external LMS DB to a destination directory."""

    @property
    def table_name(self):
        return 'student_courseenrollment'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('user_id', 'INT'),
            ('course_id', 'STRING'),
            ('created', 'TIMESTAMP'),
            ('is_active', 'BOOLEAN'),
            ('mode', 'STRING'),
        ]


class ImportCourseEntitlementTask(ImportMysqlToHiveTableTask):
    """ Imports the table containing learners' course entitlements. """

    @property
    def table_name(self):
        return 'entitlements_courseentitlement'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('uuid', 'STRING'),
            ('course_uuid', 'STRING'),
            ('user_id', 'INT'),
            ('enrollment_course_run_id', 'INT'),
            ('order_number', 'STRING'),
            ('expired_at', 'TIMESTAMP'),
            ('created', 'TIMESTAMP'),
            ('modified', 'TIMESTAMP'),
        ]


class ImportAuthUserTask(ImportMysqlToHiveTableTask):
    """Imports user information from an external LMS DB to a destination directory."""

    @property
    def table_name(self):
        return 'auth_user'

    @property
    def columns(self):
        # Fields not included are 'password', 'first_name' and 'last_name'.
        # In our LMS, the latter two are always empty.
        return [
            ('id', 'INT'),
            ('username', 'STRING'),
            ('last_login', 'TIMESTAMP'),
            ('date_joined', 'TIMESTAMP'),
            ('is_active', 'BOOLEAN'),
            ('is_superuser', 'BOOLEAN'),
            ('is_staff', 'BOOLEAN'),
            ('email', 'STRING'),
        ]


class ImportAuthUserProfileTask(ImportMysqlToHiveTableTask):
    """
    Imports user demographic information from an external LMS DB to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'auth_userprofile'

    @property
    def columns(self):
        return [
            ('user_id', 'INT'),
            ('name', 'STRING'),
            ('gender', 'STRING'),
            ('year_of_birth', 'INT'),
            ('level_of_education', 'STRING'),
            ('language', 'STRING'),
            ('location', 'STRING'),
            ('mailing_address', 'STRING'),
            ('city', 'STRING'),
            ('country', 'STRING'),
            ('goals', 'STRING'),
        ]


class ImportCourseUserGroupTask(ImportMysqlToHiveTableTask):
    """
    Imports course cohort information from an external LMS DB to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'course_groups_courseusergroup'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('name', 'STRING'),
            ('course_id', 'STRING'),
            ('group_type', 'STRING'),
        ]


class ImportCourseUserGroupUsersTask(ImportMysqlToHiveTableTask):
    """
    Imports user cohort information from an external LMS DB to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'course_groups_courseusergroup_users'

    @property
    def columns(self):
        return [
            ('courseusergroup_id', 'INT'),
            ('user_id', 'INT'),
        ]


class ImportShoppingCartOrder(ImportMysqlToHiveTableTask):
    """
    Imports orders from an external LMS DB shopping cart table to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'shoppingcart_order'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('user_id', 'INT'),
            ('currency', 'STRING'),
            ('status', 'STRING'),
            ('purchase_time', 'TIMESTAMP'),
            ('bill_to_first', 'STRING'),
            ('bill_to_last', 'STRING'),
            ('bill_to_street1', 'STRING'),
            ('bill_to_street2', 'STRING'),
            ('bill_to_city', 'STRING'),
            ('bill_to_state', 'STRING'),
            ('bill_to_postalcode', 'STRING'),
            ('bill_to_country', 'STRING'),
            ('bill_to_ccnum', 'STRING'),
            ('bill_to_cardtype', 'STRING'),
            ('processor_reply_dump', 'STRING'),
            ('refunded_time', 'STRING'),
            ('company_name', 'STRING'),
            ('company_contact_name', 'STRING'),
            ('company_contact_email', 'STRING'),
            ('recipient_name', 'STRING'),
            ('recipient_email', 'STRING'),
            ('customer_reference_number', 'STRING'),
            ('order_type', 'STRING'),
        ]


class ImportShoppingCartOrderItem(ImportMysqlToHiveTableTask):
    """
    Imports individual order items from an external LMS DB shopping cart table to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'shoppingcart_orderitem'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('order_id', 'INT'),
            ('user_id', 'INT'),
            ('status', 'STRING'),
            ('qty', 'int'),
            ('unit_cost', hive_decimal_type(12, 2)),
            ('line_desc', 'STRING'),
            ('currency', 'STRING'),
            ('fulfilled_time', 'TIMESTAMP'),
            ('report_comments', 'STRING'),
            ('refund_requested_time', 'TIMESTAMP'),
            ('service_fee', hive_decimal_type(12, 2)),
            ('list_price', hive_decimal_type(12, 2)),
            ('created', 'TIMESTAMP'),
            ('modified', 'TIMESTAMP'),
        ]


class ImportShoppingCartCertificateItem(ImportMysqlToHiveTableTask):
    """
    Imports certificate items from an external LMS DB shopping cart table to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'shoppingcart_certificateitem'

    @property
    def columns(self):
        return [
            ('orderitem_ptr_id', 'INT'),
            ('course_id', 'STRING'),
            ('course_enrollment_id', 'INT'),
            ('mode', 'STRING'),
        ]


class ImportShoppingCartPaidCourseRegistration(ImportMysqlToHiveTableTask):
    """
    Imports paid course registrations from an external LMS DB shopping cart table to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'shoppingcart_paidcourseregistration'

    @property
    def columns(self):
        return [
            ('orderitem_ptr_id', 'INT'),
            ('course_id', 'STRING'),
            ('mode', 'STRING'),
            ('course_enrollment_id', 'INT'),
        ]


class ImportShoppingCartDonation(ImportMysqlToHiveTableTask):
    """
    Imports donations from an external LMS DB shopping cart table to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'shoppingcart_donation'

    @property
    def columns(self):
        return [
            ('orderitem_ptr_id', 'INT'),
            ('donation_type', 'STRING'),
            ('course_id', 'STRING'),
        ]


class ImportShoppingCartCourseRegistrationCodeItem(ImportMysqlToHiveTableTask):
    """
    Imports course registration codes from an external ecommerce table to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'shoppingcart_courseregcodeitem'

    @property
    def columns(self):
        return [
            ('orderitem_ptr_id', 'INT'),
            ('course_id', 'STRING'),
            ('mode', 'STRING'),
        ]


class ImportShoppingCartCoupon(ImportMysqlToHiveTableTask):
    """
    Imports coupon definitions from an external LMS DB shopping cart table to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'shoppingcart_coupon'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('code', 'STRING'),
            ('description', 'STRING'),
            ('course_id', 'STRING'),
            ('percentage_discount', 'INT'),
            ('created_at', 'TIMESTAMP'),
            ('is_active', 'BOOLEAN'),
            ('expiration_date', 'TIMESTAMP'),
            ('created_by_id', 'INT'),
        ]


class ImportShoppingCartCouponRedemption(ImportMysqlToHiveTableTask):
    """
    Imports coupon redeptions from an external LMS DB shopping cart table to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'shoppingcart_couponredemption'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('coupon_id', 'INT'),
            ('order_id', 'INT'),
            ('user_id', 'INT'),
        ]


class ImportEcommerceUser(ImportMysqlToHiveTableTask):
    """Ecommerce: Users: Imports users from an external ecommerce table to a destination dir."""

    @property
    def table_name(self):
        return 'ecommerce_user'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('username', 'STRING'),
            ('email', 'STRING'),
            ('lms_user_id', 'INT'),
        ]


class ImportProductCatalog(ImportMysqlToHiveTableTask):
    """
    Ecommerce: Products: Imports product catalog from an external ecommerce table to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'catalogue_product'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('structure', 'STRING'),
            ('upc', 'STRING'),
            ('title', 'STRING'),
            ('slug', 'STRING'),
            ('description', 'STRING'),
            ('rating', 'STRING'),
            ('date_created', 'TIMESTAMP'),
            ('date_updated', 'TIMESTAMP'),
            ('is_discountable', 'STRING'),
            ('parent_id', 'INT'),
            ('product_class_id', 'INT'),
            ('course_id', 'STRING'),
            ('expires', 'TIMESTAMP'),
        ]


class ImportProductCatalogClass(ImportMysqlToHiveTableTask):
    """Ecommerce: Products: Imports product catalog classes from an external ecommerce table to a destination dir."""

    @property
    def table_name(self):
        return 'catalogue_productclass'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('name', 'STRING'),
            ('slug', 'STRING'),
            ('requires_shipping', 'TINYINT'),
            ('track_stock', 'TINYINT'),
        ]


class ImportProductCatalogAttributes(ImportMysqlToHiveTableTask):
    """
    Ecommerce: Products: Imports product catalog attributes from an external ecommerce table to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'catalogue_productattribute'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('name', 'STRING'),
            ('code', 'STRING'),
            ('type', 'STRING'),
            ('required', 'INT'),
            ('option_group_id', 'INT'),
            ('product_class_id', 'INT'),
        ]


class ImportProductCatalogAttributeValues(ImportMysqlToHiveTableTask):
    """
    Ecommerce: Products: Imports product catalog attribute values from an external ecommerce table to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'catalogue_productattributevalue'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('value_text', 'STRING'),
            ('value_integer', 'INT'),
            ('value_boolean', 'BOOLEAN'),
            ('value_float', 'STRING'),
            ('value_richtext', 'STRING'),
            ('value_date', 'TIMESTAMP'),
            ('value_file', 'STRING'),
            ('value_image', 'STRING'),
            ('entity_object_id', 'INT'),
            ('attribute_id', 'INT'),
            ('entity_content_type_id', 'INT'),
            ('product_id', 'INT'),
            ('value_option_id', 'INT'),
        ]


class ImportCurrentRefundRefundLineState(ImportMysqlToHiveTableTask):
    """
    Ecommerce: Current: Imports current refund line items from an ecommerce table to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'refund_refundline'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('line_credit_excl_tax', hive_decimal_type(12, 2)),
            ('quantity', 'INT'),
            ('status', 'STRING'),
            ('order_line_id', 'INT'),
            ('refund_id', 'INT'),
            ('created', 'TIMESTAMP'),
            ('modified', 'TIMESTAMP'),
        ]


class ImportCurrentOrderState(ImportMysqlToHiveTableTask):
    """
    Ecommerce Current: Imports current orders from an ecommerce table to both a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'order_order'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('number', 'STRING'),
            ('currency', 'STRING'),
            ('total_incl_tax', hive_decimal_type(12, 2)),
            ('total_excl_tax', hive_decimal_type(12, 2)),
            ('shipping_incl_tax', hive_decimal_type(12, 2)),
            ('shipping_excl_tax', hive_decimal_type(12, 2)),
            ('shipping_method', 'STRING'),
            ('shipping_code', 'STRING'),
            ('status', 'STRING'),
            ('guest_email', 'STRING'),
            ('date_placed', 'TIMESTAMP'),
            ('basket_id', 'INT'),
            ('billing_address_id', 'INT'),
            ('shipping_address_id', 'INT'),
            ('site_id', 'INT'),
            ('user_id', 'INT'),
        ]


class ImportCurrentOrderLineState(ImportMysqlToHiveTableTask):
    """
    Ecommerce: Current: Imports current order line items from an ecommerce table to a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'order_line'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('partner_name', 'STRING'),
            ('partner_sku', 'STRING'),
            ('partner_line_reference', 'STRING'),
            ('partner_line_notes', 'STRING'),
            ('title', 'STRING'),
            ('upc', 'STRING'),
            ('quantity', 'INT'),
            ('line_price_incl_tax', hive_decimal_type(12, 2)),
            ('line_price_excl_tax', hive_decimal_type(12, 2)),
            ('line_price_before_discounts_incl_tax', hive_decimal_type(12, 2)),
            ('line_price_before_discounts_excl_tax', hive_decimal_type(12, 2)),
            ('unit_cost_price', hive_decimal_type(12, 2)),
            ('unit_price_incl_tax', hive_decimal_type(12, 2)),
            ('unit_price_excl_tax', hive_decimal_type(12, 2)),
            ('unit_retail_price', hive_decimal_type(12, 2)),
            ('status', 'STRING'),
            ('est_dispatch_date', 'TIMESTAMP'),
            ('order_id', 'INT'),
            ('partner_id', 'INT'),
            ('product_id', 'INT'),
            ('stockrecord_id', 'INT'),
        ]


class ImportCurrentOrderDiscountState(ImportMysqlToHiveTableTask):
    """
    Ecommerce: Current: Imports current order discount records from an ecommerce table to a
    destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'order_orderdiscount'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('category', 'STRING'),
            ('offer_id', 'INT'),
            ('offer_name', 'STRING'),
            ('voucher_id', 'INT'),
            ('voucher_code', 'STRING'),
            ('frequency', 'INT'),
            ('amount', hive_decimal_type(12, 2)),
            ('message', 'STRING'),
            ('order_id', 'INT'),
        ]


class ImportCouponVoucherIndirectionState(ImportMysqlToHiveTableTask):
    """
    Ecommerce: Current: Imports the voucher_couponvouchers table from the ecommerce database to
    a destination directory and a HIVE metastore.

    This table is just an extra layer of indirection in the source schema design and is required
    to translate a 'couponvouchers_id' into a coupon id.
    Coupons are represented as products in the product table, which is imported separately.
    A coupon can have many voucher codes associated with it.
    """
    @property
    def table_name(self):
        return 'voucher_couponvouchers'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('coupon_id', 'INT'),
        ]


class ImportCouponVoucherState(ImportMysqlToHiveTableTask):
    """
    Ecommerce: Current: Imports the voucher_couponvouchers_vouchers table from the ecommerce
    database to a destination directory and a HIVE metastore.

    A coupon can have many voucher codes associated with it. This table associates voucher IDs
    with 'couponvouchers_id's, which are stored in the voucher_couponvouchers table and
    have a 1:1 relationship to coupon IDs.
    """
    @property
    def table_name(self):
        return 'voucher_couponvouchers_vouchers'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('couponvouchers_id', 'INT'),
            ('voucher_id', 'INT'),
        ]


class ImportEcommercePartner(ImportMysqlToHiveTableTask):
    """
    Ecommerce: Current: Imports Partner information from an ecommerce table to a
    destination directory and a HIVE metastore.
    """

    @property
    def table_name(self):
        return 'partner_partner'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('code', 'STRING'),
            ('name', 'STRING'),
            ('short_code', 'STRING'),
        ]


class ImportCourseModeTask(ImportMysqlToHiveTableTask):
    """
    Course Information: Imports course_modes table to both a destination directory and a HIVE metastore.

    """
    @property
    def table_name(self):
        return 'course_modes_coursemode'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('course_id', 'STRING'),
            ('mode_slug', 'STRING'),
            ('mode_display_name', 'STRING'),
            ('min_price', 'INT'),
            ('suggested_prices', 'STRING'),
            ('currency', 'STRING'),
            ('expiration_date', 'TIMESTAMP'),
            ('expiration_datetime', 'TIMESTAMP'),
            ('description', 'STRING'),
            ('sku', 'STRING'),
        ]


class ImportGeneratedCertificatesTask(ImportMysqlToHiveTableTask):

    @property
    def table_name(self):
        return 'certificates_generatedcertificate'

    @property
    def columns(self):
        return [
            ('user_id', 'INT'),
            ('course_id', 'STRING'),
            ('grade', 'STRING'),
            ('status', 'STRING'),
            ('mode', 'STRING'),
            ('created_date', 'TIMESTAMP'),
            ('modified_date', 'TIMESTAMP'),
        ]


class ImportPersistentCourseGradeTask(ImportMysqlToHiveTableTask):
    """Imports the `grades_persistentcoursegrade` table to S3/Hive."""

    @property
    def table_name(self):
        return 'grades_persistentcoursegrade'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('user_id', 'INT'),
            ('course_id', 'STRING'),
            ('course_edited_timestamp', 'TIMESTAMP'),
            ('course_version', 'STRING'),
            ('grading_policy_hash', 'STRING'),
            ('percent_grade', 'DECIMAL(10,2)'),
            ('letter_grade', 'STRING'),
            ('passed_timestamp', 'TIMESTAMP'),
            ('created', 'TIMESTAMP'),
            ('modified', 'TIMESTAMP'),
        ]


class ImportAllDatabaseTablesTask(DatabaseImportMixin, OverwriteOutputMixin, luigi.WrapperTask):
    """Imports a set of database tables from an external LMS RDBMS."""

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
            ImportStudentCourseEnrollmentTask(**kwargs),
            ImportAuthUserTask(**kwargs),
            ImportAuthUserProfileTask(**kwargs),
        )

    def output(self):
        return [task.output() for task in self.requires()]
