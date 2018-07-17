import datetime
import logging

import luigi
import luigi.task
from edx.analytics.tasks.common.mysql_load import MysqlInsertTask
from edx.analytics.tasks.common.spark import SparkExportFromMysqlTaskMixin, SparkJobTask, SparkMysqlImportMixin, \
    ImportAuthUserSparkTask, ImportAuthUserProfileSparkTask, SparkMixin
from edx.analytics.tasks.enterprise.enterprise_database_imports import (
    ImportBenefitTask, ImportConditionalOfferTask, ImportDataSharingConsentTask,
    ImportEnterpriseCourseEnrollmentUserTask, ImportEnterpriseCustomerTask, ImportEnterpriseCustomerUserTask,
    ImportStockRecordTask, ImportUserSocialAuthTask, ImportVoucherTask
)
from edx.analytics.tasks.enterprise.enterprise_enrollments import EnterpriseEnrollmentRecord
from edx.analytics.tasks.insights.database_imports import (
    ImportAuthUserProfileTask, ImportAuthUserTask, ImportCurrentOrderDiscountState, ImportCurrentOrderLineState,
    ImportCurrentOrderState, ImportEcommerceUser, ImportPersistentCourseGradeTask, ImportProductCatalog,
    ImportStudentCourseEnrollmentTask
)
from edx.analytics.tasks.insights.enrollments import OverwriteHiveAndMysqlDownstreamMixin
from edx.analytics.tasks.insights.user_activity import UserActivityTableTask
from edx.analytics.tasks.util.decorators import workflow_entry_point
from edx.analytics.tasks.util.hive import (
    BareHiveTableTask, HivePartitionTask, OverwriteAwareHiveQueryDataTask, WarehouseMixin
)
from edx.analytics.tasks.util.record import (
    BooleanField, DateField, DateTimeField, FloatField, IntegerField, Record, StringField
)
from edx.analytics.tasks.util.url import get_target_from_url, url_path_join
from edx.analytics.tasks.warehouse.load_internal_reporting_course_catalog import (
    CoursePartitionTask, LoadInternalReportingCourseCatalogMixin
)


class EnterpriseEnrollmentSparkMysqlTask(SparkMysqlImportMixin, LoadInternalReportingCourseCatalogMixin, SparkJobTask):
    """
    Executes a spark sql query to gather enterprise enrollment data and store it in the enterprise_enrollment mysql table.
    All enrollments of enterprise users enrolled in courses associated with their enterprise.
    """

    otto_credentials = luigi.Parameter(
        config_path={'section': 'otto-database-import', 'name': 'credentials'}
    )
    otto_database = luigi.Parameter(
        config_path={'section': 'otto-database-import', 'name': 'database'}
    )

    def requires(self):
        kwargs = {
            'destination': self.warehouse_path
        }
        spark_kwargs = {
            'destination': self.warehouse_path,
            'driver_memory': self.driver_memory,
            'executor_memory': self.executor_memory,
            'executor_cores': self.executor_cores,
            'spark_conf': self.spark_conf
        }

        for requirement in super(EnterpriseEnrollmentSparkMysqlTask, self).requires():
            yield requirement

        # the process that generates the source table used by this query
        yield (
            ImportAuthUserSparkTask(**spark_kwargs),
            ImportAuthUserProfileSparkTask(**spark_kwargs),
            ImportEnterpriseCustomerTask(**kwargs),
            ImportEnterpriseCustomerUserTask(**kwargs),
            ImportEnterpriseCourseEnrollmentUserTask(**kwargs),
            ImportDataSharingConsentTask(**kwargs),
            ImportUserSocialAuthTask(**kwargs),
            ImportStudentCourseEnrollmentTask(**kwargs),
            ImportPersistentCourseGradeTask(**kwargs),
            CoursePartitionTask(
                date=self.date,
                warehouse_path=self.warehouse_path,
                api_root_url=self.api_root_url,
                api_page_size=self.api_page_size,
            ),
            UserActivityTableTask(
                warehouse_path=self.warehouse_path,
                overwrite_n_days=0,
                date=self.date
            )
        )

        kwargs = {
            'credentials': self.otto_credentials,
            'database': self.otto_database,
            'destination': self.warehouse_path
        }
        yield (
            ImportProductCatalog(**kwargs),
            ImportCurrentOrderLineState(**kwargs),
            ImportCurrentOrderDiscountState(**kwargs),
            ImportVoucherTask(**kwargs),
            ImportStockRecordTask(**kwargs),
            ImportCurrentOrderState(**kwargs),
            ImportEcommerceUser(**kwargs),
            ImportConditionalOfferTask(**kwargs),
            ImportBenefitTask(**kwargs),
        )

    @property
    def columns(self):
        return EnterpriseEnrollmentRecord.get_sql_schema()

    @property
    def indexes(self):
        return [
            ('enterprise_id',),
        ]

    @property
    def table(self):  # pragma: no cover
        return 'enterprise_enrollment_spark'

    def get_dataframe_schema(self, cols_list):
        """
        Builds a dataframe schema for a given list of columns.
        """
        from pyspark.sql.types import StructType, StringType
        schema = StructType()
        for column in cols_list:
            schema = schema.add(column, StringType(), True)  # for now, using StringTypes for all data types
        return schema

    def get_dataframe_by_source(self, source_path=None, schema=None, column_delimiter='\x01'):
        """
        Creates spark dataframe for a given source and schema using specified column delimiter.

        Returns spark dataframe with source_path as input source and given schema.
        """
        if not source_path or not schema:
            return None
        dataframe = self._spark.read \
            .format('csv') \
            .schema(schema) \
            .options(delimiter=column_delimiter) \
            .load(source_path)
        return dataframe

    def get_insert_sql_query(self):
        return """
            SELECT DISTINCT enterprise_customer.uuid AS enterprise_id,
                    enterprise_customer.name AS enterprise_name,
                    enterprise_user.user_id AS lms_user_id,
                    enterprise_user.id AS enterprise_user_id,
                    enterprise_course_enrollment.course_id,
                    enterprise_course_enrollment.created AS enrollment_created_timestamp,
                    enrollment.mode AS user_current_enrollment_mode,
                    CASE consent.granted
                        WHEN 'true' then 1
                        ELSE 0
                    END AS consent_granted,
                    grades.letter_grade,
                    CASE
                        WHEN grades.passed_timestamp IS NULL THEN 0
                        ELSE 1
                    END AS has_passed,
                    grades.passed_timestamp,
                    SUBSTRING_INDEX(social_auth.uid_full, ':', -1) AS enterprise_sso_uid,
                    enterprise_customer.site_id AS enterprise_site_id,
                    course.catalog_course_title AS course_title,
                    course.start_time AS course_start,
                    course.end_time AS course_end,
                    course.pacing_type AS course_pacing_type,
                    CASE
                        WHEN course.pacing_type = 'self_paced' THEN 'Self Paced'
                        ELSE CAST(CEIL(DATEDIFF(course.end_time, course.start_time) / 7) AS STRING)
                    END AS course_duration_weeks,
                    CASE
                        WHEN course.min_effort = '' THEN 0
                        ELSE course.min_effort
                    END AS course_min_effort,
                    course.max_effort AS course_max_effort,
                    auth_user.date_joined AS user_account_creation_timestamp,
                    auth_user.email AS user_email,
                    auth_user.username AS user_username,
                    course.catalog_course AS course_key,
                    user_profile.country AS user_country_code,
                    user_activity.latest_date AS last_activity_date,
                    ecommerce_data.coupon_name AS coupon_name,
                    ecommerce_data.coupon_code AS coupon_code,
                    ecommerce_data.offer AS offer,
                    grades.percent_grade AS current_grade,
                    ecommerce_data.course_price AS course_price,
                    ecommerce_data.discount_price AS discount_price
            FROM enterprise_enterprisecourseenrollment enterprise_course_enrollment
            JOIN enterprise_enterprisecustomeruser enterprise_user
                    ON enterprise_course_enrollment.enterprise_customer_user_id = enterprise_user.id
            JOIN enterprise_enterprisecustomer enterprise_customer
                    ON enterprise_user.enterprise_customer_id = enterprise_customer.uuid
            JOIN student_courseenrollment enrollment
                    ON enterprise_course_enrollment.course_id = enrollment.course_id
                    AND enterprise_user.user_id = enrollment.user_id
            JOIN auth_user auth_user
                    ON enterprise_user.user_id = auth_user.id
            JOIN auth_userprofile user_profile
                    ON enterprise_user.user_id = user_profile.user_id
            LEFT JOIN (
                    SELECT
                        user_id,
                        course_id,
                        MAX(`date`) AS latest_date
                    FROM
                        user_activity_by_user
                    GROUP BY
                        user_id,
                        course_id
                ) user_activity
                    ON enterprise_course_enrollment.course_id = user_activity.course_id
                    AND enterprise_user.user_id = user_activity.user_id
            LEFT JOIN consent_datasharingconsent consent
                    ON auth_user.username =  consent.username
                    AND enterprise_course_enrollment.course_id = consent.course_id
            LEFT JOIN grades_persistentcoursegrade grades
                    ON enterprise_user.user_id = grades.user_id
                    AND enterprise_course_enrollment.course_id = grades.course_id
            LEFT JOIN (
                    SELECT
                        user_id,
                        provider,
                        MAX(uid) AS uid_full
                    FROM
                        social_auth_usersocialauth
                    WHERE
                        provider = 'tpa-saml'
                    GROUP BY
                        user_id, provider
                ) social_auth
                    ON enterprise_user.user_id = social_auth.user_id
            JOIN course_catalog course
                    ON enterprise_course_enrollment.course_id = course.course_id

            -- The subquery below joins across the tables in ecommerce to get the orders that were created.
            -- It also pulls in any coupons or offers that were used to provide a discount to the user.
            -- Finally, it filters out audit orders in the case that a user enrolls and later upgrades to a paid track,
            -- by choosing the order with the product that has the maximum price for the same course_id.
            LEFT JOIN (
                    SELECT
                        ecommerce_user.username AS username,
                        ecommerce_catalogue_product.course_id AS course_id,
                        ecommerce_stockrecord.price_excl_tax AS course_price,
                        ecommerce_order.total_incl_tax AS discount_price,
                        CASE
                            WHEN ecommerce_offer.id IS NULL THEN NULL
                            WHEN ecommerce_offer.offer_type = 'Voucher' THEN NULL
                            ELSE CASE
                                WHEN ecommerce_benefit.proxy_class IS NULL THEN CONCAT(
                                  ecommerce_benefit.type, ', ', ecommerce_benefit.value, ' (#', ecommerce_offer.id, ')'
                                )
                                WHEN ecommerce_benefit.proxy_class LIKE '%Percentage%' THEN CONCAT(
                                  'Percentage, ', ecommerce_benefit.value, ' (#', ecommerce_offer.id, ')'
                                )
                                ELSE CONCAT('Absolute, ', ecommerce_benefit.value, ' (#', ecommerce_offer.id, ')')
                            END
                        END AS offer,
                        ecommerce_voucher.name AS coupon_name,
                        ecommerce_voucher.code AS coupon_code
                    FROM order_order ecommerce_order
                    JOIN ecommerce_user ecommerce_user
                        ON ecommerce_user.id = ecommerce_order.user_id
                    JOIN order_line ecommerce_order_line
                        ON ecommerce_order_line.order_id = ecommerce_order.id
                    JOIN catalogue_product ecommerce_catalogue_product
                        ON ecommerce_catalogue_product.id = ecommerce_order_line.product_id
                    JOIN partner_stockrecord ecommerce_stockrecord
                        ON ecommerce_order_line.stockrecord_id = ecommerce_stockrecord.id
                    INNER JOIN (
                            SELECT
                                ecomm_order.user_id AS user_id,
                                ecomm_product.course_id AS course_id,
                                MAX(ecomm_stockrecord.price_excl_tax) AS course_price
                            FROM order_order ecomm_order
                            JOIN order_line ecomm_order_line
                                ON ecomm_order.id = ecomm_order_line.order_id
                            JOIN catalogue_product ecomm_product
                                ON ecomm_order_line.product_id = ecomm_product.id
                            JOIN partner_stockrecord ecomm_stockrecord
                                ON ecomm_order_line.stockrecord_id = ecomm_stockrecord.id
                            GROUP BY ecomm_order.user_id, ecomm_product.course_id
                    ) ecomm_order_product
                        ON ecommerce_user.id = ecomm_order_product.user_id
                        AND ecommerce_catalogue_product.course_id = ecomm_order_product.course_id
                        AND ecommerce_stockrecord.price_excl_tax = ecomm_order_product.course_price
                    LEFT JOIN order_orderdiscount ecommerce_order_discount
                        ON ecommerce_order_line.order_id = ecommerce_order_discount.order_id
                    LEFT JOIN voucher_voucher ecommerce_voucher
                        ON ecommerce_order_discount.voucher_id = ecommerce_voucher.id
                    LEFT JOIN offer_conditionaloffer ecommerce_offer
                        ON ecommerce_order_discount.offer_id = ecommerce_offer.id
                    LEFT JOIN offer_benefit ecommerce_benefit
                        ON ecommerce_offer.benefit_id = ecommerce_benefit.id
                ) ecommerce_data
                    ON auth_user.username = ecommerce_data.username
                    AND enterprise_course_enrollment.course_id = ecommerce_data.course_id
                """

    def load_dataframes_and_tempview(self, cols, table='', column_delimiter='\x01'):
        if len(cols) < 2 or not table:
            self.log.warn("\n Columns or table name should not be empty : {}\n".format(table))
            return
        date_of_import = str(datetime.datetime.utcnow().date())
        if table in ['course_catalog', 'user_activity_by_user']:
            date_of_import = self.date
        schema = self.get_dataframe_schema(cols)
        source_path = url_path_join(self.warehouse_path, table, 'dt=' + str(date_of_import) + '/')
        dataframe = self.get_dataframe_by_source(source_path, schema, column_delimiter)
        dataframe = dataframe.replace('\N', '')  # replacing null placeholder
        dataframe.createOrReplaceTempView(table)

    def spark_job(self, *args):
        from pyspark.sql.functions import date_format
        auth_user_columns = ['id', 'username', 'last_login', 'date_joined', 'is_active', 'is_superuser', 'is_staff',
                             'email']
        auth_userprofile_columns = ['user_id', 'name', 'gender', 'year_of_birth', 'level_of_education', 'language',
                                    'location', 'mailing_address', 'city', 'country', 'goals']
        enterprisecustomer_columns = ['created', 'modified', 'uuid', 'name', 'active', 'site_id', 'catalog',
                                      'enable_data_sharing_consent', 'enforce_data_sharing_consent',
                                      'enable_audit_enrollment', 'enable_audit_data_reporting']
        enterprisecustomeruser_columns = ['id', 'created', 'modified', 'user_id', 'enterprise_customer_id']
        enterprisecourseenrollment_columns = ['id', 'created', 'modified', 'course_id', 'enterprise_customer_user_id']
        datasharingconsent_columns = ['id', 'created', 'modified', 'username', 'granted', 'course_id',
                                      'enterprise_customer_id']
        usersocialauth_columns = ['id', 'user_id', 'provider', 'uid', 'extra_data']
        studentcourseenrollment_columns = ['id', 'user_id', 'course_id', 'created', 'is_active', 'mode']
        grades_persistentcoursegrade_columns = ['id', 'user_id', 'course_id', 'course_edited_timestamp',
                                                'course_version', 'grading_policy_hash', 'percent_grade',
                                                'letter_grade', 'passed_timestamp', 'created', 'modified']
        course_catalog_columns = ['course_id', 'catalog_course', 'catalog_course_title', 'start_time', 'end_time',
                                  'enrollment_start_time', 'enrollment_end_time', 'content_language', 'pacing_type',
                                  'level_type', 'availability', 'org_id', 'partner_short_code', 'marketing_url',
                                  'min_effort', 'max_effort', 'announcement_time', 'reporting_type']
        user_activity_by_user_columns = ['user_id', 'course_id', 'date', 'category', 'count']
        catalogue_product_columns = ['id', 'structure', 'upc', 'title', 'slug', 'description', 'rating', 'date_created',
                                     'date_updated', 'is_discountable', 'parent_id', 'product_class_id', 'course_id',
                                     'expires']
        order_line_columns = ['id', 'partner_name', 'partner_sku', 'partner_line_reference', 'partner_line_notes',
                              'title', 'upc', 'quantity', 'line_price_incl_tax', 'line_price_excl_tax',
                              'line_price_before_discounts_incl_tax', 'line_price_before_discounts_excl_tax',
                              'unit_cost_price', 'unit_price_incl_tax', 'unit_price_excl_tax', 'unit_retail_price',
                              'status', 'est_dispatch_date', 'order_id', 'partner_id', 'product_id', 'stockrecord_id']
        order_orderdiscount_columns = ['id', 'category', 'offer_id', 'offer_name', 'voucher_id', 'voucher_code',
                                       'frequency', 'amount', 'message', 'order_id']
        voucher_voucher_columns = ['id', 'name', 'code', 'usage', 'start_datetime', 'end_datetime',
                                   'num_basket_additions', 'num_orders', 'total_discount', 'date_created']
        partner_stockrecord_columns = ['id', 'partner_sku', 'price_currency', 'price_excl_tax', 'price_retail',
                                       'cost_price', 'num_in_stock', 'num_allocated', 'low_stock_threshold',
                                       'date_created', 'date_updated', 'partner_id', 'product_id']
        order_order_columns = ['id', 'number', 'currency', 'total_incl_tax', 'total_excl_tax', 'shipping_incl_tax',
                               'shipping_excl_tax', 'shipping_method', 'shipping_code', 'status', 'guest_email',
                               'date_placed', 'basket_id', 'billing_address_id', 'shipping_address_id', 'site_id',
                               'user_id']
        ecommerce_user_columns = ['id', 'username', 'email']
        offer_conditionaloffer_columns = ['id', 'name', 'slug', 'description', 'offer_type', 'status', 'priority',
                                          'start_datetime', 'end_datetime', 'max_global_applications',
                                          'max_user_applications', 'max_basket_applications', 'max_discount',
                                          'total_discount', 'num_applications', 'num_orders', 'redirect_url',
                                          'date_created', 'benefit_id', 'condition_id', 'email_domains''site_id']
        offer_benefit_columns = ['id', 'type', 'value', 'max_affected_items', 'proxy_class', 'range_id']

        # loading dataframes & creating temporary views
        self.load_dataframes_and_tempview(auth_user_columns, 'auth_user')
        self.load_dataframes_and_tempview(auth_userprofile_columns, 'auth_userprofile')
        self.load_dataframes_and_tempview(enterprisecustomer_columns, 'enterprise_enterprisecustomer')
        self.load_dataframes_and_tempview(enterprisecustomeruser_columns, 'enterprise_enterprisecustomeruser')
        self.load_dataframes_and_tempview(enterprisecourseenrollment_columns, 'enterprise_enterprisecourseenrollment')
        self.load_dataframes_and_tempview(datasharingconsent_columns, 'consent_datasharingconsent')
        self.load_dataframes_and_tempview(usersocialauth_columns, 'social_auth_usersocialauth')
        self.load_dataframes_and_tempview(studentcourseenrollment_columns, 'student_courseenrollment')
        self.load_dataframes_and_tempview(grades_persistentcoursegrade_columns, 'grades_persistentcoursegrade')
        self.load_dataframes_and_tempview(course_catalog_columns, 'course_catalog', '\t')
        self.load_dataframes_and_tempview(user_activity_by_user_columns, 'user_activity_by_user', '\t')
        self.load_dataframes_and_tempview(catalogue_product_columns, 'catalogue_product')
        self.load_dataframes_and_tempview(order_line_columns, 'order_line')
        self.load_dataframes_and_tempview(order_orderdiscount_columns, 'order_orderdiscount')
        self.load_dataframes_and_tempview(voucher_voucher_columns, 'voucher_voucher')
        self.load_dataframes_and_tempview(partner_stockrecord_columns, 'partner_stockrecord')
        self.load_dataframes_and_tempview(order_order_columns, 'order_order')
        self.load_dataframes_and_tempview(ecommerce_user_columns, 'ecommerce_user')
        self.load_dataframes_and_tempview(offer_conditionaloffer_columns, 'offer_conditionaloffer')
        self.load_dataframes_and_tempview(offer_benefit_columns, 'offer_benefit')

        # run the query
        tmp_result = self._spark.sql(self.get_insert_sql_query())
        # formating the timestamp columns
        result = tmp_result.withColumn('enrollment_created_timestamp',
                                       date_format(tmp_result['enrollment_created_timestamp'], 'yyyy-MM-dd HH:mm:ss')) \
            .withColumn('passed_timestamp',
                        date_format(tmp_result['passed_timestamp'], 'yyyy-MM-dd HH:mm:ss')) \
            .withColumn('user_account_creation_timestamp',
                        date_format(tmp_result['user_account_creation_timestamp'], 'yyyy-MM-dd HH:mm:ss'))
        result.write \
            .option("numPartitions", self.num_partitions) \
            .option('driver', self.mysql_driver_class) \
            .jdbc(
                url=self.get_jdbc_url(),
                table=self.table,
                mode='append',
                properties={
                    'user': self.credentials.get('username', ''),
                    'password': self.credentials.get('password', '')
                }
            )

    def run(self):
        self.remove_output_on_overwrite()
        self.credentials = self.get_credentials()
        super(EnterpriseEnrollmentSparkMysqlTask, self).run()

    def on_success(self):
        self.output().touch_marker()

    def update_id(self):
        return str(hash(self)).replace('-', 'n')

    def output(self):
        """
        Ideally this should be a MysqlTarget representing the inserted dataset ( which requires alot more code changes
        to make it work ), so for the sake of discovery ticket I'm writing a marker file.
        """
        marker_url = url_path_join(self.marker, self.update_id())
        return get_target_from_url(marker_url, marker=True)


@workflow_entry_point
class ImportEnterpriseEnrollmentsIntoMysqlSpark(
    SparkMixin,
    OverwriteHiveAndMysqlDownstreamMixin,
    LoadInternalReportingCourseCatalogMixin,
    luigi.WrapperTask
):
    """Import enterprise enrollment data into MySQL."""

    def requires(self):
        kwargs = {
            'warehouse_path': self.warehouse_path,
            'overwrite': self.overwrite_hive,
            'date': self.date,
            'api_root_url': self.api_root_url,
            'api_page_size': self.api_page_size,
            'driver_memory': self.driver_memory,
            'executor_memory': self.executor_memory,
            'executor_cores': self.executor_cores,
            'spark_conf': self.spark_conf,
        }

        yield [
            EnterpriseEnrollmentSparkMysqlTask(**kwargs),
        ]
