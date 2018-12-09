"""Compute metrics related to user enrollments in courses"""

import logging

import luigi
import luigi.task

from edx.analytics.tasks.common.mysql_load import MysqlInsertTask
from edx.analytics.tasks.enterprise.enterprise_database_imports import (
    ImportBenefitTask, ImportConditionalOfferTask, ImportDataSharingConsentTask,
    ImportEnterpriseCourseEnrollmentUserTask, ImportEnterpriseCustomerTask, ImportEnterpriseCustomerUserTask,
    ImportStockRecordTask, ImportUserSocialAuthTask, ImportVoucherTask
)
from edx.analytics.tasks.insights.database_imports import (
    ImportAuthUserProfileTask, ImportAuthUserTask, ImportCurrentOrderDiscountState, ImportCurrentOrderLineState,
    ImportCurrentOrderState, ImportEcommerceUser, ImportPersistentCourseGradeTask, ImportProductCatalog,
    ImportStudentCourseEnrollmentTask
)
from edx.analytics.tasks.insights.enrollments import OverwriteHiveAndMysqlDownstreamMixin
from edx.analytics.tasks.insights.user_activity import UserActivityTableTask
from edx.analytics.tasks.util.decorators import workflow_entry_point
from edx.analytics.tasks.util.hive import BareHiveTableTask, HivePartitionTask, OverwriteAwareHiveQueryDataTask
from edx.analytics.tasks.util.record import (
    BooleanField, DateField, DateTimeField, FloatField, IntegerField, Record, StringField
)
from edx.analytics.tasks.warehouse.load_internal_reporting_course_catalog import (
    CoursePartitionTask, LoadInternalReportingCourseCatalogMixin
)

log = logging.getLogger(__name__)


class EnterpriseEnrollmentRecord(Record):
    """Summarizes a course's enrollment by gender and date."""
    enterprise_id = StringField(length=32, nullable=False, description='')
    enterprise_name = StringField(length=255, nullable=False, description='')
    lms_user_id = IntegerField(nullable=False, description='')
    enterprise_user_id = IntegerField(nullable=False, description='')
    course_id = StringField(length=255, nullable=False, description='The course the learner is enrolled in.')
    enrollment_created_timestamp = DateTimeField(nullable=False, description='')
    user_current_enrollment_mode = StringField(length=32, nullable=False, description='')
    consent_granted = BooleanField(description='')
    letter_grade = StringField(length=32, description='')
    has_passed = BooleanField(description='')
    passed_timestamp = DateTimeField(description='')
    enterprise_sso_uid = StringField(length=255, description='')
    enterprise_site_id = IntegerField(description='')
    course_title = StringField(length=255, description='')
    course_start = DateTimeField(description='')
    course_end = DateTimeField(description='')
    course_pacing_type = StringField(length=32, description='')
    course_duration_weeks = StringField(length=32, description='')
    course_min_effort = IntegerField(description='')
    course_max_effort = IntegerField(description='')
    user_account_creation_timestamp = DateTimeField(description='')
    user_email = StringField(length=255, description='')
    user_username = StringField(length=255, description='')
    course_key = StringField(length=255, description='')
    user_country_code = StringField(length=2, description='')
    last_activity_date = DateField(description='')
    coupon_name = StringField(length=255, description='')
    coupon_code = StringField(length=255, description='')
    offer = StringField(length=255, description='')
    current_grade = FloatField(description='')
    course_price = FloatField(description='')
    discount_price = FloatField(description='')


class EnterpriseEnrollmentHiveTableTask(BareHiveTableTask):
    """
    Creates the metadata for the enterprise_enrollment hive table
    """
    @property  # pragma: no cover
    def partition_by(self):
        return 'dt'

    @property
    def table(self):  # pragma: no cover
        return 'enterprise_enrollment'

    @property
    def columns(self):
        return EnterpriseEnrollmentRecord.get_hive_schema()


class EnterpriseEnrollmentHivePartitionTask(HivePartitionTask):
    """
    Generates the enterprise_enrollment hive partition.
    """
    date = luigi.DateParameter()

    @property
    def hive_table_task(self):  # pragma: no cover
        return EnterpriseEnrollmentHiveTableTask(
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite
        )

    @property
    def partition_value(self):  # pragma: no cover
        """ Use a dynamic partition value based on the date parameter. """
        return self.date.isoformat()  # pylint: disable=no-member


class EnterpriseEnrollmentDataTask(
    OverwriteHiveAndMysqlDownstreamMixin,
    LoadInternalReportingCourseCatalogMixin,
    OverwriteAwareHiveQueryDataTask
):
    """
    Executes a hive query to gather enterprise enrollment data and store it in the enterprise_enrollment hive table.
    """

    otto_credentials = luigi.Parameter(
        config_path={'section': 'otto-database-import', 'name': 'credentials'}
    )
    otto_database = luigi.Parameter(
        config_path={'section': 'otto-database-import', 'name': 'database'}
    )

    @property
    def insert_query(self):
        """The query builder that controls the structure and fields inserted into the new table."""
        return """
            SELECT DISTINCT enterprise_customer.uuid AS enterprise_id,
                    enterprise_customer.name AS enterprise_name,
                    enterprise_user.user_id AS lms_user_id,
                    enterprise_user.id AS enterprise_user_id,
                    enterprise_course_enrollment.course_id,
                    enterprise_course_enrollment.created AS enrollment_created_timestamp,
                    enrollment.mode AS user_current_enrollment_mode,
                    consent.granted AS consent_granted,
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
                    course.min_effort AS course_min_effort,
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

    @property
    def hive_partition_task(self):  # pragma: no cover
        """The task that creates the partition used by this job."""
        return EnterpriseEnrollmentHivePartitionTask(
            date=self.date,
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )

    def requires(self):  # pragma: no cover
        for requirement in super(EnterpriseEnrollmentDataTask, self).requires():
            yield requirement

        # the process that generates the source table used by this query
        yield (
            ImportAuthUserTask(),
            ImportAuthUserProfileTask(),
            ImportEnterpriseCustomerTask(),
            ImportEnterpriseCustomerUserTask(),
            ImportEnterpriseCourseEnrollmentUserTask(),
            ImportDataSharingConsentTask(),
            ImportUserSocialAuthTask(),
            ImportStudentCourseEnrollmentTask(),
            ImportPersistentCourseGradeTask(),
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


class EnterpriseEnrollmentMysqlTask(
    OverwriteHiveAndMysqlDownstreamMixin,
    LoadInternalReportingCourseCatalogMixin,
    MysqlInsertTask
):
    """
    All enrollments of enterprise users enrolled in courses associated with their enterprise.
    """

    @property
    def table(self):  # pragma: no cover
        return 'enterprise_enrollment'

    @property
    def insert_source_task(self):  # pragma: no cover
        return EnterpriseEnrollmentDataTask(
            warehouse_path=self.warehouse_path,
            overwrite_hive=self.overwrite_hive,
            overwrite_mysql=self.overwrite_mysql,
            overwrite=self.overwrite,
            date=self.date,
            api_root_url=self.api_root_url,
            api_page_size=self.api_page_size,
        )

    @property
    def columns(self):
        return EnterpriseEnrollmentRecord.get_sql_schema()

    @property
    def indexes(self):
        return [
            ('enterprise_id',),
        ]


@workflow_entry_point
class ImportEnterpriseEnrollmentsIntoMysql(
    OverwriteHiveAndMysqlDownstreamMixin,
    LoadInternalReportingCourseCatalogMixin,
    luigi.WrapperTask
):
    """Import enterprise enrollment data into MySQL."""

    def requires(self):
        kwargs = {
            'warehouse_path': self.warehouse_path,
            'overwrite_hive': self.overwrite_hive,
            'overwrite_mysql': self.overwrite_mysql,
            'overwrite': self.overwrite_hive,
            'date': self.date,
            'api_root_url': self.api_root_url,
            'api_page_size': self.api_page_size,
        }

        yield [
            EnterpriseEnrollmentMysqlTask(**kwargs),
        ]
