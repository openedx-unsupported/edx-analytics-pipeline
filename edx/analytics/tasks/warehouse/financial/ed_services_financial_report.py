"""Generates a financial report to be delivered to our good friends in Ed Services."""
import luigi

from edx.analytics.tasks.reports.reconcile import ReconciledOrderTransactionTableTask
from edx.analytics.tasks.database_imports import (
    DatabaseImportMixin, ImportCourseModeTask, ImportStudentCourseEnrollmentTask
)
from edx.analytics.tasks.mapreduce import MapReduceJobTaskMixin
from edx.analytics.tasks.util.hive import HiveTableFromQueryTask, HivePartition, WarehouseMixin, hive_decimal_type
from edx.analytics.tasks.vertica_load import VerticaCopyTask


class BuildEdServicesReportTask(DatabaseImportMixin, MapReduceJobTaskMixin, HiveTableFromQueryTask):
    """
    Builds the financial report delivered to Ed Services.
    """

    def requires(self):
        yield (
            ImportCourseModeTask(
                import_date=self.import_date
            ),
            ImportStudentCourseEnrollmentTask(
                import_date=self.import_date
            ),
            ReconciledOrderTransactionTableTask(
                import_date=self.import_date,
                n_reduce_tasks=self.n_reduce_tasks
            )
        )

    @property
    def table(self):
        return 'ed_services_report'

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
            ('mode_slug', 'STRING'),
            ('suggested_prices', 'STRING'),
            ('min_price', 'INT'),
            ('expiration_datetime', 'STRING'),
            ('total_currently_enrolled', 'INT'),
            ('audit_currently_enrolled', 'INT'),
            ('honor_currently_enrolled', 'INT'),
            ('verified_currently_enrolled', 'INT'),
            ('professional_currently_enrolled', 'INT'),
            ('no_id_professional_currently_enrolled', 'INT'),
            ('refunded_seat_count', 'INT'),
            ('refunded_amount', hive_decimal_type(12, 2)),
            ('net_seat_revenue', hive_decimal_type(12, 2)),
            ('net_seat_count', 'INT'),
            ('donation_count', 'INT'),
            ('net_donation_revenue', hive_decimal_type(12, 2)),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.import_date.isoformat())  # pylint: disable=no-member

    @property
    def insert_query(self):
        return """
            SELECT
                courses.course_id,
                COALESCE(VP_COURSES.mode_slug, "\\\\N"),
                COALESCE(VP_COURSES.suggested_prices, "\\\\N"),
                COALESCE(VP_COURSES.min_price, "\\\\N"),
                COALESCE(CAST(VP_COURSES.expiration_datetime AS STRING), "\\\\N"),
                COALESCE(ALL_ENROLLS.total_currently_enrolled, 0),
                COALESCE(ALL_ENROLLS.audit_currently_enrolled, 0),
                COALESCE(ALL_ENROLLS.honor_currently_enrolled, 0),
                COALESCE(ALL_ENROLLS.verified_currently_enrolled, 0),
                COALESCE(ALL_ENROLLS.professional_currently_enrolled, 0),
                COALESCE(ALL_ENROLLS.no_id_professional_currently_enrolled, 0),
                COALESCE(seats.refunded_seats,0) refunded_seat_count,
                COALESCE(seats.refunded_amount,0) refunded_amount,
                COALESCE(seats.net_amount,0) net_seat_revenue,
                COALESCE(seats.net_seats,0) net_seat_count,
                COALESCE(donations.donations,0) donation_count,
                COALESCE(donations.net_donation_revenue,0) net_donation_revenue

            FROM
            (
                SELECT DISTINCT
                    course_id
                FROM (
                    SELECT
                        order_course_id AS course_id
                    FROM
                        reconciled_order_transactions
                    WHERE
                        order_course_id is not NULL
                    UNION ALL
                    SELECT
                        course_id
                    FROM
                        course_modes_coursemode
                    WHERE
                        mode_slug in ('verified', 'professional', 'no-id-professional')
                ) courses_inner
            ) courses

            -- Course Information --
            LEFT OUTER JOIN
            (
                SELECT
                    course_id,
                    mode_slug,
                    suggested_prices,
                    min_price,
                    expiration_datetime
                FROM
                    course_modes_coursemode
                WHERE
                    mode_slug in ('verified', 'professional', 'no-id-professional')
            ) VP_COURSES ON VP_COURSES.course_id = courses.course_id

            -- Enrollment --
            LEFT OUTER JOIN
            (
                select ce.course_id, count(*) total_currently_enrolled
                    , sum( case when ce.mode = 'audit' then 1 else 0 end ) audit_currently_enrolled
                    , sum( case when ce.mode = 'honor' then 1 else 0 end ) honor_currently_enrolled
                    , sum( case when ce.mode = 'verified' then 1 else 0 end ) verified_currently_enrolled
                    , sum( case when ce.mode = 'professional' then 1 else 0 end ) professional_currently_enrolled
                    , sum( case when ce.mode = 'no-id-professional' then 1 else 0 end ) no_id_professional_currently_enrolled
                    , sum( case when (ce.mode != 'audit' AND ce.mode != 'honor' AND ce.mode != 'verified' AND ce.mode != 'professional' AND ce.mode != 'no-id-professional') then 1 else 0 end ) error_currently_enrolled
                from student_courseenrollment ce
                where  ce.is_active=1
                group by ce.course_id
            ) ALL_ENROLLS on courses.course_id = ALL_ENROLLS.course_id


            -- Seat Transactions --
            LEFT OUTER JOIN
            (
                SELECT
                    item.order_course_id,
                    SUM(item.order_item_active_seats) AS net_seats,
                    SUM(item.order_item_net_revenue) AS net_amount,
                    SUM(item.order_item_refunded_seats) AS refunded_seats,
                    SUM(item.order_item_refund_amount) AS refunded_amount
                FROM
                (
                    SELECT
                        order_course_id,
                        order_line_item_id,
                        IF( SUM(transaction_amount_per_item) > 0.01, 1, 0 ) AS order_item_active_seats,
                        IF( SUM(transaction_amount_per_item) <= 0.01, 1, 0 ) AS order_item_refunded_seats,
                        SUM(transaction_amount_per_item) AS order_item_net_revenue,
                        SUM( CASE WHEN transaction_type = 'refund' THEN transaction_amount_per_item ELSE 0.0BD END ) AS order_item_refund_amount
                    FROM reconciled_order_transactions
                    WHERE
                        order_product_class = 'seat'
                    GROUP BY
                        order_course_id,
                        order_line_item_id,
                        order_processor
                ) item
                GROUP BY item.order_course_id
            ) seats ON seats.order_course_id = courses.course_id


            -- Donation Transactions --
            LEFT OUTER JOIN
            (
                SELECT
                    donation_item.order_course_id,
                    COUNT(donation_item.order_line_item_id) AS donations,
                    SUM(donation_item.order_item_net_donation_revenue) AS net_donation_revenue
                FROM
                (
                    SELECT
                        order_course_id,
                        order_line_item_id,
                        SUM(transaction_amount_per_item) AS order_item_net_donation_revenue
                    FROM reconciled_order_transactions
                    WHERE
                        order_product_class = 'donation'
                    GROUP BY
                        order_course_id,
                        order_line_item_id
                ) donation_item
                GROUP BY
                    donation_item.order_course_id
            ) donations ON donations.order_course_id = courses.course_id
            ;
        """


class LoadInternalReportingEdServicesReportToWarehouse(MapReduceJobTaskMixin, WarehouseMixin, VerticaCopyTask):
    """
    Loads Ed Services Report table from Hive into the Vertica data warehouse.
    """
    # Instead of importing all of DatabaseImportMixin at this level, we just define
    # what we need and are willing to pass through.  That way the use of "credentials"
    # for the output of the report data is not conflicting.
    import_date = luigi.DateParameter()

    @property
    def insert_source_task(self):
        # This gets added to what requires() yields in VerticaCopyTask.
        return (
            BuildEdServicesReportTask(
                import_date=self.import_date,
                n_reduce_tasks=self.n_reduce_tasks,
                # DO NOT PASS OVERWRITE FURTHER.  We mean for overwrite here
                # to just apply to the writing to Vertica, not to anything further upstream.
                # overwrite=self.overwrite,
            )
        )

    @property
    def table(self):
        return 'ed_services_report'

    @property
    def auto_primary_key(self):
        """No automatic primary key here."""
        return None

    @property
    def columns(self):
        """
        Most values are mapped back to their original table definitions.
        """
        return [
            ('course_id', 'VARCHAR(255)'),
            ('mode_slug', 'VARCHAR(100)'),
            ('suggested_prices', 'VARCHAR(255)'),  # a CommaSeparatedIntegerField in LMS CourseMode
            ('min_price', 'DECIMAL(12,2)'),  # an Integer in LMS CourseMode
            ('expiration_datetime', 'TIMESTAMP'),
            ('total_currently_enrolled', 'INTEGER'),
            ('audit_currently_enrolled', 'INTEGER'),
            ('honor_currently_enrolled', 'INTEGER'),
            ('verified_currently_enrolled', 'INTEGER'),
            ('professional_currently_enrolled', 'INTEGER'),
            ('no_id_professional_currently_enrolled', 'INTEGER'),
            ('refunded_seat_count', 'INTEGER'),
            ('refunded_amount', 'DECIMAL(12,2)'),
            ('net_seat_revenue', 'DECIMAL(12,2)'),
            ('net_seat_count', 'INTEGER'),
            ('donation_count', 'INTEGER'),
            ('net_donation_revenue', 'DECIMAL(12,2)'),
        ]
