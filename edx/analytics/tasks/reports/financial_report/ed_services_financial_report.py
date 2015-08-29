import luigi
import luigi.hdfs
import luigi.date_interval

from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.hive import HiveTableFromQueryTask, HivePartition
# from edx.analytics.tasks.reports.financial_report.finance_reports import BuildFinancialReportsMixin
from edx.analytics.tasks.reports.reconcile import ReconciledOrderTransactionTableTask
from edx.analytics.tasks.database_imports import (
    DatabaseImportMixin, ImportCourseModeTask, ImportStudentCourseEnrollmentTask
)

class ImportCourseAndEnrollmentTablesTask(DatabaseImportMixin, luigi.WrapperTask):
    """
    Builds the Course and Enrollment data to satisfy the Ed Services report.
    """
    interval_start = luigi.DateParameter()

    def requires(self):
        kwargs = {
            'interval_start': self.interval_start,
        }
        yield (
            # Import Course Information: Mainly Course Mode & Suggested Prices
            ImportCourseModeTask(),
            # Import Student Enrollment Information
            ImportStudentCourseEnrollmentTask(),
            # Import Reconciled Orders and Transactions
            ReconciledOrderTransactionTableTask(
                interval_start=self.interval_start,
            ),
        )

    def output(self):
        return [task.output() for task in self.requires()]


class BuildEdServicesReportTask(DatabaseImportMixin, HiveTableFromQueryTask):
    """
    Builds the financial report delivered to Ed Services.

    """
    interval_start = luigi.DateParameter()

    def requires(self):
        kwargs = {
            'interval_start': self.interval_start,
            'verbose': self.verbose,
        }
        yield (
            ImportCourseAndEnrollmentTablesTask(
                database=self.database,
                **kwargs
            ),
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
            ('refunded_amount', 'DECIMAL'),
            ('net_seat_revenue', 'DECIMAL'),
            ('net_seat_count', 'INT'),
            ('donation_count', 'INT'),
            ('net_donation_revenue', 'DECIMAL'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.import_date.isoformat())  # pylint: disable=no-member

    @property
    def insert_query(self):
        return """
            SELECT
                courses.course_id,
                COALESCE(VP_COURSES.mode_slug, ""),
                COALESCE(VP_COURSES.suggested_prices, ""),
                COALESCE(VP_COURSES.min_price, ""),
                COALESCE(CAST(VP_COURSES.expiration_datetime AS STRING), ""),
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
                    order_course_id AS course_id
                FROM reconciled_order_transactions
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
