"""
Tasks associated with pulling and storing financial fees related data.
"""
from __future__ import absolute_import

import logging

import luigi

from edx.analytics.tasks.common.vertica_load import VerticaCopyTask
from edx.analytics.tasks.util.hive import WarehouseMixin
from edx.analytics.tasks.util.url import ExternalURL, url_path_join
from edx.analytics.tasks.warehouse.financial.affiliate_window import (
    AffiliateWindowTaskMixin, IntervalPullFromAffiliateWindowTask
)

logger = logging.getLogger(__name__)


class AffiliateWindowTask(AffiliateWindowTaskMixin, WarehouseMixin, luigi.WrapperTask):
    """
    Task to pull and import Affiliate Window report data.
    """
    def requires(self):
        yield IntervalPullFromAffiliateWindowTask(
            output_root=self.output_root,
            interval_end=self.run_date,
        )

    def output(self):
        # TODO: Once VerticaCopyTask handles multiple input files update this
        # to use the outputs of the sub-jobs instead of always returning all
        # files.

        # Affiliate Window reports for each day are stored in dated directories.
        # We want to be able to load all that data into Vertica in one go, hence we use
        # a wildcard('*') here.
        url = url_path_join(self.warehouse_path, 'fees', 'affiliate_window') + '/dt=*/'
        return ExternalURL(url=url).output()


class LoadFeesToWarehouse(WarehouseMixin, VerticaCopyTask):
    """
    An entry point to loading fee-related data to the warehouse.
    """
    run_date = luigi.DateParameter()

    # Forcing this to overwrite now, as this script will duplicate data
    # without it. This does not trickle down to the Affiliate Window calls.
    # TODO: Make this optional once VerticaCopyTask handles multiple input files.
    overwrite = True

    @property
    def insert_source_task(self):
        return AffiliateWindowTask(
            run_date=self.run_date,
            output_root=url_path_join(self.warehouse_path, 'fees', 'affiliate_window')
        )

    @property
    def table(self):
        return 'affiliate_window_transactions'

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
            ('id', 'INTEGER'),
            ('url', 'VARCHAR(500)'),
            ('publisher_id', 'INTEGER'),
            ('commission_sharing_publisher_id', 'INTEGER'),
            ('site_name', 'VARCHAR(255)'),
            ('commission_status', 'VARCHAR(255)'),
            ('commission_amount', 'DECIMAL(12,2)'),
            ('sale_amount', 'DECIMAL(12,2)'),
            ('customer_country', 'VARCHAR(5)'),
            ('click_date', 'TIMESTAMP'),
            ('transaction_date', 'TIMESTAMP'),
            ('validation_date', 'TIMESTAMP'),
            ('type', 'VARCHAR(255)'),
            ('decline_reason', 'VARCHAR(512)'),
            ('voucher_code_used', 'BOOLEAN'),
            ('voucher_code', 'VARCHAR(255)'),
            ('amended', 'BOOLEAN'),
            ('amend_reason', 'VARCHAR(1000)'),
            ('old_sale_amount', 'DECIMAL(12,2)'),
            ('old_commission_amount', 'DECIMAL(12,2)'),
            ('publisher_url', 'VARCHAR(500)'),
            ('order_ref', 'VARCHAR(255)'),
            ('paid_to_publisher', 'BOOLEAN'),
            ('payment_id', 'INTEGER'),
            ('transaction_query_id', 'INTEGER'),
            ('original_sale_amount', 'DECIMAL(12,2)'),
            ('original_json', 'VARCHAR(5000)'),
        ]
