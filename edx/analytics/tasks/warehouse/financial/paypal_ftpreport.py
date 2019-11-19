from __future__ import absolute_import

import csv
import datetime
import fnmatch
import logging

import luigi
from paramiko import SFTPClient, Transport
from six.moves import filter

from edx.analytics.tasks.common.vertica_load import VerticaCopyTask
from edx.analytics.tasks.util.hive import HivePartition, WarehouseMixin
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.record import DateField, DateTimeField, IntegerField, Record, StringField
from edx.analytics.tasks.util.url import get_target_from_url, url_path_join

log = logging.getLogger(__name__)


class PayPalCaseReportMixin(OverwriteOutputMixin):
    """Parameter for Loading Case Report task into Vertica """

    _connection = None
    run_date = luigi.DateParameter(
        default=datetime.datetime.now() - datetime.timedelta(days=1),
        description='Date to generate a report for. Default is yesterday',
    )
    host = luigi.Parameter(
        config_path={'section': 'paypal_sftp', 'name': 'host'}, significant=False,
        description='Hostname for sFTP connection.',
    )
    port = luigi.Parameter(
        config_path={'section': 'paypal_sftp', 'name': 'port'}, significant=False,
        description='Port for sFTP connection.',
    )
    username = luigi.Parameter(
        config_path={'section': 'paypal_sftp', 'name': 'user'}, significant=False,
        description='Username for sFTP connection.',
    )
    password = luigi.Parameter(
        config_path={'section': 'paypal_sftp', 'name': 'password'}, significant=False,
        description='Password for sFTP connection.',
    )
    remote_path = luigi.Parameter(
        config_path={'section': 'paypal_sftp', 'name': 'remote_path'},
        description='A remote path on sFTP server.',
    )
    schema = luigi.Parameter(
        description='The schema in vertica database to which to write.',
    )


class PayPalPullCaseReportTask(PayPalCaseReportMixin, WarehouseMixin, OverwriteOutputMixin, luigi.Task):
    """
    Run PayPal 'Case Report', gather data and output raw csv file in S3.

    """

    def __init__(self, *args, **kwargs):
        super(PayPalPullCaseReportTask, self).__init__(*args, **kwargs)
        target = self.output()
        if target.exists():
            target.remove()

    def create_connection(cls, host, port, username, password):
        """ Create PayPal SFTP connection """
        transport = Transport(host, port)
        transport.connect(username=username, password=password)
        return SFTPClient.from_transport(transport)

    def get_file(self, remote_path):
        """ Get remote filename. Sample remote filename: DDR-20190822.01.008.CSV """
        date_string = self.run_date.strftime('%Y%m%d')
        pattern = ('DDR', date_string, 'CSV')
        remote_filepattern = "*".join(pattern)
        for file in self._connection.listdir(self.remote_path):
            if fnmatch.fnmatch(file, remote_filepattern):
                return file
        return None

    def report_record_count(self, readdict):
        """ To verify report record count with generated output in Case Report """
        sb_count, sf_count = 0, 0
        for row in readdict:
            if row['CH'] == 'SB':
                sb_count += 1
            if row['CH'] == 'SF':
                sf_count = int(row['Case type'])
                break
        if sb_count != sf_count:
            return False
        return True

    def run(self):
        """
        Read Report file and reformat output.

        Here is file structure doc link.
        https://www.paypalobjects.com/webstatic/en_US/developer/docs/pdf/PP_LRD_GenDisputeReport.pdf#page=13
        """

        self._connection = self.create_connection(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
        )
        remote_filename = self.get_file(self.remote_path)
        if remote_filename is not None:
            self._connection.chdir(self.remote_path)
            input_file = self._connection.open(remote_filename, 'rb')
            file_read = input_file.readlines()[3:]
            reader = csv.DictReader(file_read, delimiter=str(',').encode('utf-8'))
            record_count = self.report_record_count(reader)
            if not record_count:
                raise Exception('There is mismatch in report record count. Check remote file: {0}'.format(remote_filename))
            input_file.close()
            input_file = self._connection.open(remote_filename, 'rb')
            with self.output().open('w') as output_file:
                for row in input_file:
                    output_file.write(row)
                input_file.close()
                self._connection.close()
        else:
            self._connection.close()
            raise Exception("Remote File Not found for date: {0}".format(self.run_date.strftime('%Y-%m-%d')))

    def output(self):
        """
        Output is set up so it can be read in as a Hive table with partitions.

        The form is {output_root}/paypal_casereport/paypal_ftpraw/dt={CCYY-mm-dd}/PayPalCaseReport.tsv
        """
        date_string = self.run_date.strftime('%Y-%m-%d')
        partition_path_spec = HivePartition('dt', date_string).path_spec
        filename = "PayPalCaseReport.tsv"
        url_with_filename = url_path_join(self.warehouse_path, "paypal_casereport", "paypal_ftpraw", partition_path_spec, filename)
        return get_target_from_url(url_with_filename)


class PayPalProcessCaseReportTask(PayPalCaseReportMixin, WarehouseMixin, OverwriteOutputMixin, luigi.Task):
    """
    Run PayPal 'Case Report', read raw CSV file and format output as TSV file.

    """

    def __init__(self, *args, **kwargs):
        super(PayPalProcessCaseReportTask, self).__init__(*args, **kwargs)
        target = self.output()
        if target.exists():
            target.remove()

    def requires(self):
        return PayPalPullCaseReportTask(
            run_date=self.run_date,
            schema=self.schema,
        )

    def filtercsv_row(self, rows):
        """
        There are multiple row types in PayPal Case Report. Like File header(FH), Section body(SB), Section Footer(SF).
        We are only interested in SB columns in processed file
        """
        return 'SB' in list(rows.values())

    def amount_to_decimal(self, amount):
        """
        PayPal represents currency as whole numbers. Like $50 will be represented as 5000. We will divide by
        100 to get dollar value amount.
        """
        if amount == '':
            return None
        return int(amount) / 100

    def run(self):
        """
        Read Report file and reformat output
        Skip initial 3 lines that provide information about file source and header
        4th line should contain header information. We are only interested in "Section Body Data" in docs
        In processed file we are saving "Section Body Data" only.
        """
        with self.input().open('r') as input_file:
            file_read = input_file.readlines()[3:]
            reader = csv.DictReader(file_read, delimiter=',')
            date_time_field = DateTimeField()
            with self.output().open('w') as output_file:
                for row in filter(self.filtercsv_row, reader):
                    if row['Response due date'] == '':
                        response_date = None
                    else:
                        response_date = date_time_field.deserialize_from_string(row['Response due date'])

                    record = PayPalCaseReportRecord(
                        case_type=row['Case type'],
                        case_id=row['Case ID'],
                        original_transaction_id=row['Original transaction ID'],
                        transaction_date=date_time_field.deserialize_from_string(row['Transaction date']),
                        transaction_invoice_id=row['Transaction invoice ID'],
                        card_type=row['Card Type'],
                        case_reason=row['Case reason'],
                        claimant_name=row['Claimant name'],
                        claimant_email_address=row['Claimant email address'],
                        case_filing_date=date_time_field.deserialize_from_string(row['Case filing date']),
                        case_status=row['Case status'],
                        response_due_date=response_date,
                        disputed_amount=self.amount_to_decimal(row['Disputed amount']),
                        disputed_currency=row['Disputed currency'],
                        disputed_transaction_id=row['Disputed transaction ID'],
                        money_movement=row['Money movement'],
                        settlement_type=row['Settlement type'],
                        seller_protection=row['Seller protection'],
                        seller_protection_payout_amount=self.amount_to_decimal(row['Seller protection payout amount']),
                        seller_protection_currency=row['Seller protection currency'],
                        payment_tracking_id=row['Payment Tracking ID'],
                        buyer_comments=row['Buyer comments'],
                        store_id=row['Store ID'],
                        chargeback_reason_code=row['Chargeback Reason Code'],
                        outcome=row['Outcome'],
                        report_generation_date=DateField().deserialize_from_string(self.run_date.strftime('%Y-%m-%d'))
                    )
                    output_file.write(record.to_separated_values())
                    output_file.write('\n')

    def output(self):
        """
        Output is set up so it can be read in as a Hive table with partitions.

        The form is {output_root}/paypal_casereport/paypal_ftpprocessed/dt={CCYY-mm-dd}/PayPalCaseReport.tsv
        """
        date_string = self.run_date.strftime('%Y-%m-%d')
        partition_path_spec = HivePartition('dt', date_string).path_spec
        filename = "PayPalCaseReport.tsv"
        url_with_filename = url_path_join(self.warehouse_path, "paypal_casereport", "paypal_ftpprocessed", partition_path_spec, filename)
        return get_target_from_url(url_with_filename)


class PayPalCaseReportRecord(Record):
    """Record in PayPal Case Report """

    case_type = StringField(
        length=50, nullable=True,
        description='Type of case made against the transaction for e.g. Chargeback, Dispute, Claim etc'
    )
    case_id = StringField(
        length=18, nullable=True,
        description='PayPal generated unique ID for Case'
    )
    original_transaction_id = StringField(
        length=255, nullable=True,
        description='PayPal generated ID of transaction against which Case was filed'
    )
    transaction_date = DateTimeField(
        nullable=True,
        description='Completion date of the transaction'
    )
    transaction_invoice_id = StringField(
        length=255, nullable=True,
        description='Invoice ID provided with transaction'
    )
    card_type = StringField(
        length=255, nullable=True,
        description='Credit Card type used for the transaction'
    )
    case_reason = StringField(
        length=255, nullable=True,
        description='Systematic reason for the case like Inquiry by PayPal, Item not received, Not as described etc'
    )
    claimant_name = StringField(
        length=128, nullable=True,
        description='Name of the claimant'
    )
    claimant_email_address = StringField(
        length=128, nullable=True,
        description='PayPal email address of the buyer'
    )
    case_filing_date = DateTimeField(
        nullable=True,
        description='Date that the case was originally filed with PayPal'
    )
    case_status = StringField(
        length=255, nullable=True,
        description='State or the status of case'
    )
    response_due_date = DateTimeField(
        nullable=True,
        description='Date by which filed case should be responded'
    )
    disputed_amount = IntegerField(
        nullable=True,
        description='Amount being disputed by the buyer in the original transaction'
    )
    disputed_currency = StringField(
        length=255, nullable=True,
        description='Currency of the disputed amount'
    )
    disputed_transaction_id = StringField(
        length=255, nullable=True,
        description='Transaction ID generated at the time of the money movement event'
    )
    money_movement = StringField(
        length=255, nullable=True,
        description='PayPal amount status like Credit, Debit, On temporary hold etc'
    )
    settlement_type = StringField(
        length=255, nullable=True,
        description='Mode to return money to buyer'
    )
    seller_protection = StringField(
        length=255, nullable=True,
        description='Specifies the Seller Protection status'
    )
    seller_protection_payout_amount = IntegerField(
        nullable=True,
        description='Amount PayPal paid on the behalf of the seller to the buyer as a result of the Seller Protection coverage'
    )
    seller_protection_currency = StringField(
        length=255, nullable=True,
        description='Seller protection currency'
    )
    payment_tracking_id = StringField(
        length=255, nullable=True,
        description='Unique ID to obtain information about payment or refund'
    )
    buyer_comments = StringField(
        length=500, nullable=True,
        description='Comments by buyer'
    )
    store_id = StringField(
        length=255, nullable=True,
        description='Merchant identifier of the store where the purchase occurred'
    )
    chargeback_reason_code = StringField(
        length=32, nullable=True,
        description='Unique identifier to distinguish chargeback nature / reason'
    )
    outcome = StringField(
        length=3000, nullable=True,
        description='Outcome'
    )
    report_generation_date = DateField(
        length=10, nullable=False,
        description='Report file generation date'
    )


class LoadPayPalCaseReportToVertica(PayPalCaseReportMixin, WarehouseMixin, VerticaCopyTask):
    """
    Task for loading PayPal Case report from Hive into Vertica
    """

    @property
    def insert_source_task(self):
        return PayPalProcessCaseReportTask(
            run_date=self.run_date,
            schema=self.schema,
        )

    @property
    def table(self):
        return 'paypal_case_report'

    @property
    def default_columns(self):
        return None

    @property
    def auto_primary_key(self):
        return None

    @property
    def columns(self):
        return PayPalCaseReportRecord.get_sql_schema()
