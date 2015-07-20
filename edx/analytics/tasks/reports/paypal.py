import datetime
import xml.etree.cElementTree as ET
from cStringIO import StringIO
import logging
from collections import namedtuple, OrderedDict
from decimal import Decimal
import time

import luigi
from luigi.configuration import get_config
import requests

from edx.analytics.tasks.url import get_target_from_url
from edx.analytics.tasks.url import url_path_join
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin

# Tell urllib3 to switch the ssl backend to PyOpenSSL.
# see https://urllib3.readthedocs.org/en/latest/security.html#pyopenssl
import urllib3.contrib.pyopenssl
urllib3.contrib.pyopenssl.inject_into_urllib3()

log = logging.getLogger(__name__)


class PaypalApiRequest(object):

    # WARNING: the XML parser used by the paypal service will not recognize fields in a different order than the one
    # specified below. DO NOT CHANGE the ordering of fields within requests! They could have avoided this by using
    # <xsd:all> instead of <xsd:sequence> for their types that shouldn't care about the ordering of fields (many of
    # their complex types fall into this category).

    def __init__(self):
        configuration = get_config()
        self.partner = configuration.get('paypal', 'partner', 'PayPal')
        self.vendor = configuration.get('paypal', 'vendor')
        self.password = configuration.get('paypal', 'password')
        self.user = configuration.get('paypal', 'user', None)

    def create_request_document(self):
        root_node = ET.Element('reportingEngineRequest')
        self.append_authentication_node(root_node)
        self.append_request_node(root_node)

        tree = ET.ElementTree(root_node)
        string_buffer = StringIO()
        tree.write(string_buffer, encoding='UTF-8', xml_declaration=True)
        return string_buffer.getvalue()

    def append_authentication_node(self, root_node):
        auth_node = ET.SubElement(root_node, 'authRequest')

        for attribute in ('user', 'vendor', 'partner', 'password'):
            child_node = ET.SubElement(auth_node, attribute)
            child_node.text = unicode(getattr(self, attribute))

    def append_request_node(self, root_node):
        pass


class PaypalApiResponse(object):

    SUCCESSFUL_RESPONSE_CODE = 100

    def __init__(self, response_code, response_message):
        self.response_code = response_code
        self.response_message = response_message

    @classmethod
    def params_from_xml(cls, root_node):
        base_response_node = root_node.find('baseResponse')
        return {
            'response_code': int(base_response_node.findtext('responseCode')),
            'response_message': base_response_node.findtext('responseMsg')
        }

    @classmethod
    def from_xml(cls, root_node):
        params = cls.params_from_xml(root_node)
        if params is None:
            return None
        else:
            return cls(**params)

    def raise_for_status(self):
        if self.response_code != self.SUCCESSFUL_RESPONSE_CODE:
            raise RuntimeError(
                'Paypal API request failed with code {0}: {1}'.format(self.response_code, self.response_message)
            )


def send_paypal_request(api_request):
    url = get_config().get('paypal', 'url')
    request_document = api_request.create_request_document()
    headers = {
        'Content-Type': 'text/plain'
    }
    response = requests.post(
        url,
        data=request_document,
        headers=headers,
    )
    response.raise_for_status()
    api_response = parse_paypal_api_response(response.content)

    return api_response


def parse_paypal_api_response(response_str):
    root = ET.fromstring(response_str)

    response_classes = [
        PaypalReportResponse, PaypalReportMetadataResponse, PaypalReportDataResponse, PaypalApiResponse
    ]

    for response_cls in response_classes:
        response = response_cls.from_xml(root)
        if response is not None:
            return response


class PaypalReportRequest(PaypalApiRequest):

    DEFAULT_PAGE_SIZE = 50

    def __init__(self, report_name, **report_params):
        super(PaypalReportRequest, self).__init__()
        self.report_name = report_name
        self.report_params = report_params
        self.page_size = self.report_params.pop('page_size', self.DEFAULT_PAGE_SIZE)

    def append_request_node(self, root_node):

        # WARNING: the paypal XML parser is position sensitive. Do NOT change the ordering of the fields in the request.
        request_node = ET.SubElement(root_node, 'runReportRequest')
        name_node = ET.SubElement(request_node, 'reportName')
        name_node.text = unicode(self.report_name)

        for param_name, param_value in self.report_params.iteritems():
            param_node = ET.SubElement(request_node, 'reportParam')
            param_name_node = ET.SubElement(param_node, 'paramName')
            param_name_node.text = unicode(param_name)
            param_value_node = ET.SubElement(param_node, 'paramValue')
            param_value_node.text = unicode(param_value)

        page_size_node = ET.SubElement(request_node, 'pageSize')
        page_size_node.text = unicode(self.page_size)


class PaypalReportResponse(PaypalApiResponse):

    REPORT_CREATED_STATUS_CODE = 1
    REPORT_EXECUTING_STATUS_CODE = 2
    REPORT_COMPLETE_STATUS_CODE = 3

    def __init__(self, response_code, response_message, report_id, status_code, status_message):
        super(PaypalReportResponse, self).__init__(response_code, response_message)
        self.report_id = report_id
        self.status_code = status_code
        self.status_message = status_message

    @classmethod
    def params_from_xml(cls, root_node):
        maybe_report_node = root_node.find('runReportResponse')
        if maybe_report_node is None:
            maybe_report_node = root_node.find('getResultsResponse')
            if maybe_report_node is not None:
                maybe_report_node = maybe_report_node.find('Results')

        if maybe_report_node is None:
            return None

        params = PaypalApiResponse.params_from_xml(root_node)
        params['report_id'] = maybe_report_node.findtext('reportId')
        params['status_code'] = int(maybe_report_node.findtext('statusCode'))
        params['status_message'] = maybe_report_node.findtext('statusMsg')
        return params

    @property
    def is_running(self):
        return self.status_code in (self.REPORT_CREATED_STATUS_CODE, self.REPORT_EXECUTING_STATUS_CODE)

    @property
    def is_ready(self):
        return self.status_code == self.REPORT_COMPLETE_STATUS_CODE

    def raise_for_status(self):
        super(PaypalReportResponse, self).raise_for_status()
        if self.status_code > self.REPORT_COMPLETE_STATUS_CODE:
            raise RuntimeError(
                'Paypal report request failed with status code {0}: {1}'.format(self.status_code, self.status_message)
            )


class PaypalReportMetadataRequest(PaypalApiRequest):

    def __init__(self, report_id):
        super(PaypalReportMetadataRequest, self).__init__()
        self.report_id = report_id

    def append_request_node(self, root_node):
        request_node = ET.SubElement(root_node, 'getMetaDataRequest')
        report_id_node = ET.SubElement(request_node, 'reportId')
        report_id_node.text = unicode(self.report_id)


ColumnMetadata = namedtuple('ColumnMetadata', ('name', 'data_type'))


class PaypalReportMetadataResponse(PaypalApiResponse):

    def __init__(self, response_code, response_message, num_rows, num_pages, page_size, columns):
        super(PaypalReportMetadataResponse, self).__init__(response_code, response_message)
        self.num_rows = num_rows
        self.num_pages = num_pages
        self.page_size = page_size
        self.columns = columns

    @classmethod
    def params_from_xml(cls, root_node):
        maybe_node = root_node.find('getMetaDataResponse')
        if maybe_node is None:
            return None

        params = PaypalApiResponse.params_from_xml(root_node)
        params['num_rows'] = int(maybe_node.findtext('numberOfRows'))
        params['num_pages'] = int(maybe_node.findtext('numberOfPages'))
        params['page_size'] = int(maybe_node.findtext('pageSize'))

        columns = OrderedDict()
        for column_node in maybe_node.iterfind('columnMetaData'):
            name = column_node.findtext('dataName')
            data_type = column_node.findtext('dataType')
            columns[name] = ColumnMetadata(name=name, data_type=data_type)
        params['columns'] = columns

        return params


class PaypalReportDataRequest(PaypalApiRequest):

    def __init__(self, report_id, page_num=1):
        super(PaypalReportDataRequest, self).__init__()
        self.report_id = report_id
        self.page_num = page_num

    def append_request_node(self, root_node):
        request_node = ET.SubElement(root_node, 'getDataRequest')
        report_id_node = ET.SubElement(request_node, 'reportId')
        report_id_node.text = unicode(self.report_id)
        page_num_node = ET.SubElement(request_node, 'pageNum')
        page_num_node.text = unicode(self.page_num)


class PaypalReportDataResponse(PaypalApiResponse):

    def __init__(self, response_code, response_message, rows):
        super(PaypalReportDataResponse, self).__init__(response_code, response_message)
        self.rows = rows

    @classmethod
    def params_from_xml(cls, root_node):
        maybe_node = root_node.find('getDataResponse')
        if maybe_node is None:
            return None

        params = PaypalApiResponse.params_from_xml(root_node)

        rows = []
        for row_node in maybe_node.iterfind('reportDataRow'):
            row = []
            for column_node in row_node.iterfind('columnData'):
                row.append(column_node.findtext('data'))

            rows.append(row)

        params['rows'] = rows
        return params

    def write_to_tsv_file(self, tsv_file):
        for row in self.rows:
            tsv_file.write('\t'.join(row))
            tsv_file.write('\n')


class PaypalReportResultsRequest(PaypalApiRequest):

    def __init__(self, report_id):
        super(PaypalReportResultsRequest, self).__init__()
        self.report_id = report_id

    def append_request_node(self, root_node):
        request_node = ET.SubElement(root_node, 'getResultsRequest')
        report_id_node = ET.SubElement(request_node, 'reportId')
        report_id_node.text = unicode(self.report_id)


class PaypalTaskMixin(OverwriteOutputMixin):
    output_root = luigi.Parameter()
    date = luigi.DateParameter(default=datetime.datetime.utcnow().date())


class RawPaypalTask(PaypalTaskMixin, luigi.Task):

    def run(self):
        self.remove_output_on_overwrite()

        ppr = PaypalReportRequest(
            'SettlementReport',
            processor='PayPal',
            start_date=self.date.isoformat() + ' 00:00:00',
            end_date=self.date.isoformat() + ' 23:59:59',
            timezone='GMT'
        )
        report_response = send_paypal_request(ppr)
        report_response.raise_for_status()
        report_id = report_response.report_id

        is_running = report_response.is_running
        while is_running:
            # Wait for the report to finish running
            time.sleep(5)
            results_response = send_paypal_request(PaypalReportResultsRequest(report_id=report_id))
            results_response.raise_for_status()
            is_running = results_response.is_running

        mdr = PaypalReportMetadataRequest(report_id=report_id)
        metadata_response = send_paypal_request(mdr)
        metadata_response.raise_for_status()

        with self.output().open('w') as output_tsv_file:
            for page_num in range(metadata_response.num_pages):
                rdr = PaypalReportDataRequest(report_id=report_id, page_num=(page_num + 1))
                data_response = send_paypal_request(rdr)
                data_response.raise_for_status()
                data_response.write_to_tsv_file(output_tsv_file)

    def output(self):
        url_with_filename = url_path_join(self.output_root, 'paypal', '{0}.tsv'.format(self.date.isoformat()))
        return get_target_from_url(url_with_filename)


BaseSettlementReportRecord = namedtuple('SettlementReportRecord', [
    'transaction_id',
    'time',
    'type',
    'tender_type',
    'account_number',
    'expires',
    'amount',
    'result_code',
    'response_msg',
    'comment_1',
    'comment_2',
    'batch_id',
    'currency_symbol',
    'paypal_transaction_id',
    'paypal_fees',
    'paypal_email_id',
    'original_pnref',
    'original_type',
    'original_amount',
    'original_time',
    'invoice_number',
    'purchase_order',
    'customer_ref'
])

class SettlementReportRecord(BaseSettlementReportRecord):

    @property
    def transaction_type(self):
        if self.type == 'Sale':
            return 'sale'
        elif self.type == 'Credit':
            return 'refund'
        else:
            raise RuntimeError("Unknown transaction type: {0}".format(self.type))

    @property
    def decimal_amount(self):
        return str(self.amount_string_to_decimal(self.amount))

    @property
    def decimal_fees(self):
        return str(self.amount_string_to_decimal(self.paypal_fees))

    def amount_string_to_decimal(self, amount):
        # $50.00 will be represented as "5000" in the report since different locales will use different decimal place
        # separators etc. We do two things here:
        # 1) Ensure the decimal value uses 2 fractional decimal places by appending the string ".00" to the input string
        # 2) Divide by 100 to get the dollar value amount
        # The sequence looks like: "5000" -> "5000.00" -> "50.00"
        # By convention, refunds have a negative dollar value.
        decimal_amount = Decimal(amount + '.00') / Decimal(100)
        if self.transaction_type == 'refund':
            decimal_amount = decimal_amount * Decimal(-1)

        return decimal_amount


class PaypalTransactionsByDayTask(PaypalTaskMixin, luigi.Task):

    account_id = luigi.Parameter(
        default_from_config={'section': 'paypal', 'name': 'account_id'}
    )

    def run(self):
        with self.output().open('w') as output_file:
            for input_target in self.input():
                with input_target.open('r') as input_file:
                    for line in input_file:
                        payment_record = SettlementReportRecord(*line.rstrip('\n').split('\t'))

                        if payment_record.type == 'Sale':
                            transaction_type = 'sale'
                        elif payment_record.type == 'Credit':
                            transaction_type = 'refund'
                        else:
                            raise RuntimeError("Unknown transaction type: {0}".format(payment_record.type))

                        record = [
                            # date
                            payment_record.time.split(' ')[0],
                            # payment gateway
                            'paypal',
                            # payment gateway account ID
                            self.account_id,
                            # payment reference number, this is used to join with orders
                            payment_record.invoice_number,
                            # currency
                            payment_record.currency_symbol,
                            # amount
                            payment_record.decimal_amount,
                            # transaction fee
                            payment_record.decimal_fees,
                            # transaction type - either "sale" or "refund"
                            transaction_type,
                            # payment method - currently this is only "instant_transfer"
                            'instant_transfer',
                            # payment method type - currently this is only ever "paypal"
                            'paypal',
                            # identifier for the transaction
                            payment_record.paypal_transaction_id,
                        ]
                        output_file.write('\t'.join(record) + '\n')

    def requires(self):
        yield RawPaypalTask(
            output_root=self.output_root,
            date=self.date,
            overwrite=self.overwrite,
        )

    def output(self):
        return get_target_from_url(
            url_path_join(self.output_root, 'payments', 'dt=' + self.date.isoformat(), 'paypal.tsv')
        )
