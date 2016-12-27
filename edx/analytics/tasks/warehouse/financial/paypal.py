"""
This module uses the PayPal Payflow Gateway Reporting API to gather transaction data from PayPal. Note that we attempted
to make use of the REST API, but were unable to find a viable solution using that product. Here is the general
documentation for the API:

https://developer.paypal.com/docs/classic/payflow/reporting/
"""

import datetime
import xml.etree.cElementTree as ET
from cStringIO import StringIO
import logging
from collections import namedtuple, OrderedDict
from decimal import Decimal
import time

import luigi
from luigi import date_interval
from luigi.configuration import get_config
import requests

from edx.analytics.tasks.url import get_target_from_url
from edx.analytics.tasks.url import url_path_join
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.hive import WarehouseMixin

log = logging.getLogger(__name__)


class PaypalApiResponse(object):
    """
    A generic API response. All API responses include the "baseResponse" element which is parsed by this class so any
    children overriding `params_from_xml` are expected to call this class.

    Subclasses are expected to take in the response_code and response_message in their constructor and pass them up to
    the constructor of this class.

    Args:
        response_code: The numeric status code for the request, this indicates the overall request status.
        response_message: A human readable description of the status of the request.
    """

    SUCCESSFUL_RESPONSE_CODE = 100

    def __init__(self, response_code, response_message):
        # NOTE: subclasses overriding this method should also include these parameters in their signature since the
        # constructor call is generated dynamically during the XML parsing process.
        self.response_code = response_code
        self.response_message = response_message

    @classmethod
    def from_http_response(cls, response):
        """
        Parse an HTTP response from the API. Raises errors if the response is not usable. Returns the populated response
        object if it is.

        Args:
            response: a requests.HTTPResponse object

        Raises:
            HTTPError: a requests.exceptions.HTTPError object if the API returned an HTTP status code other than 200 OK.
            PaypalError: If either the response was malformed or Paypal indicated that it could not process the
                request for some reason.
        """
        response.raise_for_status()
        response_root = ET.fromstring(response.content)
        response_obj = cls.from_xml(response_root)
        response_obj.raise_for_status()
        return response_obj

    @classmethod
    def params_from_xml(cls, _root_node):
        """
        Subclasses are expected to override this method to parse fields out of the XML document to be passed to their
        constructor.

        Args:
            root_node: an ElementTree.Element object representing the root node of the XML response.

        Returns:
            A dictionary mapping constructor arguments to the values extracted from the XML document.
        """
        return {}

    @classmethod
    def from_xml(cls, root_node):
        """
        Parse the status of the request from the response. This request status should be included in all responses.

        Args:
            root_node: an ElementTree.Element object representing the root node of the XML response.

        Returns:
            A valid response object.
        """
        base_response_node = find_or_raise(root_node, 'baseResponse')
        params = {
            'response_code': int(find_text_or_raise(base_response_node, 'responseCode')),
            'response_message': find_text_or_raise(base_response_node, 'responseMsg')
        }
        params.update(cls.params_from_xml(root_node))
        return cls(**params)

    def raise_for_status(self):
        """Raise an error if the request failed or the response is malformed."""
        if self.response_code != self.SUCCESSFUL_RESPONSE_CODE:
            raise PaypalApiRequestFailedError(self.response_code, self.response_message)


def find_or_raise(node, child_name):
    """
    Find the direct descendant of `node` with the tag `child_name`. If the no child is found with that tag, an error is
    raised.

    Args:
        node: an ElementTree.Element object representing a node in an XML document that is expected to have a child
            with the name specified by the `child_name` parameter.
        child_name: a string containing the name of the tag that is expected to be a direct descendant of `node`.

    Raises:
        PaypalMalformedResponseError: If the document does not have the expected structure.

    Returns:
        A reference to the child node when it is found.
    """
    child = node.find(child_name)
    if child is None:
        raise PaypalMalformedResponseError(
            'The required element "{}" was not found in the API response'.format(child_name), node
        )
    return child


def find_text_or_raise(node, child_name):
    """
    Extract the text from the direct descendant of `node` with the tag `child_name`. If the no child is found with that
    tag, an error is raised.

    Args:
        node: an ElementTree.Element object representing a node in an XML document that is expected to have a child
            with the name specified by the `child_name` parameter.
        child_name: a string containing the name of the tag that is expected to be a direct descendant of `node`.

    Raises:
        PaypalMalformedResponseError: If the document does not have the expected structure.

    Returns:
        The text contents of the child when it is found.
    """
    text = node.findtext(child_name)
    if text is None:
        raise PaypalMalformedResponseError(
            'The required element "{}" was not found in the API response'.format(child_name), node
        )
    return text


class PaypalError(Exception):
    """A base class for Paypal API related exceptions."""
    pass


class PaypalApiRequestFailedError(PaypalError):
    """Paypal responded with an error code specifying a generic failure with processing the request."""

    def __init__(self, response_code, response_message, request_type="API"):
        super(PaypalApiRequestFailedError, self).__init__(
            'Paypal {request_type} request failed with code {code}: {message}'.format(
                request_type=request_type,
                code=response_code,
                message=response_message
            )
        )


class PaypalMalformedResponseError(PaypalError):
    """The response from paypal did not match the expected format."""

    def __init__(self, message, root_node=None):
        with_tree = message
        if root_node:
            with_tree = message + ':' + ET.tostring(root_node, encoding='UTF-8', method='xml')

        super(PaypalMalformedResponseError, self).__init__(with_tree)


class PaypalApiRequest(object):
    """
    A generic API request. Subclasses are expected to override `append_request_node` to specify the details of the
    particular request in question.

    A particular request expects a particular type of response. The class used to parse the response is specified by
    overriding the `RESPONSE_CLASS` variable.
    """

    # WARNING: the XML parser used by the paypal service will not recognize fields in a different order than the one
    # specified below. DO NOT CHANGE the ordering of fields within requests! They could have avoided this by using
    # <xsd:all> instead of <xsd:sequence> for their types that shouldn't care about the ordering of fields (many of
    # their complex types fall into this category).

    RESPONSE_CLASS = PaypalApiResponse

    def __init__(self):
        configuration = get_config()
        self.partner = configuration.get('paypal', 'partner', 'PayPal')
        self.vendor = configuration.get('paypal', 'vendor')
        self.password = configuration.get('paypal', 'password')
        self.user = configuration.get('paypal', 'user', None)
        self.url = configuration.get('paypal', 'url')

    def create_request_document(self):
        """Get the string representation of the XML request."""
        root_node = ET.Element('reportingEngineRequest')
        self.append_authentication_node(root_node)
        self.append_request_node(root_node)

        # NOTE: we have to use this API to get the XML declaration, it is suboptimal that we have to construct a
        # StringIO buffer to write to.
        tree = ET.ElementTree(root_node)
        string_buffer = StringIO()
        tree.write(string_buffer, encoding='UTF-8', xml_declaration=True)
        return string_buffer.getvalue()

    def append_authentication_node(self, root_node):
        """Inject the authentication elements into the request."""
        auth_node = ET.SubElement(root_node, 'authRequest')

        for attribute in ('user', 'vendor', 'partner', 'password'):
            child_node = ET.SubElement(auth_node, attribute)
            child_node.text = unicode(getattr(self, attribute))

    def append_request_node(self, root_node):
        """Inject the request-specific elements into the request."""
        pass

    def execute(self):
        """
        Execute the request and return the parsed response object.

        Returns:
            A subclass of PaypalApiResponse
        """
        request_document = self.create_request_document()
        headers = {
            'Content-Type': 'text/plain'
        }
        response = requests.post(
            self.url,
            data=request_document,
            headers=headers,
        )
        return self.RESPONSE_CLASS.from_http_response(response)


class PaypalReportResponse(PaypalApiResponse):
    """
    A response that details the status of a reporting request. This is used to parse responses from both the
    PaypalReportRequest and PaypalReportResultsRequest request types since they are very similar and they are
    interpreted the same way.

    Args:
        response_code: The numeric status code for the request, this indicates the overall request status.
        response_message: A human readable description of the status of the request.
        report_id: A unique report identifier that can be used to identify the report when making subsequent requests.
        status_code: The numeric status code for the report generation request. Note that this code differs from the
            response_code and refers to the report status, not the request status.
    """

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
            raise PaypalMalformedResponseError(
                'Unable to find a valid response body, must be either "runReportResponse" or "getResultsResponse".',
                root_node
            )

        params = {}
        params['report_id'] = find_text_or_raise(maybe_report_node, 'reportId')
        params['status_code'] = int(find_text_or_raise(maybe_report_node, 'statusCode'))
        params['status_message'] = find_text_or_raise(maybe_report_node, 'statusMsg')
        return params

    @property
    def is_running(self):
        """Returns True iff the report is still running on the backend."""
        return self.status_code in (self.REPORT_CREATED_STATUS_CODE, self.REPORT_EXECUTING_STATUS_CODE)

    @property
    def is_ready(self):
        """Returns True iff the report completed successfully and data can be retrieved."""
        return self.status_code == self.REPORT_COMPLETE_STATUS_CODE

    def raise_for_status(self):
        """Raise an error if the report failed to execute on the backend."""
        super(PaypalReportResponse, self).raise_for_status()
        if self.status_code > self.REPORT_COMPLETE_STATUS_CODE:
            raise PaypalApiRequestFailedError(self.status_code, self.status_message, "report")


class PaypalReportRequest(PaypalApiRequest):
    """
    Request a report. The different types of reports require different parameters, which are passed into the constructor
    as keyword arguments.

    Args:
        report_name: The name of the report to generate.
        report_params: The parameters to pass along to the backend. This is commonly a date range, but may include other
            types of parameters as well.
    """

    DEFAULT_PAGE_SIZE = 50
    RESPONSE_CLASS = PaypalReportResponse

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


ColumnMetadata = namedtuple('ColumnMetadata', ('name', 'data_type'))  # pylint: disable=invalid-name


class PaypalReportMetadataResponse(PaypalApiResponse):
    """
    A response that details the format and size of the generated report.

    Args:
        response_code: The numeric status code for the request, this indicates the overall request status.
        response_message: A human readable description of the status of the request.
        num_rows: The number of rows in the report.
        num_pages: The number of pages of data that can be requested.
        page_size: The number of rows per page of data.
        columns: An OrderedDict mapping column names to ColumnMetadata objects.
    """

    def __init__(self, response_code, response_message, num_rows, num_pages, page_size, columns):
        super(PaypalReportMetadataResponse, self).__init__(response_code, response_message)
        self.num_rows = num_rows
        self.num_pages = num_pages
        self.page_size = page_size
        self.columns = columns

    @classmethod
    def params_from_xml(cls, root_node):
        node = find_or_raise(root_node, 'getMetaDataResponse')

        params = {}
        params['num_rows'] = int(find_text_or_raise(node, 'numberOfRows'))
        params['num_pages'] = int(find_text_or_raise(node, 'numberOfPages'))
        params['page_size'] = int(find_text_or_raise(node, 'pageSize'))

        columns = OrderedDict()
        for column_node in node.iterfind('columnMetaData'):
            name = find_text_or_raise(column_node, 'dataName')
            data_type = find_text_or_raise(column_node, 'dataType')
            columns[name] = ColumnMetadata(name=name, data_type=data_type)
        params['columns'] = columns

        return params


class PaypalReportMetadataRequest(PaypalApiRequest):
    """
    A request for the format and size of a particular report.

    Args:
        report_id: The report identifier.
    """

    RESPONSE_CLASS = PaypalReportMetadataResponse

    def __init__(self, report_id):
        super(PaypalReportMetadataRequest, self).__init__()
        self.report_id = report_id

    def append_request_node(self, root_node):
        request_node = ET.SubElement(root_node, 'getMetaDataRequest')
        report_id_node = ET.SubElement(request_node, 'reportId')
        report_id_node.text = unicode(self.report_id)


class PaypalReportDataResponse(PaypalApiResponse):
    """
    A page of data.

    Args:
        response_code: The numeric status code for the request, this indicates the overall request status.
        response_message: A human readable description of the status of the request.
        rows: A 2-dimensional array that represents the data table.
    """

    def __init__(self, response_code, response_message, rows):
        super(PaypalReportDataResponse, self).__init__(response_code, response_message)
        self.rows = rows

    @classmethod
    def params_from_xml(cls, root_node):
        node = find_or_raise(root_node, 'getDataResponse')

        params = {}
        rows = []
        for row_node in node.iterfind('reportDataRow'):
            row = []
            for column_node in row_node.iterfind('columnData'):
                # NOTE: data types are not used to parse the text data here!
                row.append(find_text_or_raise(column_node, 'data'))

            rows.append(row)

        params['rows'] = rows
        return params


class PaypalReportDataRequest(PaypalApiRequest):
    """
    Request a page of data.

    Args:
        report_id: The report identifier.
        page_num: The page of data to get.
    """

    RESPONSE_CLASS = PaypalReportDataResponse

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


class PaypalReportResultsRequest(PaypalApiRequest):
    """
    Request the status of a report.

    Args:
        report_id: The report identifier.
    """

    RESPONSE_CLASS = PaypalReportResponse

    def __init__(self, report_id):
        super(PaypalReportResultsRequest, self).__init__()
        self.report_id = report_id

    def append_request_node(self, root_node):
        request_node = ET.SubElement(root_node, 'getResultsRequest')
        report_id_node = ET.SubElement(request_node, 'reportId')
        report_id_node.text = unicode(self.report_id)


BaseSettlementReportRecord = namedtuple('SettlementReportRecord', [  # pylint: disable=invalid-name
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
    """A record in the settlement report."""
    # pylint: disable=no-member

    @property
    def transaction_type(self):
        """Internal transaction type code from the paypal transaction type code"""
        if self.type == 'Sale':
            return 'sale'
        elif self.type == 'Credit':
            return 'refund'
        else:
            raise TypeError("Unknown transaction type: {0}".format(self.type))

    @property
    def decimal_amount(self):
        """A string representing the amount of currency as a floating point number"""
        return str(self.amount_string_to_decimal(self.amount))

    @property
    def decimal_fees(self):
        """A string representing the amount of currency of the fee as a floating point number"""
        return str(self.amount_string_to_decimal(self.paypal_fees))

    def amount_string_to_decimal(self, amount):
        """
        Paypal represents currency as whole numbers. $50.00 will be represented as "5000" in the report since different
        locales will use different decimal place separators etc. We do two things here:

        1) Ensure the decimal value uses 2 fractional decimal places by appending the string ".00" to the input string
        2) Divide by 100 to get the dollar value amount

        The sequence looks like: "5000" -> "5000.00" -> "50.00"

        By convention, refunds have a negative dollar value.
        """
        decimal_amount = Decimal(amount + '.00') / Decimal(100)
        if self.transaction_type == 'refund':
            decimal_amount = decimal_amount * Decimal(-1)

        return decimal_amount


class PaypalTaskMixin(OverwriteOutputMixin):
    """The parameters needed to run the paypal reports."""

    output_root = luigi.Parameter(
        description='The parent folder to write the paypal transaction data to.',
    )
    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        description='The date to generate a report for. Default is today, UTC.',
    )
    account_id = luigi.Parameter(
        default_from_config={'section': 'paypal', 'name': 'account_id'},
        description='A human readable name for the paypal account data is being gathered for.',
    )


class PaypalTransactionsByDayTask(PaypalTaskMixin, luigi.Task):
    """
    Run a report, gather the data for the report and write it to the output as a TSV file.

    """
    # pylint: disable=no-member

    def run(self):
        self.remove_output_on_overwrite()

        ppr = PaypalReportRequest(
            'SettlementReport',
            processor='PayPal',
            start_date=self.date.isoformat() + ' 00:00:00',
            end_date=self.date.isoformat() + ' 23:59:59',
            timezone='GMT'
        )
        report_response = ppr.execute()
        report_id = report_response.report_id

        is_running = report_response.is_running
        timeout = get_config().getint('paypal', 'timeout', 60 * 60 * 2)
        start_time = time.time()
        while is_running:
            if timeout >= 0 and time.time() >= (start_time + timeout):
                raise PaypalTimeoutError(start_time)
            time.sleep(5)
            results_response = PaypalReportResultsRequest(report_id=report_id).execute()
            is_running = results_response.is_running

        metadata_response = PaypalReportMetadataRequest(report_id=report_id).execute()

        with self.output().open('w') as output_tsv_file:
            for page_num in range(metadata_response.num_pages):
                data_response = PaypalReportDataRequest(report_id=report_id, page_num=(page_num + 1)).execute()
                for row in data_response.rows:
                    self.write_transaction_record(row, output_tsv_file)

    def write_transaction_record(self, row, output_tsv_file):
        """
        Given a raw row of data, transform it into the appropriate output format and write it to the output file.

        Args:
            row: An array of strings representing the data in each column of the report for this row.
            output_tsv_file: A file-like object that can be used to write the TSV data to.
        """
        payment_record = SettlementReportRecord(*row)

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
            payment_record.transaction_type,
            # payment method - currently this is only "instant_transfer"
            'instant_transfer',
            # payment method type - currently this is only ever "paypal"
            'paypal',
            # identifier for the transaction
            payment_record.paypal_transaction_id,
        ]
        output_tsv_file.write('\t'.join(record) + '\n')

    def output(self):
        # NOTE: both the cybersource and paypal tasks write to the payments folder
        return get_target_from_url(
            url_path_join(self.output_root, 'payments', 'dt=' + self.date.isoformat(), 'paypal.tsv')
        )


class PaypalTimeoutError(PaypalError):
    """The requested report did not finish generating in time."""

    def __init__(self, start_time):
        super(PaypalTimeoutError, self).__init__(
            "Aborting since the report did not finish generating within the acceptable time range. Started generation"
            " at {start_time}.".format(
                start_time=start_time
            )
        )


class PaypalTransactionsIntervalTask(PaypalTaskMixin, WarehouseMixin, luigi.WrapperTask):
    """Generate paypal transaction reports for each day in an interval."""

    date = None
    interval = luigi.DateIntervalParameter(default=None)
    interval_start = luigi.DateParameter(
        default_from_config={'section': 'paypal', 'name': 'interval_start'},
        significant=False,
    )
    interval_end = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        significant=False,
        description='Default is today, UTC.',
    )
    output_root = luigi.Parameter(
        default=None,
        description='URL of location to write output.',
    )

    def __init__(self, *args, **kwargs):
        super(PaypalTransactionsIntervalTask, self).__init__(*args, **kwargs)
        # Provide default for output_root at this level.
        if self.output_root is None:
            self.output_root = self.warehouse_path

        if self.interval is None:
            self.interval = date_interval.Custom(self.interval_start, self.interval_end)

    def requires(self):
        for day in self.interval:
            yield PaypalTransactionsByDayTask(
                account_id=self.account_id,
                output_root=self.output_root,
                date=day,
                overwrite=self.overwrite,
            )

    def output(self):
        return [task.output() for task in self.requires()]
