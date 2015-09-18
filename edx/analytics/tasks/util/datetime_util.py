"""Utility functions for manipulating datetime information."""
import datetime
import re


def ensure_microseconds(timestamp):
    """
    Make sure timestamp strings contain microseconds, even if zero.

    This is needed after datetime.isoformat() calls, which truncate zero microseconds.
    """
    if '.' not in timestamp:
        return '{datetime}.000000'.format(datetime=timestamp)
    else:
        return timestamp


def add_microseconds(timestamp, microseconds):
    """
    Add given microseconds to a timestamp.

    Input and output are timestamps as ISO format strings.  Microseconds can be negative.
    """
    # First try to parse the timestamp string and do simple math, to avoid
    # the high cost of using strptime to parse in most cases.
    timestamp_base, _period, microsec_base = timestamp.partition('.')
    if not microsec_base:
        microsec_base = '0'
        timestamp = ensure_microseconds(timestamp)
    microsec_int = int(microsec_base) + microseconds
    if microsec_int >= 0 and microsec_int < 1000000:
        return "{}.{}".format(timestamp_base, str(microsec_int).zfill(6))

    # If there's a carry, then just use the datetime library.
    parsed_timestamp = datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f')
    newtimestamp = (parsed_timestamp + datetime.timedelta(microseconds=microseconds)).isoformat()
    return ensure_microseconds(newtimestamp)


def mysql_datetime_to_isoformat(mysql_datetime):
    """
    Convert mysql datetime strings to isoformat standard.

    Mysql outputs strings of the form '2012-07-25 12:26:22.0'.
    Log files use isoformat strings for sorting.
    """
    date_parts = [int(d) for d in re.split(r'[:\-\. ]', mysql_datetime)]
    if len(date_parts) > 6:
        tenths = date_parts[6]
        date_parts[6] = tenths * 100000
    timestamp = datetime.datetime(*date_parts).isoformat()
    return ensure_microseconds(timestamp)


def weekly_date_grouping_key(date_string, interval_end):
    """
    Return date to be used as a grouping key for weekly information.
    """
    last_complete_date = interval_end - datetime.timedelta(days=1)
    last_weekday = last_complete_date.isoweekday()

    split_date = date_string.split('-')
    event_date = datetime.date(int(split_date[0]), int(split_date[1]), int(split_date[2]))
    event_weekday = event_date.isoweekday()

    days_until_end = last_weekday - event_weekday
    if days_until_end < 0:
        days_until_end += 7

    end_of_week_date = event_date + datetime.timedelta(days=days_until_end)
    date_grouping_key = end_of_week_date.isoformat()

    return date_grouping_key
