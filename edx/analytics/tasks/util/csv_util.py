"""
Simple CSV utilities.
"""

import csv
from StringIO import StringIO


class MySQLDumpDialect(csv.Dialect):
    """CSV dialect for files created by mysqldump"""
    delimiter = ','
    doublequote = False
    escapechar = "\\"
    lineterminator = "\n"
    quotechar = "\'"
    quoting = csv.QUOTE_ALL
    skipinitialspace = False
    strict = True


class MySQLPipeDialect(csv.Dialect):
    """
    CSV Dialect for files created by piping the output of a mysql
    command query to a file.
    """
    delimiter = '\t'
    doublequote = False
    escapechar = "\\"
    lineterminator = "\n"
    quotechar = None
    quoting = csv.QUOTE_NONE
    skipinitialspace = False
    strict = True


class MySQLExportDialect(MySQLPipeDialect):
    """
    CSV Dialect for files created by edx-analytics-exporter.
    """
    # Needed to preserve the character sequence, otherwise csv parser would interpret '\n' as 'n'
    escapechar = None


DIALECTS = {
    'mysqldump': MySQLDumpDialect,
    'mysqlpipe': MySQLPipeDialect,
    'mysqlexport': MySQLExportDialect
}

for dialect_name, dialect_class in DIALECTS.iteritems():
    csv.register_dialect(dialect_name, dialect_class)


def parse_line(line, dialect='excel'):
    """Parse one line of CSV in the dialect specified."""
    # csv.reader requires an iterable per row, so we wrap the line in a list
    parsed = csv.reader([line], dialect=dialect).next()

    return parsed


def to_csv_line(row, dialect='excel'):
    """Return a CSV line by joining the values in row in the dialect specified."""
    output = StringIO()
    csv.writer(output, dialect=dialect).writerow(row)

    output.seek(0)
    return output.read().strip()
