__author__ = 'johnbaker'

from datetime import date

INTERVAL_START = 2015-04-06
INTERVAL_END   = 2015-04-20


def daterange(d1, d2):
    return (d1 + datetime.timedelta(days=i) for i in range((d2 - d1).days + 1))

for d in daterange(INTERVAL_START, INTERVAL_END):
    print d.strftime('%Y.%m.%d')