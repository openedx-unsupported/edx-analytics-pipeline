
try:
    from isoweek import Week
except ImportError:
    # This will be imported on slave nodes even though they don't actually have the package installed. The module is
    # is never used in those contexts, so we can ignore the issue.
    Week = NotImplemented
