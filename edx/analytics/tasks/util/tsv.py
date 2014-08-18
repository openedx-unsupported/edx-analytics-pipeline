"""Helpers for reading TSV files."""

import csv
import pandas


def read_tsv(input_file, names):
    """
    Reads a tab-separated file into a DataFrame.

    Args:
        input_file (str): Path to the input file.
        names (list): The names of the columns in the input file.
    Returns:
        A pandas DataFrame read from the file contents of the file.
    """
    return pandas.read_csv(
        input_file,
        names=names,
        quoting=csv.QUOTE_NONE,
        encoding=None,
        delimiter='\t'
    )
