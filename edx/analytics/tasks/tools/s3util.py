"""Command-line utility for using (and testing) s3 utility methods."""

import argparse
import os

from boto import connect_s3

from edx.analytics.tasks.util.s3_util import generate_s3_sources, get_s3_key, join_as_s3_url


def list_s3_files(source_url, patterns):
    """List remote s3 files that match one of the patterns."""
    s3_conn = connect_s3()
    for bucket, root, path in generate_s3_sources(s3_conn, source_url, patterns):
        source = join_as_s3_url(bucket, root, path)
        src_key = get_s3_key(s3_conn, source)
        print "%10d %s" % (src_key.size if src_key is not None else -1, path)


def get_s3_files(source_url, dest_root, patterns):
    """Copy remote s3 files that match one of the patterns to a local destination."""
    s3_conn = connect_s3()
    for bucket, root, path in generate_s3_sources(s3_conn, source_url, patterns):
        source = join_as_s3_url(bucket, root, path)
        dest_name = path.replace('/', '_')
        destination = os.path.join(dest_root, dest_name)
        src_key = get_s3_key(s3_conn, source)
        if src_key is not None:
            src_key.get_contents_to_filename(destination)
        else:
            print "No key for source " + source


def main():
    """Command-line utility for using (and testing) s3 utility methods."""
    arg_parser = argparse.ArgumentParser(description='Perform s3 utility operations.')
    arg_parser.add_argument(
        'command',
        help='Commands supported by this utility.',
        choices=['ls', 'get'],
    )
    arg_parser.add_argument(
        'input',
        help='Read s3 files from this location in s3.',
    )
    arg_parser.add_argument(
        '-o', '--output',
        help='Write s3 files to this location in the local file system.',
    )
    arg_parser.add_argument(
        '-p', '--patterns',
        help='Apply patterns to match the s3 file paths being fetched.',
        nargs='*',
        default=['*'],
    )
    args = arg_parser.parse_args()
    if args.command == 'ls':
        list_s3_files(args.input, args.patterns)
    elif args.command == 'get':
        get_s3_files(args.input, args.output, args.patterns)


if __name__ == '__main__':
    main()
