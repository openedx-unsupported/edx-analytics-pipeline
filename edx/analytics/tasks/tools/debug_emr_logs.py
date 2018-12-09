import argparse
import gzip
import os
import re
import shutil
import boto.emr

from subprocess import Popen, PIPE

DEFAULT_REGION = 'us-east-1'


def main():
    arg_parser = argparse.ArgumentParser(description='Get Traceback information from emr-logs.')
    arg_parser.add_argument(
        '-b', '--bucket-name',
        help='Bucket name',
        default=os.environ.get('EMR_LOGS_BUCKET_NAME', None)
    )
    arg_parser.add_argument(
        '-c', '--cluster-name',
        help='Cluster name',
    )
    arg_parser.add_argument(
        '-j', '--job-flow-name',
        help='Job flow name.'
    )
    arg_parser.add_argument(
        '-a', '--application-id',
        default='',
        help='Application ID.'
    )
    arg_parser.add_argument(
        '-o', '--output-path',
        default='./',
        help='Output path for downloaded logs.'
    )
    arg_parser.add_argument(
        '--cleanup',
        action='store_true',
        help='Cleanup logs.'
    )

    args = arg_parser.parse_args()

    if not args.bucket_name:
        exit(arg_parser.print_usage())

    if not args.job_flow_name:
        connection = boto.emr.connect_to_region(DEFAULT_REGION)
        for cluster in connection.list_clusters().clusters:
            if cluster.name == args.cluster_name:
                args.job_flow_name = cluster.id
                break

    s3_emr_logs_url = "s3://{bucket_name}/{job_flow}/containers/{application_id}".format(
        bucket_name=args.bucket_name,
        job_flow=args.job_flow_name,
        application_id=args.application_id,
    )
    return_code = download_emr_logs(s3_emr_logs_url, args.output_path)
    if return_code != 0:
        raise RuntimeError('Unable to download logs from S3.')

    extract_files(args.output_path)

    display_errors(args.output_path)

    if args.cleanup:
        shutil.rmtree(args.output_path)


def download_emr_logs(s3_emr_logs_url, output_path):
    print("Downloading logs.")
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    proc = Popen(
        ['aws', 's3', 'sync', s3_emr_logs_url, output_path, '--exclude', '*', '--include', '*stderr.gz'],
        stdout=PIPE,
    )
    stdout = proc.communicate()[0]
    return proc.returncode


def extract_files(root_path):
    print("Unzipping files.")

    for path, dirs, files in os.walk(root_path):
        for filename in files:
            if filename.endswith('.gz'):
                filename_with_path = os.path.join(path, filename)
                basename = os.path.basename(filename_with_path)
                destination_path = os.path.join(path, basename[:-3])
                with gzip.open(filename_with_path, 'rb') as input_file:
                    with open(destination_path, 'wb') as output_file:
                        for line in input_file:
                            output_file.write(line)


def display_errors(root_path):
    error_text = []

    for path, dirs, files in os.walk(root_path):
        for filename in files:
            if filename == 'stderr':
                filename_with_path = os.path.join(path, filename)
                with open(filename_with_path) as f:
                    data = f.read()
                    for exc in re.findall(r'luigi-exc-hex=[0-9a-f]+', data):
                        error_text.append(filename_with_path)
                        error_text.append(exc.split('=')[-1].decode('hex'))

    print('\n'.join(error_text))

if __name__ == '__main__':
    main()
