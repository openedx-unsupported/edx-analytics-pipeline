"""Analyze log files produced by launch-task"""

import argparse
import datetime
import re
import sys
from collections import namedtuple

from edx.analytics.tasks.tools.analyze.measure import Measurement
from edx.analytics.tasks.tools.analyze.parser import LogFileParser
from edx.analytics.tasks.tools.analyze.report import html_report, json_report, text_report

MESSAGE_START_PATTERN = r'(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) (?P<level>\w+) (?P<pid>\d+) \[(?P<module>.*?)\] (?P<filename>.*?):(?P<line_no>\d+) - (?P<content>.*)'
LogMessage = namedtuple('LogMessage', 'timestamp level pid module filename line_no content')  # pylint: disable=invalid-name


def analyze():
    arg_parser = argparse.ArgumentParser(description='Analyze log files.')
    arg_parser.add_argument(
        '-o', '--output',
        help='Save the results of the analysis to a file that can be read later.'
    )
    arg_parser.add_argument(
        '-r', '--report',
        choices=['text', 'json', 'html'],
        help='Generate a report in the requested format and stream it to stdout.'
    )
    arg_parser.add_argument(
        '--threshold-percent',
        type=float,
        default=None,
        help="Don't include entries that accounted for less than this amount of time relative to the total execution"
             " time in the report."
    )
    group = arg_parser.add_argument_group('Input File')
    group_ex = group.add_mutually_exclusive_group()
    group_ex.add_argument(
        '-l', '--log',
        default='edx_analytics.log',
        help='A log file to analyze. Defaults to "edx_analytics.log" in the current directory.'
    )
    group_ex.add_argument(
        '-i', '--input',
        help='Reads a previously saved result file.'
    )
    group_ex.add_argument(
        '-t', '--trace',
        help='Path to an execution trace of the launch-task process captured by pyinstrument and saved as JSON.'
    )

    args = arg_parser.parse_args()

    if args.input:
        root = Measurement.from_json(args.input)
    elif args.trace:
        root = Measurement.from_pyinstrument_trace(args.trace)
    else:
        root = analyze_log_file(args.log)

    if args.output:
        json_report(root, args.output, pretty=False, threshold_percent=args.threshold_percent)

    if args.report == 'json':
        json_report(root, threshold_percent=args.threshold_percent)
    elif args.report == 'text':
        text_report(root, threshold_percent=args.threshold_percent)
    elif args.report == 'html':
        html_report(root, threshold_percent=args.threshold_percent)


def analyze_log_file(filename):
    with open(filename, 'rb') as log_file:
        parser = LogFileParser(log_file, message_pattern=MESSAGE_START_PATTERN, message_factory=create_log_message)
        try:
            return analyze_log(parser)
        except Exception:
            sys.stderr.write('Exception on line {0}\n'.format(parser.line_number))
            raise


def create_log_message(matched_groups):
    timestamp_str = matched_groups['timestamp']
    matched_groups['timestamp'] = datetime.datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S,%f')
    for field_name in ('pid', 'line_no'):
        matched_groups[field_name] = int(matched_groups[field_name])

    return LogMessage(**matched_groups)


def analyze_log(parser):
    root = Measurement('Luigi Worker')
    scheduling_time = analyze_overall_scheduling(parser)
    root.add_child(scheduling_time)
    execution_time = analyze_overall_execution(parser)
    root.add_child(execution_time)
    return root


def analyze_overall_scheduling(parser):
    all_scheduling = Measurement('Scheduling Tasks')

    start_scheduling_pattern = r'Checking if (?P<task_id>.*?) is complete'
    message = True
    start_scheduling_timestamp = None

    while message:
        message = parser.next_message()

        if message.content == 'Done scheduling tasks':
            all_scheduling.set_time_from_range(message.timestamp, start_scheduling_timestamp)
            return all_scheduling

        start_match = re.match(start_scheduling_pattern, message.content, (re.MULTILINE | re.DOTALL))
        if start_match:
            if start_scheduling_timestamp is None:
                start_scheduling_timestamp = message.timestamp

            measurement = analyze_task_scheduling(message, start_match, parser)
            if measurement:
                all_scheduling.add_child(measurement)


def analyze_task_scheduling(start_message, start_match, parser):
    scheduled_pattern = r'Scheduled (?P<task_id>.*?) \((?P<status>\w+)\)'

    task = LuigiTaskDescription.from_string(start_match.group('task_id'))
    start_timestamp = start_message.timestamp
    message = start_message
    while message:
        message = parser.peek_message()

        if message.content == 'Done scheduling tasks':
            break

        if re.match(scheduled_pattern, message.content, (re.MULTILINE | re.DOTALL)):
            break

        parser.next_message()

    if not task.name == 'UncheckedExternalURL':
        return Measurement('Scheduling {}'.format(task), message.timestamp - start_timestamp)
    else:
        return None


def analyze_overall_execution(parser):
    all_execution = Measurement('Executing Tasks')

    pattern = r'.*? Worker Worker.* (?P<state>running|done|failed)\s+(?P<task_id>.*)'
    message = True
    overall_start_timestamp = None
    running_measurement = None

    while message:
        message = parser.next_message()

        if message.content == 'Done':
            if overall_start_timestamp:
                all_execution.set_time_from_range(message.timestamp, overall_start_timestamp)
            return all_execution

        match = re.match(pattern, message.content, (re.MULTILINE | re.DOTALL))
        if not match:
            if 'Running job:' in message.content or 'Starting Job =' in message.content:
                for measurement in analyze_hadoop_job(message, parser):
                    running_measurement.add_child(measurement)

            continue

        task = LuigiTaskDescription.from_string(match.group('task_id'))
        state = match.group('state')
        if state == 'running':
            start_timestamp = message.timestamp
            running_measurement = Measurement('Executing {}'.format(task))
            if not overall_start_timestamp:
                overall_start_timestamp = start_timestamp
        else:
            running_measurement.set_time_from_range(message.timestamp, start_timestamp)
            all_execution.add_child(running_measurement)


def analyze_hadoop_job(starting_message, parser):
    match = re.match(r'.*?(?P<job_id>job_\d{12}\d?_\d{4})', starting_message.content)
    job_id = match.group('job_id')
    start_timestamp = starting_message.timestamp

    message = starting_message
    while message:
        message = parser.next_message()

        job_complete = ('Job complete:' in message.content or 'completed successfully' in message.content)
        if job_complete or 'Ended Job = ' in message.content:
            if job_complete:
                move_measure = analyze_output_move(parser)
                if move_measure:
                    yield move_measure

            yield Measurement('Hadoop ' + job_id, message.timestamp - start_timestamp)
            return


def analyze_output_move(parser):
    output_message = parser.peek_message()
    if re.match(r'.*?Output:.*-temp-', output_message.content):
        start_timestamp = output_message.timestamp
        parser.next_message()
        next_message = parser.peek_message()
        return Measurement('Moving Job Output', next_message.timestamp - start_timestamp)


class LuigiTaskDescription(object):

    PATTERN = r'(?P<name>\w+)\((?P<params>.*)\)'

    def __init__(self, name, params=None):
        self.name = name
        self.params = params or {}

    def __str__(self):
        param_string = ', '.join(['='.join((k, str(v)[:100])) for k, v in self.params.iteritems()])
        return '{name}{params}'.format(
            name=self.name,
            params='(' + param_string + ')' if len(self.params) > 0 else ''
        )

    @staticmethod
    def from_string(id_str):
        match = re.match(LuigiTaskDescription.PATTERN, id_str, (re.MULTILINE | re.DOTALL))
        if match:
            task_name = match.group('name')
            raw_params = match.group('params')
            param_parser = default_parameter_parser
            if task_name == 'HiveTableFromQueryTask':
                param_parser = hive_parameter_parser
            if task_name == 'SqoopImportFromMysql':
                param_parser = sqoop_parameter_parser
            return LuigiTaskDescription(task_name, param_parser(raw_params))
        else:
            raise ValueError('Unable to parse task id "%s"', id_str)


def default_parameter_parser(_raw_params):
    return {}


def hive_parameter_parser(raw_params):
    table_param_match = re.search(r'table=(?P<name>[\w_]+)', raw_params)
    if table_param_match:
        return {'table': table_param_match.group('name')}


def sqoop_parameter_parser(raw_params):
    table_param_match = re.search(r'table_name=(?P<name>[\w_]+)', raw_params)
    if table_param_match:
        return {'table': table_param_match.group('name')}


if __name__ == '__main__':
    analyze()
