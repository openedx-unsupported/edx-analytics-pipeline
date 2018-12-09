# -*- coding: utf-8 -*-
import json
import os
import subprocess
import sys
import uuid
from string import Template  # pylint: disable=deprecated-module

STATIC_FILES_PATH = os.path.join(sys.prefix, 'share', 'edx.analytics.tasks')


def text_report(measurement, file_obj=None, indent=u'', child_indent=u'', threshold_percent=None):
    file_obj = file_obj or sys.stdout
    is_a_tty = hasattr(file_obj, 'isatty') and file_obj.isatty()
    total_time = measurement.time_including_children()
    file_obj.write(
        u'{indent}{time} {percentage:.1f}% {desc}\n'.format(
            indent=indent,
            time=total_time,
            percentage=measurement.percentage_of_total(),
            desc=measurement.description
        )
    )
    visible_children = measurement.sorted_filtered_children(threshold_percent=threshold_percent)
    last_child_index = len(visible_children) - 1
    for index, child in enumerate(visible_children):
        if index < last_child_index:
            c_indent = child_indent + (u'├─ ' if is_a_tty else '|- ')
            cc_indent = child_indent + (u'│  ' if is_a_tty else '|  ')
        else:
            c_indent = child_indent + (u'└─ ' if is_a_tty else '`- ')
            cc_indent = child_indent + u'   '
        text_report(
            child, file_obj=file_obj, indent=c_indent, child_indent=cc_indent, threshold_percent=threshold_percent)


def json_report(measurement, file_obj=None, pretty=True, threshold_percent=None):
    file_obj = file_obj or sys.stdout
    measurement.to_json(file_obj, pretty=pretty, threshold_percent=threshold_percent)


def html_report(measurement, file_obj=None, threshold_percent=None):
    file_obj = file_obj or sys.stdout
    path_to_template = os.path.join(STATIC_FILES_PATH, 'report.html')
    with open(path_to_template, 'r') as template_file:
        template = Template(template_file.read())

    def visit_measurement(node):
        data = node.serializable()
        serialized = {
            'name': data['description'],
            'value': data['self_time'],
            'percentage': node.percentage_of_total(),
            'time_delta': str(node.time_including_children()),
            'bucket': node.categorize(),
        }
        if len(node.children) > 0:
            serialized['children'] = []
            for child in node.sorted_filtered_children(threshold_percent=threshold_percent):
                serialized['children'].append(visit_measurement(child))

        return serialized

    treemap_data = json.dumps(visit_measurement(measurement))
    file_obj.write(
        template.safe_substitute(
            data=treemap_data,
            call_graph_svg=get_call_graph_svg(measurement, threshold_percent)
        )
    )


def get_call_graph_svg(measurement, threshold_percent=None):

    def generate_dot_entries(node, node_id=None):
        node_id = node_id or generate_node_id()
        dot = []
        bucket = node.categorize()
        if bucket == 'large':
            bg_color = '#ff000026'
        elif bucket == 'medium':
            bg_color = '#ffff0026'
        elif bucket == 'small':
            bg_color = '#00ff0013'
        else:
            bg_color = '#ffffffff'

        text = '{percentage:.1f}%\n{desc}'.format(percentage=node.percentage_of_total(), desc=node.description)
        dot.append(
            '{id} [label="{desc}" fillcolor="{color}" style="filled"];'.format(
                id=node_id,
                desc=text,
                color=bg_color
            )
        )

        for child in node.sorted_filtered_children(threshold_percent=threshold_percent):
            child_id = generate_node_id()
            dot.append('{parent} -> {child};'.format(parent=node_id, child=child_id))
            dot.extend(generate_dot_entries(child, child_id))

        return dot

    content = generate_dot_entries(measurement)
    dot_data = '\n'.join(['digraph calls {'] + ['    ' + l for l in content] + ['}'])

    dot_proc = subprocess.Popen(['dot', '-Tsvg'], stdout=subprocess.PIPE, stdin=subprocess.PIPE)
    return dot_proc.communicate(input=dot_data)[0]


def generate_node_id():
    return 'id' + str(uuid.uuid4()).replace('-', '')
