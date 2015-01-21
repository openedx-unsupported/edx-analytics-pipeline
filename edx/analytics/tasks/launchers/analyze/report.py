
import json
import os
import sys
from string import Template


def text_report(measurement, file_obj=None, indent=u'', child_indent=u''):
    file_obj = file_obj or sys.stdout
    total_time = measurement.time_including_children()
    file_obj.write(
        '{indent}{time} {percentage:.1f}% {desc}\n'.format(
            indent=indent,
            time=total_time,
            percentage=measurement.percentage_of_total(),
            desc=measurement.description
        )
    )
    for child in measurement.sorted_children:
        text_report(child, file_obj, indent + '   ')


def json_report(measurement, file_obj=None, pretty=True):
    file_obj = file_obj or sys.stdout
    measurement.to_json(file_obj, pretty=pretty)


def html_report(measurement, file_obj=None):
    file_obj = file_obj or sys.stdout
    path_to_template = os.path.join(os.path.dirname(__file__), 'resources', 'report.html')
    with open(path_to_template, 'r') as template_file:
        template = Template(template_file.read())

    def visit_measurement(node):
        data = node.serializable()
        serialized = {
            'name': data['description'],
            'value': data['self_time'],
            'percentage': node.percentage_of_total(),
            'time_delta': str(node.time_including_children())
        }
        if len(node.children) > 0:
            serialized['children'] = []
            for child in node.sorted_children:
                serialized['children'].append(visit_measurement(child))

        return serialized

    treemap_data = json.dumps(visit_measurement(measurement))
    file_obj.write(template.safe_substitute(data=treemap_data))
