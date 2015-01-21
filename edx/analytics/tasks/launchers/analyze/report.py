
import sys


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
