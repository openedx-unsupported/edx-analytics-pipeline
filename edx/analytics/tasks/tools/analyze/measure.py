
import datetime
import json
from operator import methodcaller


class Measurement(object):
    """
    Represents a node in a tree of measurements.

    Note that a particular measurement may be the parent for several child measurements. Those child measurements
    further break down the time spent in the parent. The time spent in the parent itself is referred to as "self_time".
    The overall time spent in that activity is (self_time + time spent in all children).

    Timing of multiple parallel processes is not captured here (yet). Although it could be extended to represent those
    types of measurements as well.
    """

    SERIALIZATION_VERSION = 1
    UNACCOUNTED_FOR_TIME_DESC = 'Other'

    def __init__(self, description, self_time=None, parent=None):
        self.description = description
        self.self_time = self_time or datetime.timedelta()
        self.children = []
        self.parent = parent
        self.enable_cache = False

    def add_child(self, child):
        child.parent = self
        self.children.append(child)

    def set_time_from_range(self, end_time, start_time):
        time_including_children = end_time - start_time
        unaccounted_for_time = time_including_children - self.time_from_children()
        if len(self.children) > 0:
            unaccounted_for_child = Measurement(self.UNACCOUNTED_FOR_TIME_DESC, self_time=unaccounted_for_time)
            self.add_child(unaccounted_for_child)
        else:
            self.self_time = unaccounted_for_time

    def time_including_children(self):
        return self.self_time + self.time_from_children()

    def time_from_children(self):
        total = datetime.timedelta()
        for child in self.children:
            total += child.time_including_children()
        return total

    def total_time_of_root(self):
        cur = self
        while cur.parent is not None:
            cur = cur.parent

        return cur.time_including_children()

    def percentage_of_total(self):
        return (self.time_including_children().total_seconds() / self.total_time_of_root().total_seconds()) * 100

    def categorize(self):
        percentage = self.percentage_of_total()
        if percentage > 50:
            return 'large'
        elif percentage > 10:
            return 'medium'
        elif percentage > 5:
            return 'small'
        else:
            return 'tiny'

    def sorted_children(self):
        return sorted(self.children, key=methodcaller('time_including_children'), reverse=True)

    def sorted_filtered_children(self, threshold_percent=None):
        if threshold_percent is None:
            return self.sorted_children()
        else:
            return [c for c in self.sorted_children() if c.percentage_of_total() > threshold_percent]

    def serializable(self, threshold_percent=None):
        serialized = {
            'version': self.SERIALIZATION_VERSION,
            'description': self.description,
            'self_time': self.self_time.total_seconds(),
        }
        filtered_children = self.sorted_filtered_children(threshold_percent=threshold_percent)
        if len(filtered_children) > 0:
            serialized['children'] = [c.serializable() for c in filtered_children]

        return serialized

    @staticmethod
    def from_serialized(serialized):
        root = Measurement(
            description=serialized['description'],
            self_time=datetime.timedelta(seconds=serialized['self_time'])
        )
        for child_ser in serialized.get('children', []):
            root.add_child(Measurement.from_serialized(child_ser))

        return root

    def to_json(self, file_ref, pretty=False, threshold_percent=None):
        serialized = self.serializable(threshold_percent=threshold_percent)
        if pretty:
            kwargs = {
                'indent': 4,
                'separators': (',', ': ')
            }
        else:
            kwargs = {}
        if hasattr(file_ref, 'write'):
            json.dump(serialized, file_ref, **kwargs)
        else:
            with open(file_ref, 'w') as output_file:
                json.dump(serialized, output_file, **kwargs)

    @staticmethod
    def from_json(file_ref):
        if hasattr(file_ref, 'write'):
            serialized = json.load(file_ref)
        else:
            with open(file_ref, 'rb') as input_file:
                serialized = json.load(input_file)

        return Measurement.from_serialized(serialized)

    @staticmethod
    def from_pyinstrument_trace(file_ref):
        import pyinstrument
        profiler = pyinstrument.Profiler()
        profiler.add(file_ref)

        root_frame = profiler.root_frame()

        def visit_frame(frame):
            code_pos = frame.code_position_short
            if code_pos is None:
                code_pos = ''

            index = code_pos.find('site-packages')
            if index > 0:
                code_pos = code_pos[index + (len('site-packages') + 1):]

            measurement = Measurement(
                '{function} {code_pos}'.format(
                    function=frame.function or '',
                    code_pos=code_pos
                ),
                self_time=datetime.timedelta(seconds=frame.self_time)
            )

            for child_frame in frame.children:
                measurement.add_child(visit_frame(child_frame))

            return measurement

        return visit_frame(root_frame)
