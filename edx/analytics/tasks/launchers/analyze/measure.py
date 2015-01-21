
import datetime
import json
from operator import methodcaller


class Measurement(object):

    SERIALIZATION_VERSION = 1
    UNACCOUNTED_FOR_TIME_DESC = 'Other'

    def __init__(self, description, self_time=None, parent=None):
        self.description = description
        self.self_time = self_time or datetime.timedelta()
        self.children = []
        self.parent = None

    def add_child(self, child):
        child.parent = self
        self.children.append(child)

    def time_including_children(self):
        return self.self_time + self.time_from_children()

    def time_from_children(self):
        total = datetime.timedelta()
        for c in self.children:
            total += c.time_including_children()
        return total

    def set_time_from_range(self, end_time, start_time):
        time_including_children = end_time - start_time
        unaccounted_for_time = time_including_children - self.time_from_children()
        if len(self.children) > 0:
            unaccounted_for_child = Measurement(self.UNACCOUNTED_FOR_TIME_DESC, self_time=unaccounted_for_time)
            self.add_child(unaccounted_for_child)
        else:
            self.self_time = unaccounted_for_time

    def total_time_of_root(self):
        cur = self
        while cur.parent is not None:
            cur = cur.parent

        return cur.time_including_children()

    def percentage_of_total(self):
        return (self.time_including_children().total_seconds() / self.total_time_of_root().total_seconds()) * 100

    def is_unaccounted_for_time(self):
        return self.description == self.UNACCOUNTED_FOR_TIME_DESC

    @property
    def sorted_children(self):
        return sorted(self.children, key=methodcaller('time_including_children'), reverse=True)

    def __str__(self):
        return 'Measurement<{desc}, {elapsed}>{children}'.format(
            desc=self.description,
            elapsed=self.self_time,
            children=' [' + ', '.join([str(c) for c in self.children]) + ']' if len(self.children) > 0 else ''
        )

    def serializable(self):
        serialized = {
            'version': self.SERIALIZATION_VERSION,
            'description': self.description,
            'self_time': self.self_time.total_seconds(),
        }
        if len(self.children) > 0:
            serialized['children'] = [c.serializable() for c in self.children]

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

    def to_json(self, file_ref, pretty=False):
        serialized = self.serializable()
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
