
import re


class LogFileParser(object):

    def __init__(self, log_file_obj, message_pattern, message_factory=dict, content_group_name='content'):
        self.log_file = log_file_obj
        self.message_pattern = message_pattern
        self.message_factory = message_factory
        self.content_group_name = content_group_name
        self.line_number = 0
        self.messages = self.parse_messages()

    def parse_messages(self):
        message = self.read_line()
        while message:
            message_match = re.match(self.message_pattern, message)
            if not message_match:
                raise ValueError('Unable to parse message "%s"', message)

            matched_groups = dict(message_match.groupdict())
            first_line_content = matched_groups.get(self.content_group_name, '')
            matched_groups[self.content_group_name] = self.read_content(first_line_content)

            yield self.message_factory(matched_groups)

            message = self.read_line()

    def read_content(self, first_line_content):
        content = first_line_content
        next_message_match = None
        while not next_message_match:
            next_line = self.peek_line()
            if not next_line:
                break
            next_message_match = re.match(self.message_pattern, next_line)
            if not next_message_match:
                self.read_line()
                content += next_line

        return content

    def peek_line(self):
        pos = self.log_file.tell()
        line = self.log_file.readline()
        self.log_file.seek(pos)
        return line

    def read_line(self):
        line = self.log_file.readline()
        if line:
            self.line_number += 1
        return line

    def next_message(self):
        try:
            return next(self.messages)
        except StopIteration:
            return None

    def peek_message(self):
        pos = self.log_file.tell()
        message = self.next_message()
        self.log_file.seek(pos)
        return message
