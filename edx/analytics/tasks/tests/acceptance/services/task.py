
import ConfigParser
import logging
import os
import sys
import tempfile

from edx.analytics.tasks.tests.acceptance.services import shell

log = logging.getLogger(__name__)


class TaskService(object):

    def __init__(self, config, task_config_override, identifier):
        self.config = config
        self.identifier = identifier
        self.log_path = os.path.join(self.config.get('tasks_log_path'), self.identifier)
        self.logs = {
            'stdout': sys.stdout,
            'stderr': sys.stderr,
            'edx_analytics.log': sys.stdout
        }
        self.default_config_override = task_config_override

    def launch(self, task_args, config_override=None):
        self.delete_existing_logs()

        config_parser = ConfigParser.ConfigParser()
        config_parser.read(os.environ['LUIGI_CONFIG_PATH'])
        self.override_config(config_parser, self.default_config_override)
        if config_override:
            self.override_config(config_parser, config_override)

        with tempfile.NamedTemporaryFile() as temp_config_file:
            config_parser.write(temp_config_file)
            temp_config_file.flush()

            temp_config_file.seek(0)
            log.info('Task Configuration')
            log.info(temp_config_file.read())
            temp_config_file.seek(0)

            command = [
                os.getenv('REMOTE_TASK'),
                '--branch', self.config.get('tasks_branch'),
                '--repo', self.config.get('tasks_repo'),
                '--remote-name', self.identifier,
                '--wait',
                '--log-path', self.log_path,
                '--user', self.config.get('connection_user'),
                '--override-config', temp_config_file.name,
            ]

            if 'job_flow_name' in self.config:
                command.extend(['--job-flow-name', self.config['job_flow_name']])
            elif 'host' in self.config:
                command.extend(['--host', self.config['host']])

            command.extend(task_args)
            command.append('--local-scheduler')

            try:
                output = shell.run(command)
            finally:
                self.write_logs_to_standard_streams()

        return output

    def delete_existing_logs(self):
        for filename in self.logs:
            try:
                os.remove(os.path.join(self.log_path, filename))
            except OSError:
                pass

    def override_config(self, config_parser, overrides):
        for section_name, section in overrides.iteritems():
            if not config_parser.has_section(section_name):
                config_parser.add_section(section_name)

            for key, value in section.iteritems():
                config_parser.set(section_name, key, value)

    def write_logs_to_standard_streams(self):
        for filename, output_file in self.logs.iteritems():
            try:
                with open(os.path.join(self.log_path, filename), 'r') as src_file:
                    while True:
                        transfer_buffer = src_file.read(1024)
                        if transfer_buffer:
                            output_file.write(transfer_buffer)
                        else:
                            break
            except IOError:
                log.exception('Unable to retrieve logged output.')
