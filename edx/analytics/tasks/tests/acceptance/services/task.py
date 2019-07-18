
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
        if 'tasks_log_path' in self.config:
            self.log_path = os.path.join(self.config.get('tasks_log_path'), self.identifier)
        else:
            self.log_path = None
        self.logs = {
            'stdout': sys.stdout,
            'stderr': sys.stderr,
            'edx_analytics.log': sys.stdout
        }
        self.default_config_override = task_config_override
        self.is_remote = self.config.get('is_remote', True)

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

            env = dict(os.environ)
            if self.is_remote:
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

                if 'wheel_url' in self.config:
                    command.extend(['--wheel-url', self.config['wheel_url']])

                if 'python_version' in self.config:
                    command.extend(['--python-version', self.config['python_version']])

                command.extend(task_args)
                command.append('--local-scheduler')
            else:
                # run the command in a shell since that's what is done by remote-task.
                # Otherwise values like '"*"' cause problems since they are properly escaped for the "shell" case but
                # malformed when not interpreted by a shell.
                command = [
                    '/bin/bash',
                    '-c',
                    '. ~/.bashrc && {0} {1} --local-scheduler'.format(
                        os.getenv('LAUNCH_TASK', 'launch-task'),
                        ' '.join(task_args))
                ]
                env['LUIGI_CONFIG_PATH'] = temp_config_file.name

            try:
                output = shell.run(command, env=env)
            finally:
                self.write_logs_to_standard_streams()

        return output

    def delete_existing_logs(self):
        if not self.log_path:
            return

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
        if not self.log_path:
            return

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
