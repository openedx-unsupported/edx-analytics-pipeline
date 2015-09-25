#!/usr/bin/env python
"""Execute tasks on a remote EMR cluster."""

import argparse
import json
import os
import pipes
from subprocess import Popen, PIPE
import sys
import uuid


STATIC_FILES_PATH = os.path.join(sys.prefix, 'share', 'edx.analytics.tasks')
EC2_INVENTORY_PATH = os.path.join(STATIC_FILES_PATH, 'ec2.py')

REMOTE_DATA_DIR = '/var/lib/analytics-tasks'
REMOTE_LOG_DIR = '/var/log/analytics-tasks'


def main():
    """Parse arguments and run the remote task."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--job-flow-id', help='EMR job flow to run the task', default=None)
    parser.add_argument('--job-flow-name', help='EMR job flow to run the task', default=None)
    parser.add_argument('--host', help='host:port to run the task on', default=None)
    parser.add_argument('--vagrant-path', help='path to the root directory containing a running vagrant container', default=None)
    parser.add_argument('--branch', help='git branch to checkout before running the task', default='release')
    parser.add_argument('--repo', help='git repository to clone')
    parser.add_argument('--remote-name', help='an identifier for this remote task')
    parser.add_argument('--wait', action='store_true', help='wait for the task to complete')
    parser.add_argument('--verbose', action='store_true', help='display very verbose output')
    parser.add_argument('--log-path', help='download luigi output streams after completing the task', default=None)
    parser.add_argument('--user', help='remote user name to connect as', default=None)
    parser.add_argument('--private-key', help='a private key file to use to connect to the host', default=None)
    parser.add_argument('--override-config', help='config file to use to run the job', default=None)
    parser.add_argument('--secure-config', help='config file in secure config repo to use to run the job', default=None)
    parser.add_argument('--secure-config-branch', help='git branch to checkout to find the secure config file', default=None)
    parser.add_argument('--secure-config-repo', help='git repository to clone to find the secure config file', default=os.getenv('ANALYTICS_SECURE_REPO'))
    parser.add_argument('--shell', help='execute a shell command on the cluster and exit', default=None)
    parser.add_argument('--sudo-user', help='execute the shell command as this user on the cluster', default='hadoop')
    parser.add_argument('--workflow-profiler', choices=['pyinstrument'], help='profiler to run on the launch-task process', default=None)
    parser.add_argument('--wheel-url', help='url of the wheelhouse', default=None)
    parser.add_argument('--skip-setup', action='store_true', help='assumes the environment has already been configured and you can simply run the task')
    arguments, extra_args = parser.parse_known_args()
    arguments.launch_task_arguments = extra_args

    log('Parsed arguments = {0}'.format(arguments))
    log('Running commands from path = {0}'.format(STATIC_FILES_PATH))
    uid = arguments.remote_name or str(uuid.uuid4())
    log('Remote name = {0}'.format(uid))

    if arguments.vagrant_path:
        parse_vagrant_ssh_config(arguments)

    if arguments.host:
        inventory = {}
    else:
        inventory = get_ansible_inventory()

    if arguments.shell:
        return_code = run_remote_shell(inventory, arguments, arguments.shell)
    else:
        return_code = run_task_playbook(inventory, arguments, uid)

    log('Exiting with status = {0}'.format(return_code))
    sys.exit(return_code)


def run_task_playbook(inventory, arguments, uid):
    """
    Execute the ansible playbook that triggers and monitors the remote task execution.

    Args:
        arguments (argparse.Namespace): The arguments that were passed in on the command line.
        uid (str): A unique identifier for this task execution.
    """
    if not arguments.skip_setup:
        extra_vars = convert_args_to_extra_vars(arguments, uid)
        args = ['task.yml', '-e', extra_vars]
        prep_result = run_ansible(tuple(args), arguments, executable='ansible-playbook')
        if prep_result != 0:
            return prep_result

    data_dir = os.path.join(REMOTE_DATA_DIR, uid)
    log_dir = os.path.join(REMOTE_LOG_DIR, uid)
    sudo_user = arguments.sudo_user

    env_vars = {}
    if arguments.workflow_profiler:
        env_vars['WORKFLOW_PROFILER'] = arguments.workflow_profiler
        env_vars['WORKFLOW_PROFILER_PATH'] = log_dir

    env_var_string = ' '.join('{0}={1}'.format(k, v) for k, v in env_vars.iteritems())

    command = 'cd {data_dir}/repo && . $HOME/.bashrc && {env_vars}{bg}{data_dir}/venv/bin/launch-task {task_arguments}{end_bg}'.format(
        env_vars=env_var_string + ' ' if env_var_string else '',
        data_dir=data_dir,
        task_arguments=' '.join(arguments.launch_task_arguments),
        log_dir=log_dir,
        bg='nohup ' if not arguments.wait else '',
        end_bg=' &' if not arguments.wait else '',
        sudo_user=sudo_user,
    )

    result = run_remote_shell(inventory, arguments, command)
    if arguments.wait and arguments.log_path:
        host_group = get_ansible_inventory_host(arguments)
        fetch_arguments = [host_group, '-m', 'fetch']
        if arguments.user:
            fetch_arguments.extend(['-u', arguments.user])
        for filename in ('edx_analytics.log', 'launch-task.trace'):
            module_arguments = 'src={src} dest={dest} flat=yes'.format(
                src=os.path.join(log_dir, filename),
                dest=os.path.join(arguments.log_path, filename)
            )
            run_ansible(fetch_arguments + ['-a', module_arguments], arguments)

    return result


def get_ansible_inventory_host(arguments):
    """Get or creates a hostname for inventory."""
    if arguments.host:
        return 'all'
    else:
        return 'mr_{0}_master'.format(arguments.job_flow_id or arguments.job_flow_name)


def convert_args_to_extra_vars(arguments, uid):
    """
    Generate the set of variables that need to be passed in to ansible since they are expected to be set by the
    playbook.

    Args:
        arguments (argparse.Namespace): The arguments that were passed in on the command line.
        uid (str): A unique identifier for this task execution.
    """
    name = get_ansible_inventory_host(arguments)
    extra_vars = {
        'name': name,
        'branch': arguments.branch,
        'uuid': uid,
        'root_data_dir': REMOTE_DATA_DIR,
        'root_log_dir': REMOTE_LOG_DIR,
    }
    if arguments.repo:
        extra_vars['repo'] = arguments.repo
    if arguments.override_config:
        extra_vars['override_config'] = arguments.override_config
    if arguments.secure_config_repo:
        extra_vars['secure_config_repo'] = arguments.secure_config_repo
    if arguments.secure_config_branch:
        extra_vars['secure_config_branch'] = arguments.secure_config_branch
    if arguments.secure_config:
        extra_vars['secure_config'] = arguments.secure_config
    if arguments.wheel_url:
        extra_vars['install_env'] = {
            'WHEEL_URL': arguments.wheel_url,
            'WHEEL_PYVER': '2.7'
        }
    if arguments.vagrant_path or arguments.host:
        extra_vars['write_luigi_config'] = False
    return json.dumps(extra_vars)


def parse_vagrant_ssh_config(arguments):
    """Runs 'vagrant ssh-config' and parses results to find argument values for host, user, port, etc."""
    log('Connecting to vagrant container in {0}'.format(arguments.vagrant_path))
    command = 'vagrant ssh-config'
    log('Running command = {0}'.format(command))
    with open('/dev/null', 'r+') as devnull:
        proc = Popen(
            command,
            stdin=devnull,
            stdout=PIPE,
            cwd=arguments.vagrant_path,
            shell=True
        )
        stdout = proc.communicate()[0]

    if proc.returncode != 0:
        raise RuntimeError('Unable to determine vagrant connectivity parameters.')

    hostname = '127.0.0.1'
    port = '2222'
    for line in stdout.split('\n'):
        split_line = line.strip().split(' ')
        if len(split_line) != 2:
            continue

        key, value = split_line
        if key == "HostName":
            hostname = value
        elif key == "User":
            arguments.user = value
        elif key == "Port":
            port = value
        elif key == "IdentityFile":
            arguments.private_key = value

    arguments.host = hostname + ':' + port


def get_ansible_inventory():
    """
    Ensure the EC2 inventory cache is cleared before running ansible.

    Otherwise new resources will not be present in the inventory which will cause ansible to fail to connect to them.

    """
    command = [EC2_INVENTORY_PATH, '--refresh-cache']
    log('Running command = {0}'.format(command))
    with open('/dev/null', 'r+') as devnull:
        proc = Popen(
            command,
            stdin=devnull,
            stdout=PIPE,
            cwd=STATIC_FILES_PATH
        )
        stdout = proc.communicate()[0]

    if proc.returncode != 0:
        raise RuntimeError('Unable to refresh ansible inventory cache.')

    return json.loads(stdout)


def run_ansible(args, arguments, executable='ansible'):
    """
    Execute ansible passing in the provided arguments.

    Args:
        args (iterable): A collection of arguments to pass to ansible on the command line.
        verbose (bool): Tell ansible to produce verbose output detailing exactly what commands it is executing.
        executable (str): The executable script to invoke on the command line.  Defaults to "ansible".

    """
    if arguments.host:
        inventory_file_path = arguments.host + ','
    else:
        inventory_file_path = EC2_INVENTORY_PATH
    executable_path = os.path.join(sys.prefix, 'bin', executable)
    command = [executable_path, '-i', inventory_file_path] + list(args)
    if arguments.user:
        command.extend(['-u', arguments.user])
    if arguments.private_key:
        command.extend(['--private-key', arguments.private_key])
    if arguments.verbose:
        command.append('-vvvv')

    env = dict(os.environ)
    env.update({
        # Ansible may be pulling down private git repos on the remote machine.  Forward the local agent so that the
        # remote machine can access any repos this one can. These machines are dynamically created, so we don't know
        # their host key.  In an ideal world we would store the host key at provisioning time, however, that doesn't
        # happen, so just trust we have the right machine.
        'ANSIBLE_SSH_ARGS': '-o ForwardAgent=yes -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no'
    })
    log('Running command = {0}'.format(command))
    with open('/dev/null', 'r+') as devnull:
        proc = Popen(
            command,
            stdin=devnull,
            env=env,
            cwd=STATIC_FILES_PATH
        )
        proc.wait()

    return proc.returncode


def run_remote_shell(inventory, arguments, shell_command):
    """Run a shell command on a hadoop cluster."""
    port = None
    if not arguments.host:
        ansible_group_name = get_ansible_inventory_host(arguments)
        hostname = inventory[ansible_group_name][0]
    else:
        split_host = arguments.host.split(':')
        hostname = split_host[0]
        if len(split_host) > 1:
            port = split_host[1]
    sudo_user = arguments.sudo_user
    if sudo_user:
        shell_command = 'sudo -Hu {0} /bin/bash -c {1}'.format(sudo_user, pipes.quote(shell_command))
    command = [
        'ssh',
        '-tt',
        '-o', 'ForwardAgent=yes',
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'UserKnownHostsFile=/dev/null',
        '-o', 'KbdInteractiveAuthentication=no',
        '-o', 'PasswordAuthentication=no',
        '-o', 'User=' + arguments.user,
        '-o', 'ConnectTimeout=10',
    ]
    if port:
        command.extend(['-p', port])
    if arguments.private_key:
        command.extend(['-i', arguments.private_key])
    command.extend([hostname, shell_command])
    log('Running command = {0}'.format(command))
    proc = Popen(
        command,
        cwd=STATIC_FILES_PATH
    )
    proc.wait()
    return proc.returncode


def log(message):
    """Writes debugging information to stderr."""
    sys.stderr.write(message)
    sys.stderr.write('\n')


if __name__ == '__main__':
    main()
