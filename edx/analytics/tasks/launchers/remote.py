#!/usr/bin/env python
"""Execute tasks on a remote EMR cluster."""

import argparse
import json
import os
import pipes
import sys
import uuid
from subprocess import PIPE, Popen
from urlparse import parse_qsl, urlparse

STATIC_FILES_PATH = os.path.join(sys.prefix, 'share', 'edx.analytics.tasks')
EC2_INVENTORY_PATH = os.path.join(STATIC_FILES_PATH, 'ec2.py')
ANSIBLE_MAX_RETRY = 3

REMOTE_DATA_DIR = '/var/lib/analytics-tasks'
REMOTE_LOG_DIR = '/var/log/analytics-tasks'

REMOTE_CONFIG_DIR_BASE = 'config'
REMOTE_CODE_DIR_BASE = 'repo'


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
    parser.add_argument('--secure-config', help='config file in secure config repo to use to run the job', default=None, action='append')
    parser.add_argument('--secure-config-branch', help='git branch to checkout to find the secure config file', default=None)
    parser.add_argument('--secure-config-repo', help='git repository to clone to find the secure config file', default=os.getenv('ANALYTICS_SECURE_REPO'))
    parser.add_argument('--shell', help='execute a shell command on the cluster and exit', default=None)
    parser.add_argument('--sudo-user', help='execute the shell command as this user on the cluster', default='hadoop')
    parser.add_argument('--workflow-profiler', choices=['pyinstrument'], help='profiler to run on the launch-task process', default=None)
    parser.add_argument('--wheel-url', help='url of the wheelhouse', default=None)
    parser.add_argument('--virtualenv-extra-args', help='additional arguments passed to virtualenv command when creating the virtual environment', default=None)
    parser.add_argument('--skip-setup', action='store_true', help='assumes the environment has already been configured and you can simply run the task')
    parser.add_argument('--package', action='append', help='pip install these packages in the pipeline virtual environment')
    parser.add_argument('--extra-repo', action='append', help="""additional git repositories to checkout on the cluster during the deployment""")
    parser.add_argument('--python-version',
                        help='Python version that will be used to run pipelines task e.g., /usr/bin/python3.6',
                        default=None)
    arguments, extra_args = parser.parse_known_args()
    arguments.launch_task_arguments = extra_args

    log('Parsed arguments = {0}'.format(arguments))
    log('Running commands from path = {0}'.format(STATIC_FILES_PATH))
    uid = arguments.remote_name or str(uuid.uuid4())
    log('Remote name = {0}'.format(uid))

    # Push in any secure config values that we got.
    if arguments.secure_config:
        for config_path in arguments.secure_config:
            # We construct an absolute path here because the parameter that comes in is simply
            # relative to the checkout of the configuration repository, but the local scheduler
            # shouldn't have to know that, which in turn makes --additional-config agnostic of
            # how we're using it for edX's purposes (with a repository).
            arguments.launch_task_arguments.append('--additional-config')
            arguments.launch_task_arguments.append(os.path.join(REMOTE_DATA_DIR, uid, REMOTE_CONFIG_DIR_BASE, config_path))

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

        retry = 0
        while prep_result != 0 and retry < ANSIBLE_MAX_RETRY:
            log('ANSIBLE RUN RETURNED NON-ZERO EXIT STATUS: {0}'.format(prep_result))
            log('RETRYING')
            retry += 1
            prep_result = run_ansible(tuple(args), arguments, executable='ansible-playbook')

        if prep_result != 0:
            log('ANSIBLE RUN FAILED AFTER {0} RETRIES'.format(ANSIBLE_MAX_RETRY))
            return prep_result

    data_dir = os.path.join(REMOTE_DATA_DIR, uid)
    code_dir = os.path.join(data_dir, REMOTE_CODE_DIR_BASE)
    log_dir = os.path.join(REMOTE_LOG_DIR, uid)
    sudo_user = arguments.sudo_user

    env_vars = {}
    if arguments.workflow_profiler:
        env_vars['WORKFLOW_PROFILER'] = arguments.workflow_profiler
        env_vars['WORKFLOW_PROFILER_PATH'] = log_dir

    env_var_string = ' '.join('{0}={1}'.format(k, v) for k, v in env_vars.iteritems())

    command = 'cd {code_dir} && . $HOME/.bashrc && . {data_dir}/venv/bin/activate && {env_vars}{bg}launch-task {task_arguments}{end_bg}'.format(
        env_vars=env_var_string + ' ' if env_var_string else '',
        data_dir=data_dir,
        code_dir=code_dir,
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
        'uuid': uid,
        'root_data_dir': REMOTE_DATA_DIR,
        'root_log_dir': REMOTE_LOG_DIR
    }
    repos = {
        'pipeline': {
            'url': 'https://github.com/edx/edx-analytics-pipeline.git',
            'branch': 'origin/master',
            'dir_name': REMOTE_CODE_DIR_BASE
        }
    }
    if arguments.repo:
        repos['pipeline']['url'] = arguments.repo
    if arguments.branch:
        repos['pipeline']['branch'] = arguments.branch
    if arguments.wheel_url is not None:
        log('WARNING: wheel_url argument is no longer supported: ignoring {0}'.format(arguments.wheel_url))
    if arguments.vagrant_path or arguments.host:
        extra_vars['write_luigi_config'] = False
    if arguments.virtualenv_extra_args:
        extra_vars['virtualenv_extra_args'] = arguments.virtualenv_extra_args
    if arguments.python_version:
        extra_vars['python_version'] = arguments.python_version
    if arguments.package:
        extra_vars['packages'] = arguments.package

    if arguments.secure_config_repo:
        repos['secure'] = {
            'url': arguments.secure_config_repo,
            'branch': 'origin/release',
            'dir_name': REMOTE_CONFIG_DIR_BASE
        }
        if arguments.secure_config_branch:
            repos['secure']['branch'] = arguments.secure_config_branch

    if arguments.override_config:
        extra_vars['override_config'] = arguments.override_config

    if arguments.extra_repo:
        # additional repos are specified as URLs with query string parameters for the branch and dir_name.
        # For Example:
        #   git+ssh://git@github.com:edx/edx-analytics-pipeline.git?dir_name=pipeline&branch=origin%2Frelease
        #   https://github.com/edx/edx-analytics-pipeline?dir_name=pipeline&branch=test12
        for idx, repo in enumerate(arguments.extra_repo):
            full_url = urlparse(repo)
            git_url = full_url.netloc + full_url.path
            params = dict(parse_qsl(full_url.query))
            name = 'repo_{}'.format(idx)
            repos[name] = {
                'url': git_url,
                'branch': params.get('branch', 'master'),
                'dir_name': params.get('dir_name', name)
            }

    extra_vars['pipeline_repo_dir_name'] = repos['pipeline']['dir_name']
    extra_vars['repos'] = list(repos.values())
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
            arguments.private_key = value.strip('"')

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
