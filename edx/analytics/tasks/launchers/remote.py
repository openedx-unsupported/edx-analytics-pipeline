"""Execute tasks on a remote EMR cluster."""

import argparse
import glob
import json
import os
import pipes
import pstats
import shutil
from subprocess import Popen, PIPE
import sys
import tempfile
import uuid


STATIC_FILES_PATH = os.path.join(sys.prefix, 'share', 'edx.analytics.tasks')


def main():
    """Parse arguments and run the remote task."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--job-flow-id', help='EMR job flow to run the task', default=None)
    parser.add_argument('--job-flow-name', help='EMR job flow to run the task', default=None)
    parser.add_argument('--branch', help='git branch to checkout before running the task', default='release')
    parser.add_argument('--repo', help='git repository to clone')
    parser.add_argument('--remote-name', help='an identifier for this remote task')
    parser.add_argument('--wait', action='store_true', help='wait for the task to complete')
    parser.add_argument('--verbose', action='store_true', help='display very verbose output')
    parser.add_argument('--log-path', help='download luigi output streams after completing the task', default=None)
    parser.add_argument('--user', help='remote user name to connect as', default=None)
    parser.add_argument('--override-config', help='config file to use to run the job', default=None)
    parser.add_argument('--secure-config', help='config file in secure config repo to use to run the job', default=None)
    parser.add_argument('--secure-config-branch', help='git branch to checkout to find the secure config file', default=None)
    parser.add_argument('--secure-config-repo', help='git repository to clone to find the secure config file', default=os.getenv('ANALYTICS_SECURE_REPO'))
    parser.add_argument('--shell', help='execute a shell command on the cluster and exit', default=None)
    parser.add_argument('--sudo-user', help='execute the shell command as this user on the cluster', default=None)
    parser.add_argument('--profile', action='store_true')
    parser.add_argument('--analyze', action='store_true')
    parser.add_argument('--analyze-post-clean', action='store_true')
    parser.add_argument('--analyze-dir', default=None)
    arguments, extra_args = parser.parse_known_args()
    arguments.launch_task_arguments = extra_args

    log('Parsed arguments = {0}'.format(arguments))
    log('Running commands from path = {0}'.format(STATIC_FILES_PATH))
    uid = arguments.remote_name or str(uuid.uuid4())
    log('Remote name = {0}'.format(uid))

    inventory = get_ansible_inventory()
    if arguments.analyze:
        return_code = run_performance_analysis(inventory, arguments, uid)
    elif arguments.shell:
        return_code = run_remote_shell(inventory, arguments)
    else:
        return_code = run_task_playbook(arguments, uid)

    log('Exiting with status = {0}'.format(return_code))
    sys.exit(return_code)


def run_task_playbook(arguments, uid):
    """
    Execute the ansible playbook that triggers and monitors the remote task execution.

    Args:
        arguments (argparse.Namespace): The arguments that were passed in on the command line.
        uid (str): A unique identifier for this task execution.
    """
    extra_vars = convert_args_to_extra_vars(arguments, uid)
    args = ['task.yml', '-e', extra_vars]
    if arguments.user:
        args.extend(['-u', arguments.user])
    return run_ansible(tuple(args), arguments.verbose, executable='ansible-playbook')


def convert_args_to_extra_vars(arguments, uid):
    """
    Generate the set of variables that need to be passed in to ansible since they are expected to be set by the
    playbook.

    Args:
        arguments (argparse.Namespace): The arguments that were passed in on the command line.
        uid (str): A unique identifier for this task execution.
    """
    extra_vars = {
        'name': arguments.job_flow_id or arguments.job_flow_name,
        'branch': arguments.branch,
        'task_arguments': ' '.join(arguments.launch_task_arguments),
        'uuid': uid,
    }
    if arguments.repo:
        extra_vars['repo'] = arguments.repo
    if arguments.wait:
        extra_vars['wait_for_task'] = True
    if arguments.log_path:
        extra_vars['local_log_dir'] = arguments.log_path
    if arguments.override_config:
        extra_vars['override_config'] = arguments.override_config
    if arguments.secure_config_repo:
        extra_vars['secure_config_repo'] = arguments.secure_config_repo
    if arguments.secure_config_branch:
        extra_vars['secure_config_branch'] = arguments.secure_config_branch
    if arguments.secure_config:
        extra_vars['secure_config'] = arguments.secure_config
    if arguments.profile:
        extra_vars['profile'] = arguments.profile
    return json.dumps(extra_vars)


def get_ansible_inventory():
    """
    Ensure the EC2 inventory cache is cleared before running ansible.

    Otherwise new resources will not be present in the inventory which will cause ansible to fail to connect to them.

    """
    executable_path = os.path.join(STATIC_FILES_PATH, 'ec2.py')
    command = [executable_path, '--refresh-cache']
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


def run_ansible(args, verbose, executable='ansible'):
    """
    Execute ansible passing in the provided arguments.

    Args:
        args (iterable): A collection of arguments to pass to ansible on the command line.
        verbose (bool): Tell ansible to produce verbose output detailing exactly what commands it is executing.
        executable (str): The executable script to invoke on the command line.  Defaults to "ansible".

    """
    executable_path = os.path.join(sys.prefix, 'bin', executable)
    command = [executable_path, '-i', 'ec2.py'] + list(args)
    if verbose:
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


def run_remote_shell(inventory, arguments):
    """Run a shell command on a hadoop cluster."""
    ansible_group_name = 'mr_{0}_master'.format(arguments.job_flow_id or arguments.job_flow_name)
    hostname = inventory[ansible_group_name][0]
    shell_command = arguments.shell
    if arguments.sudo_user:
        shell_command = 'sudo -u hadoop /bin/sh -c {0}'.format(pipes.quote(arguments.shell))
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
        hostname,
        shell_command
    ]
    log('Running command = {0}'.format(command))
    proc = Popen(
        command,
        cwd=STATIC_FILES_PATH
    )
    proc.wait()
    return proc.returncode


def run_performance_analysis(inventory, arguments, uid):
    job_flow_name = arguments.job_flow_id or arguments.job_flow_name
    task_nodes = inventory['mr_{}_task'.format(job_flow_name)]
    core_nodes = inventory['mr_{}_core'.format(job_flow_name)]
    master_node = inventory['mr_{}_master'.format(job_flow_name)][0]
    user = 'hadoop'

    is_temp_dir = False
    if not arguments.analyze_dir:
        working_dir = tempfile.mkdtemp()
        is_temp_dir = True
    else:
        working_dir = arguments.analyze_dir

    log('Working directory = {0}'.format(working_dir))
    if os.path.exists(working_dir) and len(os.listdir(working_dir)) > 0:
        log('Working directory not empty. Aborting.')
        return 1

    try:
        node_file_path = os.path.join(working_dir, 'slaves.txt')
        task_profiles_path = os.path.join(working_dir, 'task_profiles')
        perf_log_path = os.path.join(working_dir, 'edx-perf-stats.log')
        os.makedirs(task_profiles_path)

        cprofile_path = os.path.join('/var/lib/analytics-tasks', uid, 'repo', '*.cprofile')
        task_cprofile_path = '/mnt/tmp/mrrunner_profiles/job_*'
        command = [
            'scp',
            '{user}@{host}:{path}'.format(user=user, host=master_node, path=cprofile_path),
            working_dir
        ]
        log('Running command = {0}'.format(command))
        proc = Popen(
            command,
            cwd=STATIC_FILES_PATH
        )
        proc.wait()
        if proc.returncode != 0:
            return proc.returncode

        print 'Luigi process profile'
        profile_files = glob.glob(os.path.join(working_dir, '*.cprofile'))
        stats = pstats.Stats(*profile_files)
        stats.sort_stats('cumtime').print_stats()

        with open(node_file_path, 'w') as hosts_file:
            for host in (task_nodes + core_nodes):
                hosts_file.write(host)
                hosts_file.write('\n')

        pscp_command = [
            'parallel-slurp',
            '--recursive',
            '--user', user,
            '-h', node_file_path,
            '-O', 'StrictHostKeyChecking=no',
            '-O', 'UserKnownHostsFile=/dev/null',
            '--localdir', task_profiles_path,
            task_cprofile_path,
            'profiles'
        ]

        log('Running command = {0}'.format(pscp_command))
        proc = Popen(
            pscp_command,
            cwd=STATIC_FILES_PATH
        )
        proc.wait()
        if proc.returncode != 0:
            return proc.returncode

        print 'Task process profiles'
        task_profile_files = []
        for host in (task_nodes + core_nodes):
            task_profile_files += list(glob.glob(os.path.join(task_profiles_path, host, 'profiles', '*.cprofile')))
        first_file = task_profile_files.pop()
        task_profile_stats = pstats.Stats(first_file)
        for profile_file in task_profile_files:
            task_profile_stats.add(profile_file)
        task_profile_stats.sort_stats('cumtime').print_stats()
        task_profile_stats.dump_stats(os.path.join(working_dir, 'combined_mrrunner_stats.cprofile'))

        pscp_command = [
            'parallel-slurp',
            '--recursive',
            '--user', user,
            '-h', node_file_path,
            '-O', 'StrictHostKeyChecking=no',
            '-O', 'UserKnownHostsFile=/dev/null',
            '--localdir', working_dir,
            '/var/log/edx-perf-stats.log',
            'edx-perf-stats.log'
        ]

        log('Running command = {0}'.format(pscp_command))
        proc = Popen(
            pscp_command,
            cwd=STATIC_FILES_PATH
        )
        proc.wait()
        if proc.returncode != 0:
            return proc.returncode

        if arguments.analyze_post_clean:
            pssh_command = [
                'parallel-ssh',
                '--user', user,
                '-h', node_file_path,
                '-H', master_node,
                '-O', 'StrictHostKeyChecking=no',
                '-O', 'UserKnownHostsFile=/dev/null',
                'rm -rf {task_path} {core_path}'.format(
                    core_path=cprofile_path,
                    task_path=task_cprofile_path
                )
            ]
            log('Running command = {0}'.format(pssh_command))
            proc = Popen(
                pssh_command,
                cwd=STATIC_FILES_PATH
            )
            proc.wait()
            if proc.returncode != 0:
                return proc.returncode
    finally:
        if is_temp_dir:
            shutil.rmtree(working_dir)

    return 0


def log(message):
    """Writes debugging information to stderr."""
    sys.stderr.write(message)
    sys.stderr.write('\n')


if __name__ == '__main__':
    main()
