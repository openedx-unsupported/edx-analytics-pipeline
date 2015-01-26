
import os
import sys
import time


def monitor_task():
    log_dir_path = os.getenv('WORKFLOW_LOG_DIR')
    if not log_dir_path:
        sys.exit(0)

    pid_file_path = os.path.join(log_dir_path, 'launch-task.pid')

    with open(pid_file_path, 'r') as pid_file:
        pid = int(pid_file.read())

    log_file_path = os.path.join(log_dir_path, 'stdout')

    with open(log_file_path, 'r') as log_file:
        while True:
            if not check_pid(pid):
                return 0

            line = log_file.readline()
            if line:
                sys.stdout.write(line)
            else:
                time.sleep(1)


def check_pid(pid):
    """ Check For the existence of a unix pid. """
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True


if __name__ == '__main__':
    sys.exit(monitor_task())
