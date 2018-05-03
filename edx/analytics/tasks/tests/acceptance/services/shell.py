import logging
import subprocess
import sys

log = logging.getLogger(__name__)


def run(command, env=None):
    """Execute a subprocess and log the command before running it."""
    try:
        log.info('Running subprocess {0}'.format(subprocess.list2cmdline(command)))
    except TypeError:
        log.info('Running subprocess {0}'.format(command))
    buf = []

    params = {
        'args': command,
        'stdout': subprocess.PIPE,
        'stderr': subprocess.STDOUT
    }
    if env is not None:
        params['env'] = env
    # Execute the process gathering the output from stdout
    proc = subprocess.Popen(**params)

    # Read all output from the process, this loop should only exit after the process has
    # terminated and all output has been read from the stdout pipe.
    while True:
        data = proc.stdout.readline()
        if len(data) == 0:
            break

        # Pass through the output
        sys.stdout.write(data)
        buf.append(data)

    proc.wait()
    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, command, None)

    return ''.join(buf)
