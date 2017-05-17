import os

REMOTE_DATA_DIR = '/var/lib/analytics-tasks'
REMOTE_LOG_DIR = '/var/log/analytics-tasks'

REMOTE_CONFIG_DIR_BASE = 'config'

# The 'automation' bit is hard-coded. *shrug*
REMOTE_CONFIG_DIR = os.path.join(REMOTE_DATA_DIR, 'automation', REMOTE_CONFIG_DIR_BASE)
