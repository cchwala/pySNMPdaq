##########
# Config #
########## 
import logging

LOG_DIR = '/tmp/pySNMPdaq'
LOG_FILE = 'pySNMPdaq.log'
LOG_LEVEL = logging.INFO

WRITE_TO_STD_OUT = True

# Data file config
# ---------------
WRITE_TO_FILE = True
FILENAME_PREFIX = 'pySNMPdaq_test'

# Data directory config
# ---------------------
DATA_DIR = '/tmp/pySNMPdaq/data'
CONFIG_ARCHIVE_DIR = 'config'
ARCHIVE_FILES = True
ARCHIVE_DIR = 'archive'
PUT_DATA_TO_OUT_DIR = True
DATA_OUT_DIR = 'data_outbox'

# SSH config
# ----------
SSH_TRANSFER = False
SSH_USER = 'my_ssh_user'
SSH_SERVER = 'some.server.comd'
SSH_REMOTEPATH = '/data/test'
SSH_REFUGIUM_DIR = 'ssh_refugium'

# SNMP config
# -----------
SNMP_TIMEOUT_SEC = 2
SNMP_RETRIES = 1
SNMP_VERSION = 2
# Used for SNMP v1 and v2
SNMP_COMMUNITY = 'public'
# Used for SNMP v3
SNMP_USERNAME = 'example_snmp_user'
SNMP_AUTHPASSWORD = 'example_pw'

# Timer config
# ------------
NEW_FILE_WAIT_MINUTES = 1
TIMER_1_QUERY_WAIT_SEC = 5
TIMER_2_QUERY_WAIT_SEC = 5
