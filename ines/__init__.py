# -*- coding: utf-8 -*-

import datetime
import errno
from os import getpid, linesep, uname
from os.path import join as os_join
import sys
from tempfile import gettempdir
from time import time as _now_time


APPLICATIONS = {}
CAMELCASE_UPPER_WORDS = {'CSV'}
MARKER = object()
API_CONFIGURATION_EXTENSIONS = {}

DEFAULT_RENDERERS = {}
DEFAULT_METHODS = ['GET', 'PUT', 'POST', 'DELETE']

IGNORE_FULL_NAME_WORDS = ['de', 'da', 'e', 'do']

PROCESS_ID = getpid()
SYSTEM_NAME, DOMAIN_NAME, SYSTEM_RELEASE, SYSTEM_VERSION, MACHINE = uname()
DEFAULT_CACHE_DIRPATH = os_join(gettempdir(), 'ines-cache')

DEFAULT_RETRY_ERRNO = {errno.ESTALE}
DEFAULT_RETRY_ERRNO.add(116)  # Stale NFS file handle
OPEN_BLOCK_SIZE = 2**18

# datetime now without microseconds
_now = datetime.datetime.now
NOW = lambda: _now().replace(microsecond=0)
# timestamp without microseconds
NOW_TIME = lambda: int(_now_time())
TODAY_DATE = datetime.date.today

HTML_NEW_LINE = '<br/>'
NEW_LINE = linesep
NEW_LINE_AS_BYTES = NEW_LINE.encode()


def lazy_import_module(name):
    module = sys.modules.get(name, MARKER)
    if module is not MARKER:
        return module
    else:
        __import__(name)
        return sys.modules[name]
