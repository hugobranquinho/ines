# -*- coding: utf-8 -*-

import errno
from os import getpid
from os import uname
from os.path import join as os_join
from tempfile import gettempdir

from six import PY3
from six import u

if PY3:
    from functools import lru_cache
else:
    from repoze.lru import lru_cache


APPLICATIONS = {}
CAMELCASE_UPPER_WORDS = set(['CSV'])
MARKER = object()
API_CONFIGURATION_EXTENSIONS = {}

DEFAULT_RENDERERS = {}
DEFAULT_METHODS = ['GET', 'PUT', 'POST', 'DELETE']

IGNORE_FULL_NAME_WORDS = [u('de'), u('da'), u('e'), u('do')]

PROCESS_ID = getpid()
SYSTEM_NAME, DOMAIN_NAME, SYSTEM_RELEASE, SYSTEM_VERSION, MACHINE = uname()
DEFAULT_CACHE_DIRPATH = os_join(gettempdir(), 'ines-cache')

DEFAULT_RETRY_ERRNO = set([errno.ESTALE])
DEFAULT_RETRY_ERRNO.add(116)  # Stale NFS file handle
OPEN_BLOCK_SIZE = 2**18
