# -*- coding: utf-8 -*-

import errno
from os import getpid
from os import uname
from os.path import join as join_path
from tempfile import gettempdir

from translationstring import TranslationStringFactory


_ = TranslationStringFactory('ines')
APPLICATIONS = {}
CAMELCASE_UPPER_WORDS = set()
MARKER = object()
API_CONFIGURATION_EXTENSIONS = {}

DEFAULT_RENDERERS = {}
DEFAULT_METHODS = ['GET', 'PUT', 'POST', 'DELETE']
TRUES = frozenset(('t', 'true', 'y', 'yes', 'on', '1'))
FALSES = frozenset(('f', 'false', 'f', 'no', 'off', '0'))

IGNORE_FULL_NAME_WORDS = [u'de', u'da', u'e', u'do']

PROCESS_ID = getpid()
SYSTEM_NAME, DOMAIN_NAME, SYSTEM_RELEASE, SYSTEM_VERSION, MACHINE = uname()
DEFAULT_CACHE_DIRPATH = join_path(gettempdir(), 'ines-cache')

DEFAULT_RETRY_ERRNO = set([errno.ESTALE])
DEFAULT_RETRY_ERRNO.add(116)  # Stale NFS file handle
OPEN_BLOCK_SIZE = 2**16
