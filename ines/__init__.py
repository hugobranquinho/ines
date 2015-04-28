# -*- coding: utf-8 -*-

from os import getpid
from os import uname

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
DEFAULT_CACHE_DIRNAME = 'ines-cache'
