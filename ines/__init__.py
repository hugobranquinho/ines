# -*- coding: utf-8 -*-

from os import getpid
from socket import getfqdn

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
DOMAIN_NAME = str(getfqdn())
