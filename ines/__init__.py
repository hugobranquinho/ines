# -*- coding: utf-8 -*-

from translationstring import TranslationStringFactory


_ = TranslationStringFactory('ines')
APPLICATIONS = {}
CAMELCASE_UPPER_WORDS = set()
MARKER = object()
API_CONFIGURATION_EXTENSIONS = {}

DEFAULT_METHODS = ['GET', 'PUT', 'POST', 'DELETE']
TRUES = frozenset(('t', 'true', 'y', 'yes', 'on', '1'))
FALSES = frozenset(('f', 'false', 'f', 'no', 'off', '0'))
