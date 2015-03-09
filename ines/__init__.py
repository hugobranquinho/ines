# -*- coding: utf-8 -*-

APPLICATIONS = {}
CAMELCASE_UPPER_WORDS = set()
MISSING = object()
API_CONFIGURATION_EXTENSIONS = {}

DEFAULT_METHODS = ['GET', 'PUT', 'POST', 'DELETE']
TRUES = frozenset(('t', 'true', 'y', 'yes', 'on', '1'))
FALSES = frozenset(('f', 'false', 'f', 'no', 'off', '0'))
