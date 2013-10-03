# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
# 
# @author Hugo Branquinho <hugobranq@gmail.com>
# 
# $Id$

from pyramid.i18n import TranslationStringFactory


_ = TranslationStringFactory('ines')
TRANSLATION_FACTORIES = {'ines': _}

PACKAGES = {}
DEFAULT_EXTENSIONS = [('database', 'ines.api.database:BaseDatabaseSession'),
                      ('form', 'ines.api.form:BaseFormSession'),
                      ('layout', 'ines.api.layout:BaseLayoutSession')]

MISSING = object()

LOGS = []

STATIC_CACHE_AGE = 10800
