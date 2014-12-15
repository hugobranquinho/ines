# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

from pyramid.i18n import make_localizer
from pyramid.interfaces import ILocalizer
from pyramid.interfaces import ITranslationDirectories


def get_localizer(registry, locale_name):
    localizer = registry.queryUtility(ILocalizer, name=locale_name)
    if localizer is not None:
        return localizer

    tdirs = registry.queryUtility(ITranslationDirectories, default=[])
    localizer = make_localizer(locale_name, tdirs)
    registry.registerUtility(localizer, ILocalizer, name=locale_name)
    return localizer
