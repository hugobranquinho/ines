# -*- coding: utf-8 -*-

from pyramid.i18n import get_localizer as base_get_localizer
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


def translate_factory(request, locale_name=None):
    if not locale_name or locale_name == request.locale_name:
        return base_get_localizer(request).translate

    translator = get_localizer(request.registry, locale_name).translate

    def method(tstring, **kwargs):
        default = kwargs.pop('default', None)
        if tstring is not None:
            return translator(tstring, **kwargs) or default
        else:
            return default
    return method


def translate(request, tstring, locale_name=None):
    return translate_factory(request, locale_name)(tstring)
