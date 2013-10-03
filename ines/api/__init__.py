# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
# 
# @author Hugo Branquinho <hugobranq@gmail.com>
# 
# $Id$

from ines import MISSING
from ines import PACKAGES
from ines.utils import cache_property


class BaseClass(object):
    def __init__(self, config, session, package_name):
        self.config = config
        self.session = session
        self.package_name = package_name
        self.settings = config.registry.settings

    def __call__(self, request):
        return self.session(self, request)


class BaseSession(object):
    _base_class = BaseClass

    def __init__(self, api_class, request):
        self.api_class = api_class
        self.package_name = api_class.package_name
        self.config = get_package_config(self.package_name)
        self.registry = self.config.registry
        self.request = request

    @cache_property
    def cache(self):
        return self.request.cache[self.package_name]

    @cache_property
    def packages(self):
        return PackagesConnector(self.request)

    def package_is_active(self, package_name):
        return self.packages._package_is_active(package_name)

    @cache_property
    def api(self):
        if isinstance(self, BaseAPISession):
            return self
        elif self.package_name == self.request.package_name:
            return self.request.api
        else:
            return getattr(self.packages, self.package_name)

    @property
    def settings(self):
        return self.api_class.settings

    @cache_property
    def translation_factory(self):
        return self.settings['translation_factory']

    @cache_property
    def translator(self):
        return self.request.translator

    def _(self, message, **kwargs):
        translation_string = self.translation_factory(message)
        return self.translator(translation_string, **kwargs)

    def log(self, *args, **kwargs):
        kwargs['package_name'] = self.package_name
        return self.request.log(*args, **kwargs)

    def log_error(self, *args, **kwargs):
        kwargs['package_name'] = self.package_name
        return self.request.log_error(*args, **kwargs)

    def log_critical(self, *args, **kwargs):
        kwargs['package_name'] = self.package_name
        return self.request.log_critical(*args, **kwargs)

    def log_warning(self, *args, **kwargs):
        kwargs['package_name'] = self.package_name
        return self.request.log_warning(*args, **kwargs)


class BaseAPISession(BaseSession):
    def __getattribute__(self, name):
        try:
            attribute = BaseSession.__getattribute__(self, name)
        except AttributeError as error:
            extension = self.settings['extensions'].get(name)
            if not extension:
                raise

            argument_key = '_%s' % name
            attribute = getattr(self, argument_key, MISSING)
            if attribute is MISSING:
                attribute = extension(self.request)
                setattr(self, argument_key, attribute)

        return attribute

    def get_language(self):
        pass


def get_package_config(package_name):
    config = PACKAGES.get(package_name)
    if config is None:
        message = u'Missing package %s' % package_name
        raise NotImplementedError(message)
    else:
        return config


def package_is_active(package_name):
    return PACKAGES.has_key(package_name)


class PackagesConnector(object):
    def __init__(self, request):
        self._request = request
        self._cache = {}

    def __getattribute__(self, key):
        if key.startswith('_'):
            return object.__getattribute__(self, key)

        if self._cache.has_key(key):
            return self._cache[key]

        config = get_package_config(key)
        api_session = config.registry.settings['api'](self._request)
        self._cache[key] = api_session
        return api_session

    def _package_is_active(self, package_name):
        return package_is_active(package_name)
