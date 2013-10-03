# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
# 
# @author Hugo Branquinho <hugobranq@gmail.com>
# 
# $Id$

from importlib import import_module
from inspect import getmembers
from inspect import isclass
from inspect import ismodule
from pkg_resources import EntryPoint
from pkgutil import iter_modules

from ines import MISSING


def find_class_on_module(module, class_to_find):
    if not ismodule(module):
        module = import_module(module)

    module_package_name = module.__name__.split('.', 1)[0]
    for key, value in getmembers(module, isclass):
        if value is not class_to_find and \
           issubclass(value, class_to_find):
            value_package_name = value.__module__.split('.', 1)[0]
            if value_package_name == module_package_name:
                return value

    if hasattr(module, '__path__'):
        for importer, modname, ispkg in iter_modules(
                                            module.__path__,
                                            module.__name__ + '.'):
            deep_module = import_module(modname)
            class_found = find_class_on_module(deep_module, class_to_find)
            if class_found:
                return class_found


def cache_property(func):
    argument_key = '_%s' % func.__name__
    def argument_get(cls):
        argument = getattr(cls, argument_key, MISSING)
        if argument is MISSING:
            argument = func(cls)
            setattr(cls, argument_key, argument)
        return argument
    return property(argument_get)


def get_method(path, ignore=False):
    try:
        method = EntryPoint.parse('x=%s' % path).load(False)
    except ImportError as error:
        if not ignore:
            raise
    else:
        return method


class InfiniteDict(dict):
    def __missing__(self, key):
        self[key] = InfiniteDict()
        return self[key]


class MissingDict(dict):
    def __missing__(self, key):
        self[key] = {}
        return self[key]


class MissingList(dict):
    def __missing__(self, key):
        self[key] = []
        return self[key]


class MissingSet(dict):
    def __missing__(self, key):
        self[key] = set()
        return self[key]


class MissingInteger(dict):
    def __missing__(self, key):
        self[key] = 0
        return self[key]


def find_settings(settings, start_pattern):
    result = {}
    for key, value in settings.items():
        if key.startswith(start_pattern):
            key_end = key.split(start_pattern, 1)[1]
            result[key_end] = value

    return result


class Options(dict):
    def add_attribute_value(self, attribute, key, value):
        if not self.has_key(attribute):
            self[attribute] = {}
        self[attribute][key] = value

    def add_title(self, attribute, title):
        self.add_attribute_value(attribute, 'title', title)

    def get_title(self, attribute):
        return self[attribute].get('title')
