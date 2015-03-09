# -*- coding: utf-8 -*-

from importlib import import_module
from inspect import getmembers
from inspect import isclass
from inspect import ismodule
from pkg_resources import EntryPoint
from pkgutil import iter_modules


def find_class_on_module(module, class_to_find):
    if not ismodule(module):
        module = import_module(module)

    result = set()
    module_package_name = module.__name__.split('.', 1)[0]
    for key, class_found in getmembers(module, isclass):
        if class_found is not class_to_find and \
           issubclass(class_found, class_to_find):
            value_package_name = class_found.__module__.split('.', 1)[0]
            if value_package_name == module_package_name:
                result.add(class_found)

    if hasattr(module, '__path__'):
        modules = iter_modules(module.__path__, module.__name__ + '.')
        for importer, modname, ispkg in modules:
            deep_module = import_module(modname)
            result.update(find_class_on_module(deep_module, class_to_find))

    return result


def get_object_on_path(path):
    return EntryPoint.parse('x=' + path).load(False)
