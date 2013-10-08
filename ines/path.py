# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
# 
# @author Hugo Branquinho <hugobranq@gmail.com>
# 
# $Id$

from os.path import isdir
from pkg_resources import resource_filename

from pyramid.path import caller_package

from ines.convert import force_string


def find_package_name(level=0):
    level += 3
    full_path = caller_package(level=level).__name__
    return full_path.split('.', 1)[0]


def maybe_resource_dir(path):
    path = force_string(path)
    if isdir(path):
        return path

    if ':' in path:
        path = resource_filename(*path.split(':', 1))
        if isdir(path):
            return path
