# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

from zope.interface import Interface


class IBaseSessionManager(Interface):
    pass


class ISchemaView(Interface):
    pass


class IInputSchemaView(Interface):
    pass


class IOutputSchemaView(Interface):
    pass
