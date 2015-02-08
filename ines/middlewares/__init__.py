# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>


DEFAULT_MIDDLEWARE_POSITION = {
    'logging': 102,
    'cors': 101,
    'repoze.tm': 100}


class Middleware(object):
    def __init__(self, config, application, **settings):
        self.config = config
        self.application = application
        self.settings = settings
