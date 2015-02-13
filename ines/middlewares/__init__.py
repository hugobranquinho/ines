# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>


DEFAULT_MIDDLEWARE_POSITION = {
    'payload': -100,
    'logging': -99,
    'cors': -98,
    'repoze.tm': -97}


class Middleware(object):
    def __init__(self, config, application, **settings):
        self.config = config
        self.application = application
        self.settings = settings
