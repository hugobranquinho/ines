# -*- coding: utf-8 -*-


DEFAULT_MIDDLEWARE_POSITION = {
    'payload': -100,
    'cors': -99,
    'logging': -98,
    'repoze.tm': -97}


class Middleware(object):
    def __init__(self, config, application, **settings):
        self.config = config
        self.application = application
        self.settings = settings
