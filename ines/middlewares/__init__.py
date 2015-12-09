# -*- coding: utf-8 -*-


# lower position, comes first in request
DEFAULT_MIDDLEWARE_POSITION = dict(
    (k, i - 100)
    for i, k in enumerate([
        'payload',
        'cors',
        'logging',
        'repoze.tm',
        'browser',
        'gzip',
    ]))


class Middleware(object):
    def __init__(self, config, application, **settings):
        self.config = config
        self.application = application
        self.settings = settings

    def __call__(self, environ, start_response):
        return self.application(environ, start_response)
