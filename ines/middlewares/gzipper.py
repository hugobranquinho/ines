# -*- coding: utf-8 -*-


from ines.middlewares import Middleware


class Gzip(Middleware):
    name = 'gzip'

    def __init__(self, config, application, **settings):
        super(Gzip, self).__init__(config, application, **settings)

        compress_level = int(settings.get('compress_level') or 6)
        from paste.gzipper import middleware
        self.paste_gzip = middleware(self.application, compress_level)

    def __call__(self, environ, start_response):
        return self.paste_gzip(environ, start_response)
