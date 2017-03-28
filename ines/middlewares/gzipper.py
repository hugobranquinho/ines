# -*- coding: utf-8 -*-

from io import BytesIO
from gzip import compress as gzip_compress

from pyramid.decorator import reify

from ines.middlewares import Middleware


class Gzip(Middleware):
    name = 'gzip'

    def __init__(self, config, application, **settings):
        super(Gzip, self).__init__(config, application, **settings)

        self.compress_level = int(settings.get('compress_level') or 9)

        self.content_types = (
            settings.get('content_types', '').split()
            or ['text/', 'application/', 'image/svg'])
        self.all_content_types = '*' in self.content_types

    def __call__(self, environ, start_response):
        return GzipMiddlewareSession(self)(environ, start_response)


class GzipMiddlewareSession(object):
    def __init__(self, middleware):
        self.middleware = middleware
        self.compressible = False
        self.status = None
        self.headers = []
        self.exc_info = None

    def __call__(self, environ, start_response):
        if 'gzip' not in environ.get('HTTP_ACCEPT_ENCODING', ''):
            return self.middleware.application(environ, start_response)

        self.start_response = start_response
        app_iter = self.middleware.application(environ, self.gzip_start_response)
        if app_iter is not None and self.compressible:
            binary = gzip_compress(b''.join(app_iter), self.middleware.compress_level)
            if hasattr(app_iter, 'close'):
                app_iter.close()

            self.remove_header('content-length')
            self.headers.append(('content-encoding', 'gzip'))
            self.set_header('content-length', len(binary))

            start_response(self.status, self.headers, self.exc_info)
            return [binary]

        return app_iter

    @reify
    def buffer(self):
        return BytesIO()

    def remove_header(self, name):
        i = len(self.headers)
        name = name.lower()
        for key, value in reversed(self.headers):
            i -= 1
            if key == name:
                self.headers.pop(i)

    def get_header(self, name):
        name = name.lower()
        for key, value in self.headers:
            if key == name:
                return value

    def in_headers(self, name):
        name = name.lower()
        for key, value in self.headers:
            if key == name:
                return True
        else:
            return False

    def set_header(self, name, new_value):
        name = name.lower()
        for i, (key, value) in enumerate(self.headers):
            if key == name:
                self.headers[i] = (name, str(new_value))
                break
        else:
            self.headers.append((name, str(new_value)))

    def gzip_start_response(self, status, headers, exc_info=None):
        self.headers = [(key.lower(), value) for key, value in headers]
        if not self.in_headers('content-encoding'):
            content_type = self.get_header('content-type')
            if content_type and 'zip' not in content_type:
                content_type = content_type.split(';')[0]

                if self.middleware.all_content_types:
                    self.compressible = True
                else:
                    for start_content_type in self.middleware.content_types:
                        if content_type.startswith(start_content_type):
                            self.compressible = True
                            break

                if self.compressible:
                    self.status = status
                    self.exc_info = exc_info
                    return self.buffer.write

        return self.start_response(status, headers, exc_info)
