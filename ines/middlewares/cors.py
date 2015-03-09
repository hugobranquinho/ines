# -*- coding: utf-8 -*-

from pyramid.decorator import reify
from pyramid.httpexceptions import HTTPMethodNotAllowed
from pyramid.httpexceptions import HTTPNoContent

from ines.convert import force_string
from ines.convert import maybe_integer
from ines.middlewares import Middleware
from ines.utils import format_error_to_json


DEFAULT_METHODS = ['GET', 'HEAD', 'POST', 'PUT', 'OPTIONS', 'DELETE']


class Cors(Middleware):
    name = 'cors'

    @reify
    def allowed_origins(self):
        return self.settings.get('allowed_origins', '').split()

    @reify
    def allow_all_origins(self):
        return '*' in self.allowed_origins

    @reify
    def allowed_methods(self):
        allowed_methods = self.settings.get('allowed_methods', '').split()
        if not allowed_methods:
            return DEFAULT_METHODS
        else:
            return set(m.upper() for m in allowed_methods)

    @reify
    def max_age(self):
        return maybe_integer(self.settings.get('max_age'))

    def __call__(self, environ, start_response):
        http_origin = environ.get('HTTP_ORIGIN')
        if not self.allow_all_origins and http_origin not in self.allowed_origins:
            return self.application(environ, start_response)

        http_method = environ.get('REQUEST_METHOD')
        if http_method not in self.allowed_methods:
            method_not_allowed = HTTPMethodNotAllowed()
            start_response(
                method_not_allowed.status,
                [('Content-type', 'application/json')])
            return format_error_to_json(method_not_allowed)

        cors_headers = []
        if self.allow_all_origins:
            cors_headers.append(('Access-Control-Allow-Origin', '*'))
        else:
            cors_headers.append(('Access-Control-Allow-Origin', force_string(http_origin)))

        if http_method == 'OPTIONS':
            methods = environ.get('HTTP_ACCESS_CONTROL_REQUEST_METHOD')
            if methods:
                cors_headers.append(('Access-Control-Allow-Methods', force_string(methods)))

            http_headers = environ.get('HTTP_ACCESS_CONTROL_REQUEST_HEADERS')
            if http_headers:
                cors_headers.append(('Access-Control-Allow-Headers', force_string(http_headers)))

            if self.max_age is not None:
                cors_headers.append(('Access-Control-Max-Age', force_string(self.max_age)))

            start_response(HTTPNoContent().status, cors_headers)
            return []
        else:
            def start_response_decorator(status, headers, exc_info=None):
                headers.extend(cors_headers)
                return start_response(status, headers, exc_info)

            return self.application(environ, start_response_decorator)
