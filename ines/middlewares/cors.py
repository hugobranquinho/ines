# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

from pyramid.httpexceptions import HTTPMethodNotAllowed
from pyramid.httpexceptions import HTTPNoContent
from pyramid.settings import asbool

from ines.convert import maybe_integer
from ines.utils import format_json_response


class CorsMiddleware(object):
    def __init__(self, application, settings):
        self.application = application

        cors_settings = {}
        for key, value in (settings or {}).items():
            if key.startswith('cors.'):
                cors_settings[key[5:]] = value
        self.settings = cors_settings

        self.allowed_origins = self.settings.get('allowed_origins', '').split()
        self.allow_all_origins = '*' in self.allowed_origins

        methods = self.settings.get('allowed_methods', '').split()
        if not methods:
            methods = ['GET', 'POST', 'PUT', 'OPTIONS', 'DELETE']
        self.allowed_methods = set(m.upper() for m in methods)

        self.max_age = maybe_integer(self.settings.get('max_age'))

    def __call__(self, environ, start_response):
        http_origin = environ.get('HTTP_ORIGIN')
        if (not self.allow_all_origins
            and http_origin not in self.allowed_origins):
            return self.application(environ, start_response)

        http_method = environ.get('REQUEST_METHOD')
        if http_method not in self.allowed_methods:
            method_not_allowed = HTTPMethodNotAllowed()

            headers = [('Content-type', 'application/json')]
            start_response(method_not_allowed.status, headers)

            return [format_json_response(
                method_not_allowed.code,
                method_not_allowed.title.lower().replace(' ', '_'),
                method_not_allowed.title)]

        cors_headers = []
        if self.allow_all_origins:
            cors_headers.append(('Access-Control-Allow-Origin', '*'))
        else:
            cors_headers.append(('Access-Control-Allow-Origin', http_origin))

        if http_method == 'OPTIONS':
            methods = environ.get('HTTP_ACCESS_CONTROL_REQUEST_METHOD')
            if methods:
                cors_headers.append(('Access-Control-Allow-Methods', methods))

            http_headers = environ.get('HTTP_ACCESS_CONTROL_REQUEST_HEADERS')
            if http_headers:
                cors_headers.append(('Access-Control-Allow-Headers', http_headers))

            if self.max_age:
                cors_headers.append(('Access-Control-Max-Age', self.max_age))

            start_response(HTTPNoContent().status, cors_headers)
            return []
        else:
            def start_response_decorator(status, headers, exc_info=None):
                headers.extend(cors_headers)
                return start_response(status, headers, exc_info)

            return self.application(environ, start_response_decorator)
