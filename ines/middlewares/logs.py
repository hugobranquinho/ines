# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

import sys
from traceback import format_exception

from pyramid.httpexceptions import HTTPInternalServerError
from pyramid.interfaces import IRequestFactory

from ines.middlewares import Middleware
from ines.utils import format_json_response


class LoggingMiddleware(Middleware):
    name = 'logging'

    @property
    def request_factory(self):
        return self.config.registry.queryUtility(IRequestFactory)

    def __call__(self, environ, start_response):
        try:
            for chunk in self.application(environ, start_response):
                yield chunk
        except Exception as error:
            internal_server_error = HTTPInternalServerError()
            error_key = internal_server_error.title.lower().replace(' ', '_')

            type_, value, tb = sys.exc_info()
            error = ''.join(format_exception(type_, value, tb))

            request = self.request_factory(environ)
            request.registry = self.config.registry
            request.api.logging.log_critical(error_key, error)

            headers = [('Content-type', 'application/json')]
            start_response(internal_server_error.status, headers)

            yield format_json_response(
                internal_server_error.code,
                error_key,
                internal_server_error.title)
