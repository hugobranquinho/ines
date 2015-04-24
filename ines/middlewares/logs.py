# -*- coding: utf-8 -*-

import sys
from traceback import format_exception

from pyramid.decorator import reify
from pyramid.httpexceptions import HTTPInternalServerError
from pyramid.interfaces import IRequestFactory

from ines.convert import force_string
from ines.middlewares import Middleware
from ines.utils import format_error_to_json


class LoggingMiddleware(Middleware):
    name = 'logging'

    @property
    def request_factory(self):
        return self.config.registry.queryUtility(IRequestFactory)

    @reify
    def api_name(self):
        return self.settings.get('logging_api_name') or 'logging'

    def __call__(self, environ, start_response):
        try:
            for chunk in self.application(environ, start_response):
                yield chunk
        except (BaseException, Exception) as error:
            type_, value, tb = sys.exc_info()
            message = ''.join(format_exception(type_, value, tb))

            # Save / log error
            request = self.request_factory(environ)
            request.registry = self.config.registry

            try:
                small_message = '%s: %s' % (error.__class__.__name__, force_string(error))
            except (BaseException, Exception):
                small_message = error

            try:
                getattr(request.api, self.api_name).log_critical(
                    'internal_server_error',
                    str(small_message))
            except (BaseException, Exception):
                print message

            internal_server_error = HTTPInternalServerError()
            headers = [('Content-type', 'application/json')]
            start_response(internal_server_error.status, headers)
            yield format_error_to_json(internal_server_error, request=request)
