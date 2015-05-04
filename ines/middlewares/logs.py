# -*- coding: utf-8 -*-

import sys
from traceback import format_exception

from pyramid.httpexceptions import HTTPInternalServerError

from ines.convert import force_string
from ines.middlewares import Middleware
from ines.request import make_request
from ines.utils import format_error_to_json


class LoggingMiddleware(Middleware):
    name = 'logging'

    def __call__(self, environ, start_response):
        try:
            for chunk in self.application(environ, start_response):
                yield chunk
        except (BaseException, Exception) as error:
            # Save / log error
            request = make_request(self.config, environ)

            try:
                small_message = '%s: %s' % (error.__class__.__name__, force_string(error))
            except (BaseException, Exception):
                small_message = error

            try:
                request.api.logging.log_critical(
                    'internal_server_error',
                    str(small_message))
            except (BaseException, Exception):
                print ''.join(format_exception(*sys.exc_info()))

            internal_server_error = HTTPInternalServerError()
            headers = [('Content-type', 'application/json')]
            start_response(internal_server_error.status, headers)
            yield format_error_to_json(internal_server_error, request=request)
