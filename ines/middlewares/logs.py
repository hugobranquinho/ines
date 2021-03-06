# -*- coding: utf-8 -*-

import sys
from traceback import format_exception

from pyramid.httpexceptions import HTTPInternalServerError

from ines.config import APIConfigurator
from ines.convert import to_string
from ines.middlewares import Middleware
from ines.request import make_request
from ines.utils import format_error_response_to_json


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
                small_message = '%s: %s' % (error.__class__.__name__, to_string(error))
            except (BaseException, Exception):
                small_message = error

            print_message = True
            if request.api is not None:
                api_manager = self.settings.get('api_manager')
                if api_manager is not None:
                    logging_api_name = api_manager.__api_name__
                else:
                    logging_api_name = 'logging'

                try:
                    getattr(request.api, logging_api_name).log_critical(
                        'internal_server_error',
                        str(small_message))
                except (BaseException, Exception):
                    pass
                else:
                    print_message = False

            if print_message:
                print(''.join(format_exception(*sys.exc_info())))

            if isinstance(request.registry.config, APIConfigurator):
                headers = [('Content-type', 'application/json')]
                internal_server_error = HTTPInternalServerError()
                start_response(internal_server_error.status, headers)
                response = format_error_response_to_json(internal_server_error, request=request)
                yield response
            else:
                return_default = False
                error_view = self.config.settings.get('errors.interface.global_error_view')
                if error_view is None:
                    return_default = True
                else:
                    try:
                        response = error_view(error, request)
                    except Exception:
                        return_default = True
                    else:
                        start_response(response.status, response.headerlist)
                        yield response.body

                if return_default:
                    internal_server_error = HTTPInternalServerError()
                    for response in internal_server_error(environ, start_response):
                        yield response
