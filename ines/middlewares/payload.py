# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

from json import loads

from pyramid.compat import is_nonstr_iter
from webob.request import environ_add_POST

from ines.convert import force_string
from ines.exceptions import HTTPInvalidJSONPayload
from ines.middlewares import Middleware
from ines.utils import get_content_type
from ines.utils import format_error_to_json


class Payload(Middleware):
    name = 'payload'

    def __call__(self, environ, start_response):
        content_type = get_content_type(environ.get('CONTENT_TYPE'))
        if content_type == 'application/json' and 'wsgi.input' in environ:
            body = environ['wsgi.input'].read()
            if body:
                arguments = []
                try:
                    body_json = loads(body)
                    for key, values in dict(body_json).items():
                        if not is_nonstr_iter(values):
                            values = [values]
                        value = ','.join('' if v is None else force_string(v) for v in values)
                        arguments.append('%s=%s' % (force_string(key), value))
                    body = '&'.join(arguments)
                except (ValueError, UnicodeEncodeError):
                    headers = [('Content-type', 'application/json')]
                    error = HTTPInvalidJSONPayload()
                    start_response(error.status, headers)
                    return format_error_to_json(error)

            environ_add_POST(environ, body or '')

        return self.application(environ, start_response)
