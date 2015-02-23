# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

from json import loads

from pyramid.compat import is_nonstr_iter
from webob.request import environ_add_POST

from ines.convert import force_string
from ines.exceptions import Error
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
                    for key, value in dict(body_json).items():
                        if is_nonstr_iter(value):
                            value = ','.join(force_string(v) for v in value)
                        arguments.append('%s=%s' % (force_string(key), force_string(value)))
                except (ValueError, UnicodeEncodeError):
                    headers = [('Content-type', 'application/json')]
                    start_response(400, headers)
                    error = Error('json_loads', u'Invalid json request')
                    return format_error_to_json(error)

                body = '&'.join(arguments)

            environ_add_POST(environ, body or '')

        return self.application(environ, start_response)
