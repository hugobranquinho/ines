# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

from json import loads

from pyramid.compat import is_nonstr_iter
from pyramid.httpexceptions import HTTPBadRequest
from webob.request import environ_add_POST

from ines.convert import force_string
from ines.middlewares import Middleware
from ines.utils import format_json_response


class Payload(Middleware):
    name = 'payload'

    def __call__(self, environ, start_response):
        if (environ.get('CONTENT_TYPE') == 'application/json'
            and 'wsgi.input' in environ):
            body = environ['wsgi.input'].read()

            arguments = []
            try:
                body_json = loads(body)
                for key, value in dict(body_json).items():
                    if is_nonstr_iter(value):
                        value = ','.join(force_string(v) for v in value)
                    arguments.append('%s=%s' % (force_string(key), force_string(value)))
            except:
                bad_request = HTTPBadRequest()
                headers = [('Content-type', 'application/json')]
                start_response(bad_request.status, headers)
                return [format_json_response(
                    bad_request.code,
                    bad_request.title.lower().replace(' ', '_'),
                    u'Invalid json request')]

            encoded_body = '&'.join(arguments)
            environ_add_POST(environ, encoded_body)

        return self.application(environ, start_response)
