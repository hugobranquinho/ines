# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

from json import dumps

from pyramid.httpexceptions import HTTPException
from webob.response import Response

from ines.exceptions import Error
from ines.utils import format_json_response_values


def errors_json_view(context, request):
    if isinstance(context, Error):
        status = 400
        key = context.key
        message = context.message
    elif isinstance(context, HTTPException):
        if str(context.code).startswith('3'):
            # Redirect Code
            return context

        status = context.code
        key = context.title.lower().replace(' ', '_')
        message = context.title
    else:
        raise

    values = format_json_response_values(status, key, message)
    return request.render_to_response(
        'json',
        values=values,
        status=status)


def not_found_api_app(settings):
    def call_not_found(environ, start_response):
        values = format_json_response_values(404, 'not_found', u'Not Found')
        response = Response(
            body=dumps(values),
            status=404,
            content_type='application/json')
        return response(environ, start_response)

    return call_not_found
