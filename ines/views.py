# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

from pyramid.httpexceptions import HTTPException

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
