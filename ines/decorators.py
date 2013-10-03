# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
# 
# @author Hugo Branquinho <hugobranq@gmail.com>
# 
# $Id$

""" This module provides decorators to use in :app:`Pyramid` applications.
"""

from traceback import format_exc
from traceback import format_exception_only

from pyramid.exceptions import Forbidden
from pyramid.exceptions import NotFound
from translationstring import TranslationString

from ines import _
from ines.exceptions import Error
from ines.exceptions import RESTError
from ines.renderers.json import json_renderer
from ines.renderers.json import jsonp_renderer


class JSONDecorator(object):
    def __init__(self, func, json_render, no_cache=False):
        self.func = func
        self.json_render = json_render
        self.no_cache = no_cache

    def render(
            self,
            request,
            result=None,
            error=None,
            status=200,
            errors=None,
            **kwargs):

        if isinstance(error, TranslationString):
            error = request.translate(error)

        response = {'result': result, 'error': error, 'errors': errors}
        if kwargs:
            response.update(kwargs)

        return self.json_render(request, response, status, self.no_cache)

    def forbidden(self, context, request):
        return self.render(request, error=_(u'Forbidden'), status=403)

    def __call__(self, context, request):
        status = None
        result = None
        error_message = None
        errors = {}

        try:
            result = self.func(context, request)

        except Forbidden:
            status = 403
            error_message = _(u'Forbidden')

        except NotFound:
            status = 404
            error_message = _(u'Not Found')

        except Error as error:
            status = 400
            if isinstance(error, RESTError) and error.status:
                status = error.status

            error_message = error.msg
            errors = error.asdict(request)

        except Exception as error:
            status = 500
            error_lines = format_exception_only(type(error), error)
            error_message = '\n'.join(error_lines)

            request.api.log_critical('undefined_json_error', format_exc())

        return self.render(
                   request,
                   result=result,
                   error=error_message,
                   status=status,
                   errors=errors)


def json_decorator(no_cache=False):
    def decorator(func):
        return JSONDecorator(func, json_renderer, no_cache)
    return decorator


def jsonp_decorator(no_cache=False):
    def decorator(func):
        return JSONDecorator(func, jsonp_renderer, no_cache)
    return decorator
