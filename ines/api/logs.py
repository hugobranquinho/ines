# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

import datetime
import sys
from traceback import format_exception

from pyramid.router import Router
from pyramid.httpexceptions import HTTPInternalServerError

from ines import MIDDLEWARES_POSITION
from ines.api import BaseSessionClass
from ines.api import BaseSession
from ines.convert import force_string
from ines.utils import format_json_response


NOW_DATE = datetime.datetime.now
MISSING_RESPONSE = object()


class LoggingMiddleware(object):
    def __init__(self, application):
        self.application = application

        # Find application Router
        self.router = None
        maybe_router = self.application
        while not self.router:
            maybe_router = maybe_router.application
            if isinstance(maybe_router, Router):
                self.router = maybe_router

    def __call__(self, environ, start_response):
        try:
            for chunk in self.application(environ, start_response):
                yield chunk
        except Exception as error:
            internal_server_error = HTTPInternalServerError()
            error_key = internal_server_error.title.lower().replace(' ', '_')

            try:
                type_, value, tb = sys.exc_info()
            except:
                raise
            else:
                error = ''.join(format_exception(type_, value, tb))

                request = self.router.request_factory(environ)
                request.registry = self.router.registry
                request.api.logging.log_critical(error_key, error)

            headers = [('Content-type', 'application/json')]
            start_response(internal_server_error.status, headers)

            yield format_json_response(
                internal_server_error.code,
                error_key,
                internal_server_error.title)


class BaseLogSessionClass(BaseSessionClass):
    __api_name__ = 'logging'
    __middlewares__ = [(MIDDLEWARES_POSITION['logging'], LoggingMiddleware)]


class BaseLogSession(BaseSession):
    __api_name__ = 'logging'

    def log(self, code, message, level='INFO'):
        header = ('-' * 30) + ' ' + level + ' ' + ('-' * 30)
        print header

        arguments = [
            ('Application', self.request.application_name),
            ('Code', code),
            ('URL', self.request.url),
            ('Date', NOW_DATE()),
            ('Language', self.request.locale_name),
            ('IP address', self.request.ip_address)]

        if self.request.authenticated:
            arguments.extend([
                ('Session type', self.request.authenticated.session_type),
                ('Session id', self.request.authenticated.session_id)])
        else:
            arguments.append(('Session', 'Without autentication'))

        bigger = max(len(k) for k, v in arguments)
        for key, value in arguments:
            print key, ' ' * (bigger - len(key)), ':', force_string(value)

        print
        try:
            message = force_string(message)
            for line in message.split('\n'):
                print '  %s' % line
        except:
            pass

        print '-' * len(header)

    def log_debug(self, code, message):
        if self.config.debug:
            return self.log(
                code,
                message,
                level='DEBUG')

    def log_info(self, code, message):
        return self.log(
            code,
            message,
            level='INFO')

    def log_warning(self, code, message):
        return self.log(
            code,
            message,
            level='WARNING')

    def log_error(self, code, message):
        return self.log(
            code,
            message,
            level='ERROR')

    def log_critical(self, code, message):
        return self.log(
            code,
            message,
            level='CRITICAL')
