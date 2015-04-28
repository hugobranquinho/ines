# -*- coding: utf-8 -*-

from pyramid.httpexceptions import HTTPBadRequest
from pyramid.httpexceptions import HTTPUnauthorized


class Error(Exception):
    def __init__(self, key, message, exception=None):
        super(Exception, self).__init__(message)

        self.key = key
        self.message = message
        self.exception = exception


class HTTPTokenExpired(HTTPUnauthorized):
    explanation = u'Token expired'


class HTTPInvalidJSONPayload(HTTPBadRequest):
    explanation = u'Invalid json request'


class NoMoreDates(Error):
    pass
