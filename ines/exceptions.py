# -*- coding: utf-8 -*-

from pyramid.httpexceptions import HTTPBadRequest, HTTPClientError, HTTPUnauthorized


class Error(Exception):
    def __init__(self, key, message, exception=None, title=None):
        super(Error, self).__init__(message)

        self.key = key
        self.message = message
        self.exception = exception
        self.title = title


class HTTPBrowserUpgrade(HTTPClientError):
    code = 403
    explanation = 'Browser upgrade required'


class HTTPTokenExpired(HTTPUnauthorized):
    explanation = 'Token expired'


class HTTPInvalidJSONPayload(HTTPBadRequest):
    explanation = 'Invalid json request'


class NoMoreDates(Error):
    pass


class LockTimeout(Exception):
    def __init__(self, message, locked_path):
        super(LockTimeout, self).__init__(message)
        self.locked_path = locked_path
