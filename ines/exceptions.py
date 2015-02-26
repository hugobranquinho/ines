# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

from pyramid.httpexceptions import HTTPUnauthorized


class Error(Exception):
    def __init__(self, key, message, exception=None):
        Exception.__init__(self, message)

        self.key = key
        self.message = message
        self.exception = exception


class HTTPTokenExpired(HTTPUnauthorized):
    explanation = u'Token expired'
