# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

from cgi import FieldStorage
from io import BufferedReader

from pyramid.decorator import reify
from pyramid.interfaces import IAuthenticationPolicy
from pyramid.interfaces import IAuthorizationPolicy
from pyramid.renderers import render_to_response
from pyramid.request import Request
from webob.multidict import MultiDict
from webob.multidict import NoVars
from webob.request import FakeCGIBody

from ines import APPLICATIONS
from ines.convert import force_unicode
from ines.exceptions import Error
from ines.i18n import get_localizer
from ines.interfaces import IBaseSessionManager
from ines.utils import InfiniteDict


class inesRequest(Request):
    @reify
    def cache(self):
        return InfiniteDict()

    @reify
    def api(self):
        return (
            self.registry
            .queryUtility(IBaseSessionManager, name='api')
            (self))

    @reify
    def settings(self):
        return self.registry.settings

    @reify
    def debug(self):
        return self.registry.config.debug

    @reify
    def package_name(self):
        return self.registry.package_name

    @reify
    def application_name(self):
        return self.registry.application_name

    @reify
    def applications(self):
        return ApplicationsConnector(self)

    def render_to_response(self, renderer, values=None, status=None):
        if status is not None:
            self.response.status = int(status)
        return render_to_response(renderer, values or {}, self)

    @property
    def translator(self):
        return self.get_translator(self.locale_name)

    def get_translator(self, locale_name):
        localizer = self.localizer
        if locale_name != localizer.locale_name:
            localizer = get_localizer(self.registry, locale_name)
        return localizer.translator

    @reify
    def ip_address(self):
        value = self.environ.get('HTTP_X_FORWARDED_FOR') or \
                self.environ.get('REMOTE_ADDR')
        if value:
            return force_unicode(value)

        message = u'Missing IP Address'
        raise Error('ip_address', message)

    @reify
    def authentication(self):
        return self.registry.queryUtility(IAuthenticationPolicy)

    @reify
    def authorization(self):
        return self.registry.queryUtility(IAuthorizationPolicy)

    @reify
    def authenticated(self):
        if (self.authentication
            and hasattr(self.authentication, 'get_authenticated_session')):
            return self.authentication.get_authenticated_session(self)

    @reify
    def DELETE(self):
        if self.method != 'DELETE':
            return NoVars('Not a DELETE request')

        content_type = self.content_type
        if content_type not in (
                '',
                'application/x-www-form-urlencoded',
                'multipart/form-data'):
            # Not an HTML form submission
            return NoVars(
                'Not an HTML delete submission (Content-Type: %s)'
                % content_type)

        self._check_charset()
        if self.is_body_seekable:
            self.body_file_raw.seek(0)
        fs_environ = self.environ.copy()

        # FieldStorage assumes a missing CONTENT_LENGTH, but a
        # default of 0 is better:
        fs_environ.setdefault('CONTENT_LENGTH', '0')
        fs_environ['QUERY_STRING'] = ''

        fs = FieldStorage(
            fp=self.body_file,
            environ=fs_environ,
            keep_blank_values=True)
        delete_values = MultiDict.from_fieldstorage(fs)

        ctype = self._content_type_raw or 'application/x-www-form-urlencoded'
        f = FakeCGIBody(delete_values, ctype)
        self.body_file = BufferedReader(f)
        return delete_values


class ApplicationsConnector(object):
    def __init__(self, request):
        self._request = request

    def __getattribute__(self, key):
        try:
            attribute = object.__getattribute__(self, key)
        except AttributeError as error:
            config = APPLICATIONS.get(key)
            if config is None:
                message = u'Missing application %s' % key
                raise NotImplementedError(message)

            if self._request.application_name == key:
                attribute = self._request.api
            else:
                attribute = (
                    config.registry
                    .queryUtility(IBaseSessionManager, name='api')
                    (self._request))

            setattr(self, key, attribute)

        return attribute
