# -*- coding: utf-8 -*-

from cgi import FieldStorage
from io import BufferedReader

from pyramid.decorator import reify
from pyramid.httpexceptions import HTTPFound
from pyramid.interfaces import IAuthenticationPolicy
from pyramid.interfaces import IAuthorizationPolicy
from pyramid.interfaces import IRequestFactory
from pyramid.renderers import render_to_response
from pyramid.request import Request
from pyramid.settings import asbool
from six import moves
from six import u
from webob.compat import parse_qsl_text
from webob.multidict import MultiDict
from webob.multidict import NoVars
from webob.request import FakeCGIBody

from ines import APPLICATIONS
from ines.convert import to_string
from ines.convert import to_unicode
from ines.exceptions import Error
from ines.i18n import translate_factory
from ines.interfaces import IBaseSessionManager
from ines.utils import infinitedict


unquote = moves.urllib.parse.unquote


class InesRequest(Request):
    @reify
    def session_cache(self):
        return infinitedict()

    @reify
    def cache(self):
        return self.registry.config.cache

    @reify
    def api(self):
        api_session = self.registry.queryUtility(IBaseSessionManager, name='api')
        if api_session is not None:
            return api_session(self)

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

    @reify
    def is_production_environ(self):
        return asbool(self.settings['is_production_environ'])

    def render_to_response(self, renderer, values=None, status=None):
        if status is not None:
            self.response.status = int(status)
        return render_to_response(renderer, values or {}, self)

    @reify
    def translator(self):
        return self.get_translator()

    def get_translator(self, locale_name=None):
        return translate_factory(self, locale_name)

    def translate(self, tstring, **kwargs):
        return self.translator(tstring, **kwargs)

    @reify
    def ip_address(self):
        value = self.environ.get('HTTP_X_FORWARDED_FOR') or self.environ.get('REMOTE_ADDR')
        if value:
            return to_unicode(value)

        message = u('Missing IP Address')
        raise Error('ip_address', message)

    @reify
    def authentication(self):
        return self.registry.queryUtility(IAuthenticationPolicy)

    @reify
    def authorization(self):
        return self.registry.queryUtility(IAuthorizationPolicy)

    @reify
    def authenticated(self):
        if self.authentication and hasattr(self.authentication, 'get_authenticated_session'):
            return self.authentication.get_authenticated_session(self)

    @reify
    def DELETE(self):
        if self.method != 'DELETE':
            return NoVars('Not a DELETE request')

        content_type = self.content_type
        if content_type in (
                'application/x-www-form-urlencoded',
                'multipart/form-data'):

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

        else:
            data = parse_qsl_text(self.environ.get('QUERY_STRING', ''))
            return MultiDict(data)

    def redirect_to_url(self, url):
        url = unquote(to_string(url))
        return HTTPFound(location=url)

    def redirect_to_self(self, *elements, **kw):
        kw.update(self.context)
        url = self.route_url(self.matched_route.name, *elements, **kw)
        return self.redirect_to_url(url)

    def redirect_to_route(self, route_name, *elements, **kw):
        url = self.route_url(route_name, *elements, **kw)
        return self.redirect_to_url(url)


class ApplicationsConnector(object):
    def __init__(self, request):
        self.request = request

    def __getattr__(self, key):
        config = APPLICATIONS.get(key)
        if config is None:
            raise AttributeError('Missing application %s' % key)

        if self.request.application_name == key:
            attribute = self.request.api
        else:
            session_manager = config.registry.queryUtility(
                IBaseSessionManager,
                name='api')
            attribute = session_manager(self.request)

        setattr(self, key, attribute)
        return attribute

    def asdict(self):
        return dict((k, getattr(self, k)) for k in APPLICATIONS.keys())


def make_request(config, environ=None):
    request_factory = config.registry.queryUtility(IRequestFactory)
    request = request_factory(environ or {})
    request.registry = config.registry
    return request
