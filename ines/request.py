# -*- coding: utf-8 -*-

from cgi import FieldStorage
from io import BufferedReader
from os.path import isabs
from urllib.parse import unquote, urljoin, urlparse, urlunparse

from pyramid.compat import url_quote, WIN
from pyramid.decorator import reify
from pyramid.httpexceptions import HTTPFound
from pyramid.interfaces import IAuthenticationPolicy, IAuthorizationPolicy, IRequestFactory, IStaticURLInfo
from pyramid.path import caller_package
from pyramid.renderers import render_to_response
from pyramid.request import Request
from pyramid.threadlocal import get_current_registry
from pyramid.url import parse_url_overrides
from webob.compat import parse_qsl_text
from webob.multidict import MultiDict, NoVars
from webob.request import FakeCGIBody

from ines import APPLICATIONS
from ines.convert import to_string
from ines.exceptions import Error
from ines.i18n import translate_factory
from ines.interfaces import IBaseSessionManager
from ines.utils import infinitedict, user_agent_is_mobile


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
        return self.registry.config.is_production_environ

    @reify
    def is_mobile(self):
        return user_agent_is_mobile(self.user_agent)

    def render_to_response(self, renderer, values=None, status=None):
        if status is not None:
            self.response.status_code = int(status)

        response = render_to_response(
            renderer_name=renderer,
            value=values or {},
            request=self,
            package=self.registry.config.package,
            response=self.response)
        return response

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
            return to_string(value)
        else:
            raise Error('ip_address', 'Missing IP Address')

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

    @property
    def current_route_name(self):
        if self.matched_route:
            return self.matched_route.name

    def self_route_url(self, *elements, **kw):
        if self.current_route_name:
            return self.route_url(self.current_route_name, *elements, **kw)
        else:
            return self.url

    def redirect_to_url(self, url, headers=None):
        url = unquote(to_string(url))
        return HTTPFound(location=url, headers=headers)

    def redirect_to_self(self, *elements, **kw):
        headers = kw.pop('headers', None)
        kw.update(self.context)
        url = self.self_route_url(*elements, **kw)
        return self.redirect_to_url(url, headers)

    def redirect_to_route(self, route_name, *elements, **kw):
        headers = kw.pop('headers', None)
        url = self.route_url(route_name, *elements, **kw)
        return self.redirect_to_url(url, headers)

    @reify
    def referer_path_url(self):
        if self.referer:
            url = urlparse(self.referer)._replace(query=None).geturl()
            return to_string(url)

    def static_url(self, path, **kw):
        if not isabs(path) and ':' not in path:
            package = caller_package()
            package_name = package.__name__
            path = '%s:%s' % (package_name, path)
        else:
            package_name = path.split(':', 1)[0]

        if package_name == self.package_name:
            registry = self.registry
        else:
            for application_name, config in APPLICATIONS.items():
                if config.package_name == package_name:
                    registry = config.registry
                    break
            else:
                registry = get_current_registry() # b/c

        info = registry.queryUtility(IStaticURLInfo)
        if info is None:
            raise ValueError('No static URL definition matching %s' % path)

        registrations = info._get_registrations(registry)
        api_route_url = getattr(self.applications, registry.application_name).route_url

        for (url, spec, route_name, cachebust) in registrations:
            if path.startswith(spec):
                subpath = path[len(spec):]
                if WIN: # pragma: no cover
                    subpath = subpath.replace('\\', '/') # windows
                if cachebust:
                    subpath, kw = cachebust(subpath, kw)
                if url is None:
                    kw['subpath'] = subpath
                    return api_route_url(route_name, **kw)
                else:
                    app_url, scheme, host, port, qs, anchor = parse_url_overrides(kw)
                    parsed = urlparse(url)
                    if not parsed.scheme:
                        url = urlunparse(parsed._replace(scheme=self.environ['wsgi.url_scheme']))
                    subpath = url_quote(subpath)
                    result = urljoin(url, subpath)
                    return result + qs + anchor

        raise ValueError('No static URL definition matching %s' % path)


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
        return {k: getattr(self, k) for k in self.names()}

    def names(self):
        return list(APPLICATIONS.keys())


def make_request(config, environ=None):
    request_factory = config.registry.queryUtility(IRequestFactory)
    request = request_factory(environ or {})
    request.registry = config.registry
    return request
