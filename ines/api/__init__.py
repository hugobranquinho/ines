# -*- coding: utf-8 -*-

from pyramid.decorator import reify
from pyramid.interfaces import IRoutesMapper
from pyramid.url import _join_elements
from pyramid.url import parse_url_overrides
from zope.interface import implementer

from ines.interfaces import IBaseSessionManager


@implementer(IBaseSessionManager)
class BaseSessionManager(object):
    def __init__(self, config, session, api_name=None):
        if api_name and (not hasattr(self, '__api_name__') or self.__api_name__ != api_name):
            self.__api_name__ = api_name

        self.config = config
        self.session = session

        pattern = '%s.' % self.__api_name__
        self.settings = dict(
            (key.replace(pattern, '', 1), value)
            for key, value in config.settings.items()
            if key.startswith(pattern))

    def __call__(self, request):
        return self.session(self, request)


class BaseSession(object):
    def __init__(self, api_session_manager, request):
        self.api_session_manager = api_session_manager
        self.package_name = self.api_session_manager.config.package_name
        self.application_name = self.api_session_manager.config.application_name
        self.config = self.api_session_manager.config
        self.registry = self.config.registry
        self.settings = self.api_session_manager.settings
        self.request = request

    def __getattribute__(self, name):
        try:
            attribute = object.__getattribute__(self, name)
        except AttributeError:
            if object.__getattribute__(self, '__api_name__') == name:
                attribute = self
            else:
                extension = self.registry.queryUtility(IBaseSessionManager, name=name)
                if not extension:
                    raise
                attribute = extension(self.request)
            object.__setattr__(self, name, attribute)

        return attribute


    def __contains__(self, key):
        return (
            self.registry
            .queryUtility(IBaseSessionManager, name=key) is not None)

    @reify
    def cache(self):
        return self.config.cache

    @reify
    def applications(self):
        return self.request.applications

    def route_url(self, route_name, *elements, **kw):
        """ See: pyramid.URLMethodsMixin.route_url
        """

        mapper = self.registry.getUtility(IRoutesMapper)
        route = mapper.get_route(route_name)

        if route is None:
            raise KeyError('No such route named %s' % route_name)

        if route.pregenerator is not None:
            elements, kw = route.pregenerator(self, elements, kw)

        app_url, scheme, host, port, qs, anchor = parse_url_overrides(kw)

        if app_url is None:
            if scheme is not None or host is not None or port is not None:
                app_url = self._partial_application_url(scheme, host, port)
            else:
                app_url = self.request.host_url
                if self.config.settings.get('app_url'):
                    app_url = '/'.join(s.strip('/') for s in [app_url, self.config.settings['app_url']])

        path = route.generate(kw) # raises KeyError if generate fails

        if elements:
            suffix = _join_elements(elements)
            if not path.endswith('/'):
                suffix = '/' + suffix
        else:
            suffix = ''

        return app_url + path + suffix + qs + anchor


class BaseAPISession(BaseSession):
    __api_name__ = 'api'
