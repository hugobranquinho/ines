# -*- coding: utf-8 -*-

from pyramid.decorator import reify
from zope.interface import implementer

from ines.interfaces import IBaseSessionManager


@implementer(IBaseSessionManager)
class BaseSessionManager(object):
    def __init__(self, config, session, api_name):
        self.config = config
        self.session = session
        self.api_name = api_name

    def __call__(self, request):
        return self.session(self, request)


class BaseSession(object):
    def __init__(self, api_session_manager, request):
        self.api_session_manager = api_session_manager
        self.package_name = self.api_session_manager.config.package_name
        self.application_name = self.api_session_manager.config.application_name
        self.config = self.api_session_manager.config
        self.registry = self.config.registry
        self.settings = self.registry.settings
        self.request = request

    def __getattr__(self, name):
        if self.__api_name__ == name:
            return self

        extension = self.registry.queryUtility(IBaseSessionManager, name=name)
        if not extension:
            raise AttributeError(u'Missing method %s on extension %s' % (name, self.__api_name__))

        attribute = extension(self.request)
        setattr(self, name, attribute)
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


class BaseAPISession(BaseSession):
    __api_name__ = 'api'
