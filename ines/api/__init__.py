# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

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
    def __init__(self, api_session_class, request):
        self.api_session_class = api_session_class
        self.package_name = self.api_session_class.config.package_name
        self.application_name = self.api_session_class.config.application_name
        self.config = self.api_session_class.config
        self.registry = self.config.registry
        self.settings = self.registry.settings
        self.request = request

    def __getattribute__(self, key):
        try:
            attribute = object.__getattribute__(self, key)
        except AttributeError as error:
            if self.__api_name__ == key:
                return self

            extension = self.registry.queryUtility(IBaseSessionManager, name=key)
            if not extension:
                raise
            else:
                attribute = extension(self.request)
                setattr(self, key, attribute)

        return attribute

    def __contains__(self, key):
        return (
            self.registry
            .queryUtility(IBaseSessionManager, name=key) is not None)

    @reify
    def applications(self):
        return self.request.applications


class BaseAPISession(BaseSession):
    __api_name__ = 'api'
