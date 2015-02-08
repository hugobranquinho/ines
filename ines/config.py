# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

from importlib import import_module
from uuid import uuid4

from pkg_resources import get_distribution
from pyramid.compat import is_nonstr_iter
from pyramid.config import Configurator
from pyramid.decorator import reify
from pyramid.httpexceptions import HTTPException
from pyramid.path import caller_package
from pyramid.renderers import JSONP
from pyramid.security import Authenticated
from pyramid.security import NO_PERMISSION_REQUIRED
from pyramid.settings import asbool
from pyramid.static import static_view

from ines import APPLICATIONS
from ines.api import BaseSession
from ines.api import BaseSessionManager
from ines.authentication import ApplicationHeaderAuthenticationPolicy
from ines.authorization import INES_POLICY
from ines.authorization import TokenAuthorizationPolicy
from ines.interfaces import IBaseSessionManager
from ines.middlewares import DEFAULT_MIDDLEWARE_POSITION
from ines.path import find_class_on_module
from ines.path import get_object_on_path
from ines.request import inesRequest
from ines.route import RootFactory
from ines.utils import WarningDict


class APIConfigurator(Configurator):
    def __init__(
            self,
            application_name=None,
            global_settings=None,
            **kwargs):

        if 'registry' in kwargs:
            for application_config in APPLICATIONS.values():
                if application_config.registry is kwargs['registry']:
                    # Nothing to do where. .scan() Configuration
                    return super(APIConfigurator, self).__init__(**kwargs)

        if 'package' not in kwargs:
            kwargs['package'] = caller_package()

        settings = kwargs['settings'] = dict(kwargs.get('settings') or {})
        kwargs['settings'].update(global_settings or {})

        # Define pyramid debugs
        settings['debug'] = asbool(settings.get('debug', False))
        if 'reload_all' not in settings:
            settings['reload_all'] = settings['debug']
        if 'debug_all' not in settings:
            settings['debug_all'] = settings['debug']
        if 'reload_templates' not in settings:
            settings['reload_templates'] = settings['debug']

        if 'root_factory' not in kwargs:
            kwargs['root_factory'] = RootFactory
        if 'request_factory' not in kwargs:
            kwargs['request_factory'] = inesRequest

        super(APIConfigurator, self).__init__(**kwargs)

        self.registry.config = self
        self.registry.package_name = self.registry.__name__

        # Define application_name
        self.application_name = application_name or self.package_name
        self.registry.application_name = self.application_name

        # Find extensions on settings
        bases = WarningDict('Duplicate name "{key}" for API Class')
        sessions = WarningDict('Duplicate name "{key}" for API Session')
        for key, value in self.settings.items():
            if key.startswith('api.extension.'):
                options = key.split('.', 3)[2:]
                if len(options) == 1:
                    name, option = options[0], 'session_path'
                else:
                    name, option = options

                if option == 'session_path':
                    sessions[name] = get_object_on_path(value)
                elif option == 'class_path':
                    bases[name] = get_object_on_path(value)

        # Find sessions on module
        for session in find_class_on_module(self.package, BaseSession):
            sessions[session.__api_name__] = session

        # Find class on module
        for session_class in find_class_on_module(
                self.package,
                BaseSessionManager):
            bases[session_class.__api_name__] = session_class

        # Find default session class
        for session_class in find_class_on_module(
                'ines.api',
                BaseSessionManager):
            if session_class.__api_name__ not in bases:
                bases[session_class.__api_name__] = session_class

        # Define extensions
        for api_name, session in sessions.items():
            session_class = (
                bases.get(api_name, BaseSessionManager)
                (self, session, api_name))

            self.registry.registerUtility(
                session_class,
                IBaseSessionManager,
                name=api_name)

        # Middlewares
        self.middlewares = []

        # Register package
        APPLICATIONS[self.application_name] = self

    @reify
    def settings(self):
        return self.registry.settings

    @reify
    def debug(self):
        return self.settings.get('debug')

    def add_apidocjs_view(
            self, pattern='docs', cache_max_age=86400,
            resource_name='apidocjs'):

        static_func = static_view(
            '%s:%s/' % (self.package_name, resource_name),
            package_name=self.package_name,
            use_subpath=True,
            cache_max_age=cache_max_age)

        self.add_route(resource_name, pattern='%s*subpath' % pattern)
        self.add_view(
            route_name=resource_name,
            view=static_func,
            permission=INES_POLICY)

    @reify
    def version(self):
        return get_distribution(self.package_name).version

    def add_routes(self, *routes):
        for arguments in routes:
            if not arguments:
                raise ValueError('Define some arguments')
            elif isinstance(arguments, dict):
                self.add_route(**arguments)
            elif not is_nonstr_iter(arguments):
                self.add_route(arguments)
            else:
                length = len(arguments)
                kwargs = {'name': arguments[0]}
                if length > 1:
                    kwargs['pattern'] = arguments[1]

                self.add_route(**kwargs)

    def add_view(self, *args, **kwargs):
        if 'permission' not in kwargs:
            # Force permission validation
            kwargs['permission'] = INES_POLICY
        return super(APIConfigurator, self).add_view(*args, **kwargs)

    def make_wsgi_app(self, install_middlewares=True):
        # Scan all package routes
        self.scan(self.package_name, categories=['pyramid'])
        app = super(APIConfigurator, self).make_wsgi_app()

        if install_middlewares:
            for name, extension in (
                    self.registry
                    .getUtilitiesFor(IBaseSessionManager)):
                if hasattr(extension, '__middlewares__'):
                    for middleware in extension.__middlewares__:
                        self.install_middleware(middleware)

            if self.middlewares:
                middlewares = []
                for middleware in self.middlewares:
                    settings = {}
                    search_key = 'middleware.%s.' % middleware.name
                    for key, value in self.settings.items():
                        if key.startswith(search_key):
                            key = key.split(search_key, 1)[1]
                            settings[key] = value

                    default_position = (
                        getattr(middleware, 'position',
                            DEFAULT_MIDDLEWARE_POSITION.get(middleware.name)))
                    position = settings.get('position', default_position)
                    middlewares.append((position, middleware, settings))

                middlewares.sort()
                for position, middleware, settings in middlewares:
                    app = middleware(self, app, **settings)

        return app

    def install_middleware(self, middleware):
        self.middlewares.append(middleware)

    def add_errors_handler(self):
        # Set JSON handler
        self.add_view(
            view='ines.views.errors_json_view',
            context=HTTPException,
            permission=NO_PERMISSION_REQUIRED)
        self.add_view(
            view='ines.views.errors_json_view',
            context=Exception,
            permission=NO_PERMISSION_REQUIRED)

    def set_token_policy(
            self,
            application_name=None,
            header_key='Authorization'):

        # Authentication Policy
        application_name = (
            application_name
            or self.settings['policy.application_name'])

        authentication_policy = ApplicationHeaderAuthenticationPolicy(
            application_name,
            header_key=header_key)
        self.set_authentication_policy(authentication_policy)

        authorization_policy = TokenAuthorizationPolicy(application_name)
        self.set_authorization_policy(authorization_policy)

    def install_cors_middleware(self, settings=None):
        from ines.middlewares.cors import Cors
        self.install_middleware(Cors)

    def set_ines_defaults(self):
        # Add services documentation
        self.add_apidocjs_view()
        # Add errors handler
        self.add_errors_handler()
        # Add header token authentication
        self.set_token_policy()
        # Use cors
        self.install_cors_middleware(self.settings)
