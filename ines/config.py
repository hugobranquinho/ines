# -*- coding: utf-8 -*-

try:
    from collections import OrderedDict
except ImportError:
    OrderedDict = dict

from inspect import getargspec

from colander import Invalid
from pkg_resources import get_distribution
from pyramid.config import Configurator
from pyramid.decorator import reify
from pyramid.httpexceptions import HTTPClientError
from pyramid.path import caller_package
from pyramid.security import NO_PERMISSION_REQUIRED
from pyramid.settings import asbool
from pyramid.static import static_view

from ines import API_CONFIGURATION_EXTENSIONS
from ines import APPLICATIONS
from ines import DEFAULT_METHODS
from ines import DEFAULT_RENDERERS
from ines import DEFAULT_CACHE_DIRPATH
from ines.api import BaseSession
from ines.api import BaseSessionManager
from ines.api.jobs import BaseJobsManager
from ines.api.jobs import BaseJobsSession
from ines.api.mailer import BaseMailerSession
from ines.authentication import ApplicationHeaderAuthenticationPolicy
from ines.authorization import INES_POLICY
from ines.authorization import TokenAuthorizationPolicy
from ines.cache import SaveMe
from ines.cache import SaveMeMemcached
from ines.convert import maybe_list
from ines.exceptions import Error
from ines.interfaces import IBaseSessionManager
from ines.interfaces import IInputSchemaView
from ines.interfaces import IOutputSchemaView
from ines.interfaces import ISchemaView
from ines.middlewares import DEFAULT_MIDDLEWARE_POSITION
from ines.path import find_class_on_module
from ines.path import get_object_on_path
from ines.views.postman import PostmanCollection
from ines.views.schema import SchemaView
from ines.request import inesRequest
from ines.route import RootFactory
from ines.utils import MissingDict
from ines.utils import WarningDict


def configuration_extensions(setting_key):
    def decorator(wrapped):
        API_CONFIGURATION_EXTENSIONS[setting_key] = wrapped.__name__
        return wrapped
    return decorator


class APIWarningDict(WarningDict):
    def __setitem__(self, key, value):
        if key in self:
            existing_value = self[key]
            existing_path = '%s:%s' % (existing_value.__module__, existing_value.__name__)
            path = '%s:%s' % (value.__module__, value.__name__)
            if existing_path == path:
                # Do nothing!
                return

        super(APIWarningDict, self).__setitem__(key, value)


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
                    super(APIConfigurator, self).__init__(**kwargs)
                    return  # Nothing else to do where

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

        # Define global cache
        cache_settings = {}
        for key, value in self.settings.items():
            if key.startswith('cache.'):
                cache_settings[key[6:]] = value
        cache_type = cache_settings.pop('type', None)
        if cache_type == 'memcached':
            self.cache = SaveMeMemcached(**cache_settings)
        else:
            if 'path' not in cache_settings:
                cache_settings['path'] = DEFAULT_CACHE_DIRPATH
            self.cache = SaveMe(**cache_settings)

        # Find extensions on settings
        bases = APIWarningDict('Duplicated name "{key}" for API Class')
        sessions = APIWarningDict('Duplicated name "{key}" for API Session')
        for key, value in self.settings.items():
            if key.startswith('api.extension.'):
                options = key.split('.', 3)[2:]
                if len(options) == 1:
                    name, option = options[0], 'session_path'
                else:
                    name, option = options
                if option == 'session_path':
                    if isinstance(value, basestring):
                        sessions[name] = get_object_on_path(value)
                    else:
                        sessions[name] = value
                elif option == 'class_path':
                    if isinstance(value, basestring):
                        bases[name] = get_object_on_path(value)
                    else:
                        bases[name] = value

        # Find sessions on module
        for session in find_class_on_module(self.package, BaseSession):
            app_name = getattr(session, '__app_name__', None)
            if not app_name or app_name == application_name:
                sessions[session.__api_name__] = session

        # Find session manager on module
        for session_manager in find_class_on_module(
                self.package,
                BaseSessionManager):
            app_name = getattr(session_manager, '__app_name__', None)
            if not app_name or app_name == application_name:
                bases[session_manager.__api_name__] = session_manager

        # Find default session manager
        for session_manager in find_class_on_module(
                'ines.api',
                BaseSessionManager):
            api_name = getattr(session_manager, '__api_name__', None)
            if api_name and api_name not in bases:
                bases[api_name] = session_manager

        # Define extensions
        for api_name, session in sessions.items():
            session_manager = bases.get(api_name, BaseSessionManager)(self, session, api_name)
            self.registry.registerUtility(
                session_manager,
                provided=IBaseSessionManager,
                name=api_name)

        # Middlewares
        self.middlewares = []

        # Register package
        APPLICATIONS[self.application_name] = self

        # Default translations dirs
        self.add_translation_dirs('colander:locale')
        self.add_translation_dirs('ines:locale')

    @reify
    def settings(self):
        return self.registry.settings

    @reify
    def debug(self):
        return self.settings.get('debug')

    @configuration_extensions('apidocjs')
    def add_apidocjs_view(
            self, pattern='documentation', cache_max_age=86400,
            resource_name='apidocjs'):

        static_func = static_view(
            '%s:%s/' % (self.package_name, resource_name),
            package_name=self.package_name,
            use_subpath=True,
            cache_max_age=int(cache_max_age))

        self.add_route(resource_name, pattern='%s*subpath' % pattern)
        self.add_view(
            route_name=resource_name,
            view=static_func,
            permission=INES_POLICY)

    @reify
    def version(self):
        return get_distribution(self.package_name).version

    def register_input_schema(self, view, route_name, request_method):
        for req_method in maybe_list(request_method) or ['']:
            utility_name = '%s %s' % (route_name or '', req_method or '')
            self.registry.registerUtility(
                view,
                provided=IInputSchemaView,
                name=utility_name)

    def lookup_input_schema(self, route_name, request_method=None):
        request_method = maybe_list(request_method or DEFAULT_METHODS)
        request_method.append('')

        schemas = []
        for req_method in maybe_list(request_method):
            utility_name = '%s %s' % (route_name or '', req_method or '')
            view = self.registry.queryUtility(IInputSchemaView, name=utility_name)
            if view is not None:
                schemas.append(view)
        return schemas

    def register_output_schema(self, view, route_name, request_method):
        for req_method in maybe_list(request_method) or ['']:
            utility_name = '%s %s' % (route_name or '', req_method or '')
            self.registry.registerUtility(
                view,
                provided=IOutputSchemaView,
                name=utility_name)

    def lookup_output_schema(self, route_name, request_method=None):
        request_method = maybe_list(request_method or DEFAULT_METHODS)
        request_method.append('')

        schemas = []
        for req_method in maybe_list(request_method):
            utility_name = '%s %s' % (route_name, req_method or '')
            view = self.registry.queryUtility(IOutputSchemaView, name=utility_name)
            if view is not None:
                schemas.append(view)
        return schemas

    def add_schema_manager(self, view, route_name, pattern, **view_kwargs):
        self.registry.registerUtility(
            view,
            provided=ISchemaView,
            name=route_name)
        self.add_route(name=route_name, pattern=pattern)
        self.add_view(
            view,
            route_name=route_name,
            renderer='json',
            request_method='GET',
            **view_kwargs)

    def add_schema(
            self, pattern, routes_names, route_name=None,
            title=None,
            description=None,
            model=None,
            **view_kwargs):

        if not isinstance(routes_names, dict):
            routes_names = OrderedDict((k, None) for k in maybe_list(routes_names))
        if not routes_names:
            raise Error('schema', 'Define some routes_names')
        if not route_name:
            route_name = '%s_schema' % routes_names.keys()[0]

        view = SchemaView(
            route_name,
            routes_names,
            title=title,
            description=description,
            model=model)
        self.add_schema_manager(view, route_name, pattern, **view_kwargs)

    def add_routes(self, *routes, **kwargs):
        for arguments in routes:
            if not arguments:
                raise ValueError('Define some arguments')
            elif not isinstance(arguments, dict):
                list_arguments = maybe_list(arguments)
                arguments = {'name': list_arguments[0]}
                if len(list_arguments) > 1:
                    arguments['pattern'] = list_arguments[1]
                if len(list_arguments) > 2:
                    arguments['permission'] = list_arguments[2]

            self.add_route(**arguments)

    @configuration_extensions('postman')
    def add_postman_route(
            self, pattern, name='postman', permission=None,
            title=None, description=None):

        kwargs = {}
        if permission:
            kwargs['permission'] = permission

        self.add_route(name=name, pattern=pattern)
        self.add_view(
            PostmanCollection(
                title=title or self.application_name,
                description=description),
            route_name=name,
            renderer='json',
            **kwargs)

    def add_default_renderers(self):
        super(APIConfigurator, self).add_default_renderers()

        from ines import renderers  # for DEFAULT_RENDERERS
        for key, renderer in DEFAULT_RENDERERS.items():
            self.add_renderer(key, renderer)

    def add_view(self, *args, **kwargs):
        if 'permission' not in kwargs:
            # Force permission validation
            kwargs['permission'] = INES_POLICY
        if 'renderer' not in kwargs:
            kwargs['renderer'] = 'json'
        return super(APIConfigurator, self).add_view(*args, **kwargs)

    def lookup_extensions(self):
        found_settings = MissingDict()
        for find_setting_key, method_name in API_CONFIGURATION_EXTENSIONS.items():
            if not find_setting_key.endswith('.'):
                find_setting_key += '.'

            for key, value in self.settings.items():
                if key.startswith(find_setting_key):
                    setting_key = key.split(find_setting_key, 1)[1]
                    found_settings[method_name][setting_key] = value

        for method_name, settings in found_settings.items():
            method = getattr(self, method_name)
            method_settings = dict(
                (argument, settings[argument])
                for argument in getargspec(method).args
                if argument in settings)
            method(**method_settings)

    def make_wsgi_app(self, install_middlewares=True):
        # Find for possible configuration extensions
        self.lookup_extensions()

        # Scan all package routes
        self.scan(self.package_name, categories=['pyramid'])

        # Scan package jobs
        scan_jobs = False
        jobs_manager = None
        for name, extension in self.registry.getUtilitiesFor(IBaseSessionManager):
            if issubclass(extension.session, BaseMailerSession) and 'queue_path' in extension.settings:
                scan_jobs = True
            elif issubclass(extension.session, BaseJobsSession):
                scan_jobs = True
                jobs_manager = extension
            elif isinstance(extension, BaseJobsManager):
                jobs_manager = extension
        if scan_jobs:
            if jobs_manager is None:
                raise ValueError('Please define module for jobs.')
            self.scan(self.package_name, categories=['ines.jobs'], jobs_manager=jobs_manager)
            self.scan('ines', categories=['ines.jobs'], jobs_manager=jobs_manager)

        app = super(APIConfigurator, self).make_wsgi_app()

        if install_middlewares:
            # Look for middlewares in API Sessions
            for name, extension in self.registry.getUtilitiesFor(IBaseSessionManager):
                if hasattr(extension, '__middlewares__'):
                    for extension_middleware in extension.__middlewares__:
                        self.install_middleware(
                            extension_middleware.name,
                            extension_middleware)

            # Define middleware settings
            middlewares_settings = MissingDict()
            for key, value in self.settings.items():
                if key.startswith('middleware.'):
                    maybe_name = key.split('middleware.', 1)[1]
                    if '.' in maybe_name:
                        parts = maybe_name.split('.')
                        setting_key = parts[-1]
                        name = '.'.join(parts[:-1])
                        middlewares_settings[name][setting_key] = value
                    else:
                        # Install settings middlewares
                        middleware_class = get_object_on_path(value)
                        self.install_middleware(maybe_name, middleware_class)

            # Install middlewares with reversed order. Lower position first
            if self.middlewares:
                middlewares = []
                for name, middleware in self.middlewares:
                    settings = middlewares_settings[name]
                    default_position = (
                        getattr(middleware, 'position', DEFAULT_MIDDLEWARE_POSITION.get(name)))
                    position = settings.get('position', default_position) or 0
                    middlewares.append((position, name, middleware, settings))
                middlewares.sort(reverse=True)
                for position, name, middleware, settings in middlewares:
                    app = middleware(self, app, **settings)
                    app.name = name

        return app

    def install_middleware(self, name, middleware):
        self.middlewares.append((name, middleware))

    @configuration_extensions('apierrors.interface')
    def add_api_errors_interface(self, only_http=False):
        # Set JSON handler
        self.add_view(
            view='ines.views.errors_json_view',
            context=HTTPClientError,
            permission=NO_PERMISSION_REQUIRED)

        if not asbool(only_http):
            self.add_view(
                view='ines.views.errors_json_view',
                context=Error,
                permission=NO_PERMISSION_REQUIRED)
        if not asbool(only_http):
            self.add_view(
                view='ines.views.errors_json_view',
                context=Invalid,
                permission=NO_PERMISSION_REQUIRED)

    @configuration_extensions('policy.token')
    def set_token_policy(
            self,
            application_name,
            header_key='Authorization'):

        # Authentication Policy
        authentication_policy = ApplicationHeaderAuthenticationPolicy(
            application_name,
            header_key=header_key)
        self.set_authentication_policy(authentication_policy)

        authorization_policy = TokenAuthorizationPolicy(application_name)
        self.set_authorization_policy(authorization_policy)
