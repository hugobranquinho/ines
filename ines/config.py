# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
# 
# @author Hugo Branquinho <hugobranq@gmail.com>
# 
# $Id$

import datetime
from importlib import import_module
from pkg_resources import get_distribution
from pkg_resources import resource_filename

from pyramid.config import Configurator
from pyramid.session import UnencryptedCookieSessionFactoryConfig
from pyramid.settings import asbool
from pyramid.static import static_view

from ines import DEFAULT_EXTENSIONS
from ines import PACKAGES
from ines import STATIC_CACHE_AGE
from ines.convert import force_unicode
from ines.i18n import get_translation_factory
from ines.i18n import get_translation_paths
from ines.path import find_package_name
from ines.path import find_package_version
from ines.registry import inesRegistry
from ines.request import RootFactory
from ines.request import inesRequest
from ines.utils import cache_property
from ines.route import check_if_route_exists
from ines.utils import find_class_on_module
from ines.utils import find_settings
from ines.utils import get_method
from ines.utils import InfiniteDict


TIMEDELTA = datetime.timedelta


def initialize_configuration(global_settings, application_settings):
    settings = global_settings.copy()
    settings.update(application_settings)

    application_url = settings.get('url')
    if not application_url:
        raise ValueError('Missing application URL. Use "url" key.')
    elif not application_url.startswith('/'):
        raise ValueError('Application URL must start with "/"')

    package_name = find_package_name()
    config = inesConfigurator(package=package_name, registry=object())
    config.registry = inesRegistry(package_name)
    PACKAGES[package_name] = config

    config.setup_registry(
        settings=settings,
        root_factory=RootFactory,
        session_factory=UnencryptedCookieSessionFactoryConfig(package_name),
        request_factory=inesRequest)
    settings = config.registry.settings

    # Define debug modes
    debug = asbool(settings.get('debug', False))
    log_warning_seconds = int(settings.get('log_warning_seconds', 86400))

    environment_profile = settings.get('environment_profile') or 'development'
    environment_profile = environment_profile.lower()
    production_environment = environment_profile == 'production'

    keys_to_mask_on_log = set(settings.get('keys_to_mask_on_log', '').split())
    if keys_to_mask_on_log:
        keys_to_mask_on_log = set(k.lower() for k in keys_to_mask_on_log)

    settings.update({
        'debug': debug,
        'environment_profile': environment_profile,
        'production_environment': production_environment,
        'cache': InfiniteDict(),
        'log_warning_time': TIMEDELTA(seconds=log_warning_seconds),
        'keys_to_mask_on_log': keys_to_mask_on_log})

    # Configure translate options
    translation_factory, domain = get_translation_factory(package_name)
    languages = set(settings.get('languages', '').split())
    languages.add(settings['default_locale_name'])
    settings.update({
        'translation_factory': translation_factory,
        'translation_domain': domain,
        'languages': languages})

    # Define application API
    api_module = import_module('%s.api' % package_name)
    api_session_path = settings.get('api_session_path')
    if api_session_path:
        api_session = get_method(api_session_path)
    else:
        from ines.api import BaseAPISession
        api_session = find_class_on_module(api_module, BaseAPISession)
        if not api_session:
            message = 'Missing APISession class'
            raise NotImplementedError(message)

    api_class_path = settings.get('api_class_path')
    api_class = get_method(api_class_path, ignore=True) or \
                api_session._base_class
    settings['api'] = api_class(config, api_session, package_name)

    # Create default extensions, if defined
    extensions = [] 
    if settings.has_key('extensions'):
        for extension in settings['extensions'].split():
            extensions.append(extension.split(':', 1))
    extensions.extend(DEFAULT_EXTENSIONS)

    settings['extensions'] = {}
    for name, session_class_path in extensions:
        session_path_key = '%s_session_path' % name
        session_path = settings.get(session_path_key)
        if session_path:
            session = get_method(session_path)
        else:
            module_path, class_name = session_class_path.split(':', 1)
            session_module = import_module(module_path)
            base_session_class = getattr(session_module, class_name)
            session = find_class_on_module(api_module, base_session_class)

        if session:
            class_path_key = '%s_class_path' % name
            class_path = settings.get(class_path_key)
            class_ = get_method(class_path, ignore=True) or session._base_class
            extension = class_(config, session, package_name)
            settings['extensions'][name] = extension

    # reCaptcha factory
    recaptcha_public_key = settings.get('recaptcha_public_key')
    recaptcha_private_key = settings.get('recaptcha_private_key')
    if recaptcha_public_key and recaptcha_private_key:
        disable_recaptcha = asbool(settings.get('disable_recaptcha'))

        from ines.modules.captcha import reCaptcha
        recaptcha = reCaptcha(
                        recaptcha_public_key,
                        recaptcha_private_key,
                        disable_recaptcha=disable_recaptcha,
                        html_code=settings.get('recaptcha_html'))

        settings.update({
            'recaptcha': recaptcha})

    # Set translations dirs
    config.set_translation_dirs()

    # Define required keys
    settings.update({
        'static_views': {},
        'home_route': settings.get('home_route') or 'home',
        })

    return config


class inesConfigurator(Configurator):
    use_pyramid_chameleon = False
    use_pyramid_mako = False

    @cache_property
    def version(self):
        version = get_distribution(self.package_name).version
        return force_unicode(version)

    def registry_static_path(self, name, package_name, path):
        dir_path = resource_filename(package_name, path)
        self.registry.settings['static_views'][name] = dir_path

    def add_view(self, *args, **kwargs):
        view = kwargs.get('view')
        route_name = kwargs.get('route_name')
        if route_name and view and isinstance(view, static_view):
            self.registry_static_path(route_name,
                                      view.package_name,
                                      view.docroot)

        return Configurator.add_view(self, *args, **kwargs)

    def add_routes(self, *routes):
        for arguments in routes:
            if isinstance(arguments, dict):
                self.add_route(**arguments)
            else:
                name, path = arguments
                self.add_route(name, path)

        # Scan for all application routes
        views_path = '%s.views' % self.package_name
        self.scan(views_path)

        # Check for home route
        home_route = self.registry.settings['home_route']
        if not check_if_route_exists(self.registry, home_route):
            message = 'Missing home route "%s" on "%s"' \
                      % (home_route, self.package_name)
            raise NotImplementedError(message)

    def add_static_route(self,
                         name,
                         path='static/*subpath',
                         view_path=None,
                         use_subpath=True,
                         cache_max_age=STATIC_CACHE_AGE):

        # Define default view path
        if view_path is None:
            view_path = '%s:static/' % self.package_name

        # Registry static view path
        self.registry_static_path(name, *view_path.split(':', 1))

        static_func = static_view(view_path,
                                  use_subpath=use_subpath,
                                  cache_max_age=cache_max_age)

        self.add_route(name, path)
        self.add_view(route_name=name, view=static_func)

    def set_translation_dirs(self):
        translation_paths = get_translation_paths(self)
        self.add_translation_dirs(*translation_paths)

    def add_translation_dirs(self, *dirs):
        if dirs:
            Configurator.add_translation_dirs(self, *dirs)

        # Get translation full paths
        translation_dirs = self.registry.find_translation_dirs()

        # Update translation dirs in other packages
        for config in PACKAGES.values():
            package_dirs = config.registry.find_translation_dirs()

            update_dirs = set(translation_dirs).difference(package_dirs)
            if update_dirs:
                package_dirs.extend(update_dirs)
                config.registry.update_translation_dirs(package_dirs)

    def make_wsgi_app(self):
        if self.use_pyramid_chameleon:
            self.include('pyramid_chameleon')
        if self.use_pyramid_mako:
            self.include('pyramid_mako')

        return Configurator.make_wsgi_app(self)
