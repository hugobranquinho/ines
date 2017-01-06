# -*- coding: utf-8 -*-

from os import sep as OS_SEP
from os.path import normcase
from os.path import normpath
from os.path import join as join_path
from os.path import isdir
from os.path import exists

from pkg_resources import resource_exists
from pkg_resources import resource_filename
from pkg_resources import resource_isdir
from pyramid.asset import resolve_asset_spec
from pyramid.config.views import DefaultViewMapper
from pyramid.httpexceptions import HTTPNotFound
from pyramid.static import static_view
from pyramid.response import FileResponse
from pyramid.static import _secure_path
from pyramid.traversal import traversal_path_info
from pyramid.view import view_config as pyramid_view_config
from pyramid.view import view_defaults

from ines.convert import maybe_list
from ines.browser import BrowserDecorator
from ines.views.input import InputSchemaView
from ines.views.output import OutputSchemaView


class view_config(pyramid_view_config):
    def __call__(self, wrapped):
        settings = self.__dict__.copy()
        depth = settings.pop('_depth', 0)

        def callback(context, name, ob):
            config = context.config.with_package(info.module)

            route_name = settings.get('route_name') or getattr(ob, '__view_defaults__', {}).get('route_name')
            if route_name:
                browser_constructor = config.registry.settings.get('browser_constructor')
                if not browser_constructor:
                    browser_settings = dict(
                        (key[8:], value)
                        for key, value in config.registry.settings.items()
                        if key.startswith('browser.') and value)

                    if browser_settings:
                        browser_constructor = BrowserDecorator(browser_settings)
                        config.registry.settings['browser_constructor'] = browser_constructor

                if browser_constructor:
                    decorator = maybe_list(settings.pop('decorator', None))
                    decorator.append(browser_constructor)
                    settings['decorator'] = tuple(decorator)

            if not config.is_production_environ:
                renderer = settings.get('renderer')
                renderer_development_folder = config.settings.get('renderer_development_folder')
                if renderer and renderer_development_folder and ':' in renderer:
                    package_name, path = renderer.split(':', 1)
                    breadcrumbs = path.split(OS_SEP)
                    breadcrumbs[0] = renderer_development_folder
                    settings['renderer'] = '%s:%s' % (package_name, join_path(*breadcrumbs))

            config.add_view(view=ob, **settings)

        info = self.venusian.attach(
            wrapped,
            callback,
            category='pyramid',
            depth=depth + 1)

        if info.scope == 'class':
            if settings.get('attr') is None:
                settings['attr'] = wrapped.__name__
            if 'request_method' not in settings:
                request_method = wrapped.__name__.upper()
                if request_method == 'ADD':
                    request_method = 'POST'
                elif request_method == 'UPDATE':
                    request_method = 'PUT'
                settings['request_method'] = request_method

        settings['_info'] = info.codeinfo
        return wrapped


class api_config(pyramid_view_config):
    def __call__(self, wrapped):
        settings = self.__dict__.copy()
        depth = settings.pop('_depth', 0)

        use_fields = settings.pop('use_fields', False)
        input_option = settings.pop('input', None)
        output_option = settings.pop('output', None)
        auto_camelcase = settings.pop('auto_camelcase', True)

        def callback(context, name, ob):
            view_defaults_settings = getattr(ob, '__view_defaults__', {})

            route_name = settings.get('route_name') or view_defaults_settings.get('route_name')
            request_method = settings.get('request_method') or view_defaults_settings.get('request_method')
            renderer = settings['renderer'] = (
                settings.get('renderer')
                or view_defaults_settings.get('renderer')
                or 'json')

            if not context.config.is_production_environ:
                renderer = settings.get('renderer')
                renderer_development_folder = context.config.settings.get('renderer_development_folder')
                if renderer and renderer_development_folder and ':' in renderer:
                    package_name, path = renderer.split(':', 1)
                    breadcrumbs = path.split(OS_SEP)
                    breadcrumbs[0] = renderer_development_folder
                    settings['renderer'] = '%s:%s' % (package_name, join_path(*breadcrumbs))

            # Register input schema
            if input_option or use_fields:
                if input_option is not None:
                    if not isinstance(input_option, InputSchemaView):
                        input_view = InputSchemaView(
                            route_name,
                            request_method,
                            renderer,
                            schema=input_option,
                            use_fields=use_fields,
                            auto_camelcase=auto_camelcase)
                    else:
                        input_view = input_option
                else:
                    input_view = InputSchemaView(
                        route_name,
                        request_method,
                        renderer,
                        use_fields=use_fields,
                        auto_camelcase=auto_camelcase)

                decorator = maybe_list(settings.pop('decorator', None))
                decorator.append(input_view)
                settings['decorator'] = tuple(decorator)

                context.config.register_input_schema(input_view, route_name, request_method)

            # Register output schema
            if output_option:
                if not isinstance(output_option, OutputSchemaView):
                    output_view = OutputSchemaView(
                        route_name,
                        request_method,
                        renderer,
                        schema=output_option)
                else:
                    output_view = output_option

                previous_mapper = settings.get('mapper', DefaultViewMapper)

                class OutputViewMapper(previous_mapper):
                    def __call__(self, view):
                        view = super(OutputViewMapper, self).__call__(view)
                        return output_view(view)
                settings['mapper'] = OutputViewMapper

                context.config.register_output_schema(output_view, route_name, request_method)

            config = context.config.with_package(info.module)
            config.add_view(view=ob, **settings)

        info = self.venusian.attach(
            wrapped,
            callback,
            category='pyramid',
            depth=depth + 1)

        if info.scope == 'class':
            if settings.get('attr') is None:
                settings['attr'] = wrapped.__name__
            if 'request_method' not in settings:
                request_method = wrapped.__name__.upper()
                if request_method == 'ADD':
                    request_method = 'POST'
                elif request_method == 'UPDATE':
                    request_method = 'PUT'
                settings['request_method'] = request_method

        settings['_info'] = info.codeinfo
        return wrapped


class api_defaults(view_defaults):
    def __init__(self, **settings):
        view_defaults.__init__(self, **settings)


class gzip_static_view(static_view):
    def __init__(self, *args, **kwargs):
        gzip_path = kwargs.pop('gzip_path')
        super(gzip_static_view, self).__init__(*args, **kwargs)

        package_name, self.gzip_docroot = resolve_asset_spec(gzip_path, self.package_name)
        self.norm_gzip_docroot = normcase(normpath(self.gzip_docroot))

    def __call__(self, context, request):
        if self.use_subpath:
            path_tuple = request.subpath
        else:
            path_tuple = traversal_path_info(request.environ['PATH_INFO'])

        if self.cachebust_match:
            path_tuple = self.cachebust_match(path_tuple)
        path = _secure_path(path_tuple)

        if path is None:
            raise HTTPNotFound('Out of bounds: %s' % request.url)

        use_gzip = 'gzip' in request.accept_encoding
        if self.package_name: # package resource
            docroot = use_gzip and self.gzip_docroot or self.docroot
            resource_path ='%s/%s' % (docroot.rstrip('/'), path)
            if resource_isdir(self.package_name, resource_path):
                if not request.path_url.endswith('/'):
                    self.add_slash_redirect(request)
                resource_path = '%s/%s' % (resource_path.rstrip('/'),self.index)
            if not resource_exists(self.package_name, resource_path):
                raise HTTPNotFound(request.url)
            filepath = resource_filename(self.package_name, resource_path)

        else:
            norm_docroot = use_gzip and self.gzip_norm_docroot or self.norm_docroot
            filepath = normcase(normpath(join_path(norm_docroot, path)))
            if isdir(filepath):
                if not request.path_url.endswith('/'):
                    self.add_slash_redirect(request)
                filepath = join_path(filepath, self.index)
            if not exists(filepath):
                raise HTTPNotFound(request.url)

        response = FileResponse(filepath, request, self.cache_max_age)
        if use_gzip:
            response.content_encoding = 'gzip'

        return response
