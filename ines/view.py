# -*- coding: utf-8 -*-

from pyramid.config.views import DefaultViewMapper
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
