# -*- coding: utf-8 -*-

from pyramid.config.views import DefaultViewMapper
from pyramid.view import view_config
from pyramid.view import view_defaults

from ines.convert import maybe_list
from ines.views.input import InputSchemaView
from ines.views.output import OutputSchemaView


class api_config(view_config):
    def __call__(self, wrapped):
        settings = self.__dict__.copy()
        depth = settings.pop('_depth', 0)

        use_fields = settings.pop('use_fields', False)
        input_option = settings.pop('input', None)
        output_option = settings.pop('output', None)
        auto_camelcase = settings.pop('auto_camelcase', True)

        def callback(context, name, ob):
            route_name = settings.get('route_name')
            if not route_name:
                route_name = getattr(ob, '__view_defaults__', {}).get('route_name')
            request_method = settings.get('request_method')
            if not request_method:
                request_method = getattr(ob, '__view_defaults__', {}).get('request_method')

            # Register input schema
            if input_option or use_fields:
                if input_option is not None:
                    if not isinstance(input_option, InputSchemaView):
                        input_view = InputSchemaView(
                            route_name,
                            request_method,
                            schema=input_option,
                            use_fields=use_fields,
                            auto_camelcase=auto_camelcase)
                    else:
                        input_view = input_option
                else:
                    input_view = InputSchemaView(
                        route_name,
                        request_method,
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
