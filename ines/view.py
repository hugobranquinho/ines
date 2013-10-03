# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
# 
# @author Hugo Branquinho <hugobranq@gmail.com>
# 
# $Id$

import warnings

from pyramid.view import view_config

from ines.renderers import inesRendererHelper
from ines.route import check_if_route_exists


class ines_view(view_config):
    def __init__(self, **kwargs):
        view_config.__init__(self, **kwargs)

        self.renderer = getattr(self, 'renderer', None)
        if isinstance(self.renderer, basestring) and \
           self.renderer.endswith('.pt'):
            self.renderer = inesRendererHelper(self.renderer)

    def __call__(self, wrapped):
        if not self.route_name:
            return view_config.__call__(self, wrapped)

        settings = self.__dict__.copy()
        settings.pop('route_name')

        def callback(context, name, ob):
            config = context.config
            registry = config.registry
            if isinstance(self.renderer, inesRendererHelper):
                package = config.package
                self.renderer.set_renderer(package, registry)

                if self.renderer.type == '.pt':
                    config.use_pyramid_chameleon = True
                elif self.renderer.type in ('.mako ', '.mak'):
                    config.use_pyramid_mako = True

            if not isinstance(self.route_name, basestring):
                routes_names = self.route_name
            else:
                routes_names = [self.route_name]

            for route_name in routes_names:
                if check_if_route_exists(registry, route_name):
                    # Add view if route is defined
                    config = config.with_package(info.module)
                    config.add_view(view=ob, route_name=route_name, **settings)
                else:
                    message = ('Missing initialization of route "%s" on ' \
                               'application "%s"') \
                               % (route_name, config.package_name)
                    warnings.warn(message, UserWarning, stacklevel=2)

        info = self.venusian.attach(wrapped, callback, category='pyramid')
        if info.scope == 'class':
            if settings['attr'] is None:
                settings['attr'] = wrapped.__name__

        settings['_info'] = info.codeinfo  # fbo "action_method"
        return wrapped
