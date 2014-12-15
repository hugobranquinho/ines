# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

from pyramid.view import view_config
from pyramid.view import view_defaults


class api_config(view_config):
    def __call__(self, wrapped):
        settings = self.__dict__.copy()
        depth = settings.pop('_depth', 0)

        if 'renderer' not in settings:
            settings['renderer'] = 'json'

        def callback(context, name, ob):
            config = context.config.with_package(info.module)
            config.add_view(view=ob, **settings)

        info = self.venusian.attach(wrapped, callback, category='pyramid',
                                    depth=depth + 1)

        if info.scope == 'class':
            # if the decorator was attached to a method in a class, or
            # otherwise executed at class scope, we need to set an
            # 'attr' into the settings if one isn't already in there
            if settings.get('attr') is None:
                settings['attr'] = wrapped.__name__
            if 'request_method' not in settings:
                settings['request_method'] = wrapped.__name__.upper()

        settings['_info'] = info.codeinfo # fbo "action_method"
        return wrapped


class api_defaults(view_defaults):
    def __init__(self, **settings):
        view_defaults.__init__(self, **settings)

        if not hasattr(self, 'renderer'):
            self.renderer = 'json'
