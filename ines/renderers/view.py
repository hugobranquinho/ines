# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
# 
# @author Hugo Branquinho <hugobranq@gmail.com>
# 
# $Id$

from pyramid.renderers import RendererHelper


class inesRendererHelper(RendererHelper):
    def __init__(self, name):
        self.name = name
        self.type = '.pt'
        self.package = None
        self.registry = None

    def set_renderer(self, package, registry):
        self.registry = registry
        self.package = package
        path = '%s:templates/%s' % (registry.package_name, self.name)
        self.renderer = RendererHelper(path, package, registry)

    def render(self, value, system_values, request=None):
        renderer = self.renderer
        if system_values:
            system_values['renderer_name'] = renderer.name
            system_values['renderer_info'] = renderer

        return RendererHelper.render(renderer, value, system_values, request)
