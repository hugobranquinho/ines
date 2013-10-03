# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
# 
# @author Hugo Branquinho <hugobranq@gmail.com>
# 
# $Id$

from pyramid.renderers import get_renderer
from pyramid.renderers import render
from pyramid.settings import asbool

from ines.api import BaseClass
from ines.api import BaseSession
from ines.utils import cache_property


class BaseLayoutClass(BaseClass):
    def __init__(self, config, session, package_name):
        BaseClass.__init__(self, config, session, package_name)

        # Required options
        default_layout_path = 'ines:templates/layout.pt'
        layout_path = self.settings.get('layout_path', default_layout_path)

        default_main_path = '%s:templates/main.pt' % self.package_name
        main_path = self.settings.get('main_path', default_main_path)

        default_menu_path = '%s:templates/menu.pt' % self.package_name
        menu_path = self.settings.get('menu_path', default_menu_path)

        with_sidebar = self.settings.get('layout_with_sidebar', True)

        self.settings.update({
            'layout_path': layout_path,
            'main_path': main_path,
            'menu_path': menu_path,
            'layout_with_sidebar': asbool(with_sidebar)})


class BaseLayoutSession(BaseSession):
    _base_class = BaseLayoutClass
    doctype = '<!doctype html>'

    @cache_property
    def layout(self):
        layout_path = self.settings['layout_path']
        return get_renderer(layout_path).implementation()

    @property
    def layout_macro(self):
        return self.layout.macros['main']

    @cache_property
    def main(self):
        main_path = self.settings['main_path']
        return get_renderer(main_path).implementation()

    @property
    def main_macro(self):
        return self.main.macros['main']

    @cache_property
    def menu(self):
        menu_path = self.settings['menu_path']
        return get_renderer(menu_path).implementation()

    @property
    def menu_macro(self):
        return self.main.macros['main']

    @property
    def with_sidebar(self):
        return self.settings['layout_with_sidebar']

    @cache_property
    def title(self):
        locale = self.request._LOCALE_
        title = self.settings.get('title_%s' % locale)
        if title:
            return title

        default_locale = self.settings['default_locale_name']
        default_title = self.settings.get('title_%s' % default_locale)
        if default_title:
            return default_title
