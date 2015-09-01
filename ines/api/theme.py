# -*- coding: utf-8 -*-

from pyramid.decorator import reify
from pyramid.renderers import get_renderer

from ines.api import BaseSessionManager
from ines.api import BaseSession


class BaseThemeSessionManager(BaseSessionManager):
    __api_name__ = 'theme'


class BaseThemeSession(BaseSession):
    __api_name__ = 'theme'
    main_macro_name = 'html'
    doctype = '<!doctype html>'

    @reify
    def main(self):
        return get_renderer(self.settings['main_path']).implementation()

    @reify
    def maintemplate(self):
        return self.main.macros[self.main_macro_name]
