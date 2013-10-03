# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
# 
# @author Hugo Branquinho <hugobranq@gmail.com>
# 
# $Id$

from pyramid.interfaces import ITranslationDirectories
from pyramid.registry import Registry


class inesRegistry(Registry):
    @property
    def package_name(self):
        return self.__name__

    def update_translation_dirs(self, dirs):
        self.registerUtility(dirs, ITranslationDirectories)

    def find_translation_dirs(self):
        if not self.action_state.actions:
            return self.queryUtility(ITranslationDirectories, default=[])

        dirs = []
        for action in self.action_state.actions:
            if action.get('discriminator') == None:
                introspectables = action.get('introspectables')
                if introspectables:
                    for introspectable in introspectables:
                        if introspectable.type_name == 'translation directory':
                            dirs.append(introspectable.discriminator)

        return dirs
