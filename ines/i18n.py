# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
# 
# @author Hugo Branquinho <hugobranq@gmail.com>
# 
# $Id$

from pyramid.i18n import get_localizer
from pyramid.i18n import TranslationStringFactory

from ines import TRANSLATION_FACTORIES
from ines.path import find_package_name


def make_translation_factory(domain=None):
    if domain is None:
        domain = find_package_name()
    translation_factory = TranslationStringFactory(domain)

    # Register translation factory
    TRANSLATION_FACTORIES[domain] = translation_factory
    return translation_factory


def find_translation_factory(domain=None):
    if domain is None:
        domain = find_package_name()
    return TRANSLATION_FACTORIES.get(domain)


def get_translation_factory(domain):
    translation_factory = find_translation_factory(domain)
    if not translation_factory:
        translation_factory = make_translation_factory(domain)
    return translation_factory, domain


def translator_factory(request):
    translator = get_localizer(request).translate
    def method(message, **kwargs):
        if message is not None:
            return translator(message, **kwargs)
    return method


def translate(request, message, **kwargs):
    return translator_factory(request)(message, **kwargs)


def get_translation_domains(config=None):
    domains = TRANSLATION_FACTORIES.keys()
    if config:
        config_domain = config.registry.settings['translation_domain']
        # Set config domain as first
        if config_domain in domains:
            domains.remove(config_domain)
        domains.insert(0, config_domain)
    return domains


def get_translation_paths(config=None):
    domains = get_translation_domains(config)
    return ['%s:locale/' % d for d in domains]
