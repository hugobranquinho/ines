# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
# 
# @author Hugo Branquinho <hugobranq@gmail.com>
# 
# $Id$

import datetime
from urllib import unquote
from urlparse import urlparse

from pyramid.request import Request
from pyramid.settings import asbool
from webob.exc import HTTPFound

from ines import LOGS
from ines import MISSING
from ines.convert import force_string
from ines.convert import force_unicode
from ines.i18n import translator_factory as i18n_translator_factory
from ines.utils import cache_property
from ines.utils import InfiniteDict


NOW_DATE = datetime.datetime.now


class RootFactory(dict):
    def __init__(self, request):
        super(dict, self).__init__()
        self.update(request.matchdict or {})


class inesRequest(Request):
    @cache_property
    def cache(self):
        return InfiniteDict()

    @property
    def package_name(self):
        return self.registry.package_name

    @property
    def settings(self):
        return self.registry.settings

    @cache_property
    def api(self):
        return self.settings['api'](self)

    @cache_property
    def _LOCALE_(self):
        language = self.api.get_language()
        if language and language in self.settings['languages']:
            return language
        else:
            return self.settings['default_locale_name']

    @cache_property
    def authenticated_account(self):
        account_package_name = self.settings.get('account_package_name')
        if account_package_name:
            api_session = self.api
            if account_package_name != self.package_name:
                api_session = getattr(api_session.packages, account_package_name)

            return api_session.get_authenticated_account()

    @property
    def current_route_name(self):
        return self.matched_route.name

    def redirect_response(self, url):
        url = force_string(url)
        url = unquote(url)
        return HTTPFound(location=url)

    def redirect_to_self(self, *elements, **kwargs):
        url = self.current_route_url()
        return self.redirect_response(url)

    def redirect_to_route(self, *arguments, **kwargs):
        url = self.route_url(*arguments, **kwargs)
        return self.redirect_response(url)

    def get_value(self, key, default=None):
        value = self.POST.get(key, MISSING)
        if value is MISSING:
            value = self.GET.get(key, MISSING)
            if value is MISSING:
                return default

        return value

    def get_value_as_bool(self, key, default=None):
        value = self.get_value(key, default)
        return asbool(value)

    def has_value(self, key):
        return bool(self.POST.has_key(key) or self.GET.has_key(key))

    @cache_property
    def translator(self):
        return i18n_translator_factory(self)

    def translate(self, message, **kwargs):
        return self.translator(message, **kwargs)

    @cache_property
    def ip_address(self):
        ip = self.environ.get('HTTP_X_FORWARDED_FOR') or \
             self.environ.get('REMOTE_ADDR')

        if ip:
            return force_unicode(ip)
        else:
            message = u'Missing IP on request!'
            self.log_critical('missing_ip_address', message)

    @cache_property
    def user_agent(self):
        agent = self.environ.get('HTTP_USER_AGENT')
        if agent:
            return force_unicode(agent)

    @cache_property
    def referer_url(self):
        url = self.environ.get('HTTP_REFERER')
        if url:
            return force_unicode(url)

    @cache_property
    def protocol(self):
        protocol = urlparse(self.url).scheme
        if protocol:
            return force_unicode(protocol).lower()

    def mask_log_keys(self, values):
        mask_keys = self.settings['keys_to_mask_on_log']
        if not mask_keys:
            return values

        result = {}
        for key, value in values.items():
            if isinstance(key, basestring):
                if key.lower() in mask_keys:
                    value = u'*' * 8

            result[key] = value

        return result

    def log(self, code, message, level=u'info', package_name=None, **kwargs):
        if package_name is None:
            package_name = self.package_name

        for log_class in LOGS:
            log_class.log(self, package_name, code, message, level, **kwargs)

    def log_error(self, *args, **kwargs):
        kwargs['level'] = u'error'
        return self.log(*args, **kwargs)

    def log_critical(self, *args, **kwargs):
        kwargs['level'] = u'critical'
        return self.log(*args, **kwargs)

    def log_warning(self, code, message, package_name=None, **kwargs):
        if package_name is None:
            package_name = self.package_name
            settings = self.settings
        else:
            settings = getattr(self.api.packages, package_name).settings

        now = NOW_DATE()
        cache = settings['cache']['warnings'][code]
        date_to_next_log = cache[message]

        if not date_to_next_log or date_to_next_log < now:
            date_to_next_log = now + settings['log_warning_time']
            cache[message] = date_to_next_log
            return self.log_critical(
                       code,
                       message,
                       package_name=package_name,
                       **kwargs)


class BaseLog(object):
    def log(self, request, package_name, code, message, level=u'info', **kwargs):
        if request.settings['debug'] or level != 'info':
            print '-' * 60
            print
            print level.upper()
            print '  Request package: %s' % request.package_name
            print '  Package        : %s' % package_name
            print '  Code           : %s' % code
            print '  Url            : %s' % force_string(request.path_url)
            print '  Referer url    : %s' % request.referer_url
            print '  Date           : %s' % NOW_DATE()
            print '  Language       : %s' % request._LOCALE_
            print '  IP             : %s' % request.ip_address
            print '  User agent     : %s' % request.user_agent

            account = request.authenticated_account
            if account:
                print '  User           : %s' % account.id

            try:
                if request.POST:
                    print
                    print '  POST:'
                    for values in request.mask_log_keys(request.POST).items():
                        print '    %s: %s' % values
                    print
            except:
                pass

            try:
                if request.GET:
                    print
                    print '  GET:'
                    for values in request.mask_log_keys(request.GET).items():
                        print '    %s: %s' % values
                    print
            except:
                pass

            print 
            print '  MESSAGE:'

            try:
                message = force_string(message)
                for line in message.split('\n'):
                    print '    %s' % line
            except:
                pass

            print
            print '-' * 60


# Add default LOG
default_log = BaseLog()
LOGS.append(default_log)
