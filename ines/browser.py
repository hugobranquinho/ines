# -*- coding: utf-8 -*-

from distutils.version import LooseVersion
from functools import lru_cache
from functools import wraps

from pyramid.decorator import reify
from pyramid.settings import asbool

from ines.exceptions import HTTPBrowserUpgrade


BROWSER_REFERENCES = {
    'microsoft internet explorer': 'ie'
}

HTML5_SUPPORT = {
    'chrome': '9',
    'firefox': '4',
    'safari': '6',
    'opera': '15',
    'ie': '9',
}


class BrowserDecorator(object):
    def __init__(self, settings):
        self.locked_browsers = settings.pop('locked', '').lower().split()
        self.all_locked = 'all' in self.locked_browsers
        self.allow_bots = asbool(settings.pop('allow_bots', True))

        versions = {}
        if asbool(settings.pop('html5.support', True)):
            versions.update(HTML5_SUPPORT)
        versions.update(settings)
        self.versions = dict((k, LooseVersion(v)) for k, v in versions.items())

        import httpagentparser as hap
        self.hap = hap

    def browser_locked(self, name, version, is_bot):
        if is_bot and self.allow_bots:
            return True

        elif name:
            name = name.lower()
            name = BROWSER_REFERENCES.get(name) or name
            if name in self.locked_browsers:
                return True

            min_version = self.versions.get(name)
            if min_version:
                return min_version > LooseVersion(version)

        return self.all_locked

    @lru_cache(500)
    def lookup_info(self, user_agent):
        if user_agent:
            result = {}
            for detector in self.hap.detectorshub['browser']:
                try:
                    detector.detect(user_agent, result)
                except Exception:
                    pass

                browser = result.get('browser')
                if browser:
                    name = browser.get('name')
                    version = browser.get('version')
                    if name and version:
                        return name.lower(), version, result.get('bot', False)

        return None, None, False

    def request_is_locked(self, request):
        return self.browser_locked(*self.lookup_info(request.user_agent))

    def __call__(self, wrapped):
        if not self.versions and not self.locked_browsers and not self.all_locked:
            return wrapped

        if self.versions and not BROWSER_REFERENCES:
            for detector in self.hap.detectorshub['browser']:
                name = detector.name.lower()
                if name not in BROWSER_REFERENCES:
                    BROWSER_REFERENCES[name] = name

        @wraps(wrapped)
        def wrapper(context, request):
            if self.request_is_locked(request):
                raise HTTPBrowserUpgrade()
            else:
                return wrapped(context, request)

        return wrapper
