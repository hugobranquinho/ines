# -*- coding: utf-8 -*-

from functools import wraps

from ines.exceptions import Error
from ines.i18n import _


def requests_limit_decorator(length, expire=300):
    def decorator(wrapped):
        @wraps(wrapped)
        def wrapper(cls, *args, **kwargs):
            cache_key = '%s user limit %s' % (wrapped.__name__, cls.request.ip_address)
            counter = int(cls.cache.get(cache_key, expire=expire) or 0)
            if counter >= length:
                raise Error('tries', _('Too many tries. Try again later'))

            cls.cache.put(cache_key, counter + 1, expire=expire)
            result = wrapped(cls, *args, **kwargs)
            return result

        return wrapper
    return decorator
