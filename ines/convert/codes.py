# -*- coding: utf-8 -*-

from hashlib import sha256
from math import ceil

from repoze.lru import LRUCache

from ines.convert.strings import force_string
from ines.convert.strings import force_unicode


SHA256_CACHE = LRUCache(5000)


def make_sha256_no_cache(key):
    key = force_string(key)
    return force_unicode(sha256(key).hexdigest())


def make_sha256(key):
    key_256 = SHA256_CACHE.get(key)
    if not key_256:
        key_256 = make_sha256_no_cache(key)
        SHA256_CACHE.put(key, key_256)
    return key_256


def inject_junk(value):
    value = force_unicode(value)

    if len(value) < 6:
        orders = list('987654')
    else:
        orders = list()

    blocks = list(value)
    for order in blocks:
        orders.extend(str(ord(order)))

    while orders:
        order = int(orders.pop(0))
        # Inject some junk
        position = int(ceil(len(blocks) / (order + 1.)))
        blocks.insert(position, u'1234qwerty'[order])

    return u''.join(blocks)
