# -*- coding: utf-8 -*-

from hashlib import sha256
from math import ceil

from six import u

from ines import lru_cache
from ines.convert.strings import to_bytes
from ines.convert.strings import to_unicode
from ines.convert.strings import unicode_join


def make_sha256_no_cache(key):
    key = to_bytes(key)
    return to_unicode(sha256(key).hexdigest())


@lru_cache(5000)
def make_sha256(key):
    return make_sha256_no_cache(key)


def inject_junk(value):
    value = to_unicode(value)

    if len(value) < 6:
        orders = list('987654')
    else:
        orders = list()

    blocks = list(value)
    for order in blocks:
        orders.extend(str(ord(order)))

    junk_code = u('1234qwerty')
    while orders:
        order = int(orders.pop(0))
        # Inject some junk
        position = int(ceil(len(blocks) / (order + 1.)))
        blocks.insert(position, junk_code[order])

    return unicode_join('', blocks)
