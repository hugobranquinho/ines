# -*- coding: utf-8 -*-

from hashlib import sha256
from math import ceil

from ines.convert.strings import force_string
from ines.convert.strings import force_unicode


def make_sha256(value):
    value = force_string(value)
    return force_unicode(sha256(value).hexdigest())


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
