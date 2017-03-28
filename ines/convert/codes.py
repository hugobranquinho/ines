# -*- coding: utf-8 -*-

from functools import lru_cache
from hashlib import sha256
from math import ceil

from ines.convert.strings import to_bytes, to_string


def make_sha256_no_cache(key):
    return sha256(to_bytes(key)).hexdigest()


@lru_cache(5000)
def make_sha256(key):
    return make_sha256_no_cache(key)


def inject_junk(value):
    # TODO use hmac
    value = to_string(value)

    if len(value) < 6:
        orders = list('987654')
    else:
        orders = list()

    blocks = list(value)
    for order in blocks:
        orders.extend(str(ord(order)))

    junk_code = '1234qwerty'
    while orders:
        order = int(orders.pop(0))
        # Inject some junk
        position = int(ceil(len(blocks) / (order + 1.)))
        blocks.insert(position, junk_code[order])

    return ''.join(blocks)
