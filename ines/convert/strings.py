# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

from ines import CAMELCASE_UPPER_NAMES


def force_unicode(value, encoding='utf-8', errors='strict'):
    if isinstance(value, unicode):
        return value
    elif isinstance(value, str):
        return value.decode(encoding, errors)
    else:
        return unicode(str(value), encoding, errors)


def force_string(value, encoding='utf-8', errors='strict'):
    if isinstance(value, str):
        return value
    elif isinstance(value, unicode):
        return value.encode(encoding, errors)
    else:
        return str(value)


def maybe_integer(value):
    try:
        result = int(value)
    except (TypeError, ValueError):
        pass
    else:
        return result


def maybe_unicode(value, encoding='utf-8', errors='strict'):
    if value or value is 0:
        return force_unicode(value, encoding, errors)


def camelcase(value):
    if u'_' not in value:
        return value
    elif u'+' in value:
        return u'+'.join(camelcase(v) for v in value.split(u'+'))
    else:
        words = value.split(u'_')
        camelcase_words = [words.pop(0).lower()]
        for word in words:
            upper_word = word.upper()
            if upper_word in CAMELCASE_UPPER_NAMES:
                camelcase_words.append(upper_word)
            else:
                camelcase_words.append(word.title())
        return u''.join(camelcase_words)
