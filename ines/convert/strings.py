# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

from ines import CAMELCASE_UPPER_WORDS


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
            if upper_word in CAMELCASE_UPPER_WORDS:
                camelcase_words.append(upper_word)
            else:
                camelcase_words.append(word.title())
        return u''.join(camelcase_words)


def uncamelcase(value):
    count = 0
    words = {}
    previous_is_upper = False
    for letter in force_unicode(value):
        if letter.isupper():
            if not previous_is_upper:
                count += 1
            else:
                maybe_upper_name = (u''.join(words[count]) + letter).upper()
                if maybe_upper_name not in CAMELCASE_UPPER_WORDS:
                    count += 1
            previous_is_upper = True

        else:
            if previous_is_upper:
                maybe_upper_name = (u''.join(words[count]) + letter).upper()
                if maybe_upper_name not in CAMELCASE_UPPER_WORDS:
                    words[count + 1] = [words[count].pop()]
                    count += 1
            previous_is_upper = False

        words.setdefault(count, []).append(letter)

    words = words.items()
    words.sort()

    final_words = []
    for count, letters in words:
        if letters:
            final_words.append(u''.join(letters))
    return u'_'.join(final_words).lower()
