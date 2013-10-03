# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
# 
# @author Hugo Branquinho <hugobranq@gmail.com>
# 
# $Id$

import datetime


DATE = datetime.date
DATETIME = datetime.datetime
STRING_TO_DATETIME = datetime.datetime.strptime


def force_string(value, encoding='utf-8', errors='strict'):
    if isinstance(value, str):
        return value
    elif isinstance(value, unicode):
        return value.encode(encoding, errors)
    else:
        return str(value)


def force_unicode(value, encoding='utf-8', errors='strict'):
    if isinstance(value, unicode):
        return value
    elif isinstance(value, str):
        return value.decode(encoding, errors)
    else:
        return unicode(str(value), encoding, errors)


def maybe_unicode(value, encoding='utf-8', errors='strict'):
    if value or value is 0:
        return force_unicode(value, encoding, errors)


def maybe_string(value, encoding='utf-8', errors='strict'):
    if value or value is 0:
        return force_string(value, encoding, errors)


def maybe_datetime(value, date_format='%Y-%m-%d %H:%M:%S'):
    if isinstance(value, DATETIME):
        return value

    try:
        result = STRING_TO_DATETIME(value, date_format)
    except (TypeError, ValueError):
        pass
    else:
        return result


def maybe_date(value, date_format='%Y-%m-%d'):
    if isinstance(value, DATE):
        return value

    result = maybe_datetime(value, date_format)
    if result:
        return result.date()


def maybe_integer(value):
    try:
        result = int(value)
    except (TypeError, ValueError):
        pass
    else:
        return result


def maybe_float(value):
    try:
        result = float(value)
    except (TypeError, ValueError):
        pass
    else:
        return result


def prepare_for_json(value):
    if value is None:
        return None

    elif isinstance(value, (float, int, long)):
        return value

    elif isinstance(value, (tuple, list)):
        return prepare_list_for_json(value)

    elif isinstance(value, dict):
        return prepare_dict_for_json(value)

    else:
        return force_unicode(value)


def prepare_dict_for_json(values):
    return dict((prepare_for_json(key), prepare_for_json(value))
                for key, value in values.items())


def prepare_list_for_json(values):
    return [prepare_for_json(value) for value in values]


def split_unicode(value, split_chars=None):
    value = force_unicode(value)
    split_chars = list(split_chars or [None])

    first_split_char = split_chars.pop(0)
    result = [part for part in value.split(first_split_char) if part]

    if split_chars and result:
        for split_char in split_chars:
            result = [p for part in result
                        for p in part.split(split_char) if p]

    return result
