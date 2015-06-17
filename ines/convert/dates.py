# -*- coding: utf-8 -*-

import datetime
from time import mktime


DATE = datetime.date
DATETIME = datetime.datetime
STRING_TO_DATETIME = datetime.datetime.strptime
COMBINE_DATETIME = datetime.datetime.combine
EMPTY_TIME = datetime.time()


def maybe_datetime(value, date_format='%Y-%m-%d %H:%M:%S'):
    if isinstance(value, DATETIME):
        return value
    elif isinstance(value, DATE):
        return COMBINE_DATETIME(value, EMPTY_TIME)

    try:
        result = STRING_TO_DATETIME(value, date_format)
    except (TypeError, ValueError):
        date_value = maybe_date(value)
        if date_value:
            return COMBINE_DATETIME(date_value, EMPTY_TIME)
    else:
        return result


def maybe_date(value, date_format='%Y-%m-%d'):
    if isinstance(value, DATE):
        return value

    try:
        result = STRING_TO_DATETIME(value, date_format)
    except (TypeError, ValueError):
        pass
    else:
        return result.date()


def date_to_timestamp(value):
    value = maybe_datetime(value)
    if value:
        return mktime(value.timetuple())


def convert_timezone(value, time_zone=None):
    if isinstance(value, DATETIME) and value.utcoffset() is not None:
        value = value.replace(tzinfo=None) + value.utcoffset()
        if time_zone:
            return value + time_zone
    return value
