# -*- coding: utf-8 -*-

import datetime
from time import mktime

from ines.convert.strings import to_string


DATE = datetime.date
TODAY = DATE.today
DATETIME = datetime.datetime
STRING_TO_DATETIME = datetime.datetime.strptime
COMBINE_DATETIME = datetime.datetime.combine
EMPTY_TIME = datetime.time()


def maybe_datetime(value, date_format='%Y-%m-%d %H:%M:%S'):
    if value is not None:
        if isinstance(value, DATETIME):
            return value
        elif isinstance(value, DATE):
            return COMBINE_DATETIME(value, EMPTY_TIME)

        value = to_string(value)
        try:
            result = STRING_TO_DATETIME(value, date_format)
        except (TypeError, ValueError):
            date_value = maybe_date(value)
            if date_value:
                return COMBINE_DATETIME(date_value, EMPTY_TIME)
        else:
            return result


def maybe_date(value, date_format='%Y-%m-%d'):
    if value is not None:
        if isinstance(value, DATETIME):
            return value.date()
        elif isinstance(value, DATE):
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
        return int(mktime(value.timetuple()))


def convert_timezone(value, time_zone=None):
    if isinstance(value, DATETIME) and value.utcoffset() is not None:
        value = value.replace(tzinfo=None) + value.utcoffset()
        if time_zone:
            return value + time_zone
    return value


DATETIME_PATTERNS = [
    '%a, %d %b %Y %H:%M:%S %Z',
    '%d %b %Y']


def guess_datetime(value):
    if value:
        value = to_string(value)
        for pattern in DATETIME_PATTERNS:
            try:
                datetime_value = STRING_TO_DATETIME(value, pattern)
            except ValueError:
                pass
            else:
                return datetime_value


def total_seconds(value):
    return int(value.seconds + value.days * 24 * 3600)


def total_time_seconds(value):
    return (((value.hour * 60) + value.minute) * 60) + value.second


def calculate_age(born):
    today = TODAY()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))
