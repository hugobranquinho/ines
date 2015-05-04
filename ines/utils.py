# -*- coding: utf-8 -*-

from calendar import monthrange
import datetime
from json import dumps
from os import getpid
from os import SEEK_END
from os import stat as os_stat
from os.path import getmtime
from re import compile as regex_compile
from uuid import uuid4
import warnings

from colander import Invalid
from pyramid.httpexceptions import HTTPError

from ines import DOMAIN_NAME
from ines.cleaner import normalize_full_name
from ines.convert import camelcase
from ines.convert import force_string
from ines.convert import force_unicode
from ines.convert import make_sha256
from ines.i18n import translate_factory


NOW = datetime.datetime.now
DATE = datetime.date
TIMEDELTA = datetime.timedelta
PROCESS_ID = getpid()

# See: http://www.regular-expressions.info/email.html
EMAIL_REGEX = regex_compile(
    "[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*"
    "@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?")


class WarningDict(dict):
    def __init__(self, message='Duplicate item "{key}" with value "{value}"'):
        super(WarningDict, self).__init__()
        self.message = message

    def __setitem__(self, key, value):
        if key in self:
            warnings.warn(
                self.message.format(key=key, value=value),
                UserWarning,
                stacklevel=2)

        super(WarningDict, self).__setitem__(key, value)

    def update(self, *args, **kwargs):
        if args:
            if len(args) > 1:
                raise TypeError('update expected at most 1 arguments, got %d' % len(args))
            for key, value in dict(args[0]).items():
                self[key] = value

        for key, value in kwargs.items():
            self[key] = value

    def setdefault(self, key, value=None):
        if key not in self:
            self[key] = value
        return self[key]


class MissingDict(dict):
    _base_type = dict

    def __missing__(self, key):
        self[key] = self._base_type()
        return self[key]

    def add_item(self, key, value):
        self[key][value] = {}


class MissingList(MissingDict):
    _base_type = list

    def add_item(self, key, value):
        self[key].append(value)


class MissingDictList(MissingDict):
    _base_type = MissingList

    def add_item(self, key, value):
        self[key][value] = []


class MissingInteger(MissingDict):
    _base_type = int

    def add_item(self, key, value):
        self[key] += value


class MissingSet(MissingDict):
    _base_type = set

    def add_item(self, key, value):
        self[key].add(value)


class InfiniteDict(MissingDict):
    @property
    def _base_type(self):
        return InfiniteDict


def make_uuid_hash():
    return force_unicode(uuid4().hex)


def make_unique_hash():
    key = '.'.join((
        uuid4().hex,
        str(NOW()),
        str(PROCESS_ID),
        str(DOMAIN_NAME)))
    return make_sha256(key)


def last_read_file_time(path):
    try:
        last_read_time = int(os_stat(path).st_atime)
    except OSError:
        pass
    else:
        return last_read_time


def format_error_to_json_values(error, kwargs=None, request=None):
    if request:
        translate = translate_factory(request)
    else:
        translate = lambda tstring, **kw: tstring

    if isinstance(error, HTTPError):
        status = error.code
        key = camelcase(error.title)
        message = error.explanation
    elif isinstance(error, Invalid):
        status = 400
        key = camelcase(error._keyname())
        message = error.msg

        errors = MissingList()
        for path in error.paths():
            for exc in path:
                key = str(exc.node.name)
                if exc.positional and exc.pos:  # Ignore 0 position
                    key += '.' + str(exc.pos)

                if key and exc.msg:
                    key = camelcase(key)
                    for message in exc.messages():
                        errors[key].append(translate(message))

        if not kwargs:
            kwargs = {}
        kwargs['errors'] = errors
    else:
        status = getattr(error, 'code', 400)
        key = camelcase(getattr(error, 'key', 'undefined'))
        message = getattr(error, 'msg', getattr(error, 'message', u'Undefined'))

    values = {
        'status': status,
        'property': key,
        'message': translate(message)}
    if kwargs:
        values.update(kwargs)
    return values


def format_error_to_json(error, kwargs=None, request=None):
    return dumps(format_error_to_json_values(error, kwargs, request=request))


def file_modified_time(path):
    try:
        modified_time = getmtime(path)
    except OSError:
        pass
    else:
        return modified_time


def validate_email(value):
    value = force_string(value)
    return bool(EMAIL_REGEX.match(value))


def maybe_email(value):
    if validate_email(value):
        return force_unicode(value)


def get_content_type(value):
    if value:
        return force_string(value).split(';', 1)[0].strip()


def different_values(first, second):
    if first is None:
        if second is None:
            return False
        else:
            return True
    elif second is None:
        return True
    else:
        return bool(first != second)


def get_file_size(source_file):
    source_file.seek(0, SEEK_END)
    size = source_file.tell()
    source_file.seek(0)
    return size


def close_words(first, second, deep=1):
    for i in xrange(deep):
        close_word = first[:-1]
        if close_word and close_word == second:
            return True
        close_word = second[:-1]
        if close_word and close_word == first:
            return True
    return False


def compare_full_name_factory(name):
    normalized_name = normalize_full_name(name)
    normalized_words = normalized_name.split()
    normalized_length = len(normalized_words)

    def replacer(full_name):
        full_name = normalize_full_name(full_name)
        if full_name == normalized_name:
            return 100

        found_sequence_words = 0
        close_sequence_words = 0
        not_so_close_sequence_words = 0
        name_words = full_name.split()
        length = len(name_words)

        if normalized_length < length:
            master = name_words
            slave = normalized_words
            percentage_length = length
        else:
            master = normalized_words
            slave = name_words
            percentage_length = normalized_length

        for word in master:
            for i, name_word in enumerate(slave):
                if word == name_word:
                    found_sequence_words += 1
                    slave = slave[i + 1:]
                    break
                elif close_words(word, name_word, deep=2):
                    close_sequence_words += 1
                    break
                elif word in name_word or name_word in word:
                    not_so_close_sequence_words += 1
                    break

        percentage = (found_sequence_words * 100)
        percentage += (close_sequence_words * 60)
        percentage += (not_so_close_sequence_words * 20)
        if percentage:
            percentage /= percentage_length

        if percentage > 100:
            return 100
        elif percentage < 0:
            return 0
        else:
            return percentage
    return replacer


def add_months(value, months):
    month = value.month - 1 + months
    year = value.year + (month / 12)
    month = month % 12 + 1
    day = min(value.day, monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


def last_day_of_month_for_weekday(year, month, weekday):
    month_days = monthrange(year, month)[1]
    last_day = DATE(year, month, month_days)

    last_weekday = last_day.weekday()
    if last_weekday > weekday:
        last_day -= TIMEDELTA(days=last_weekday - weekday)
    elif last_weekday < weekday:
        last_day -= TIMEDELTA(days=7 - weekday + last_weekday)

    return last_day
