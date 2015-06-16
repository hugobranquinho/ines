# -*- coding: utf-8 -*-

from calendar import monthrange
import datetime
import errno
from hashlib import sha256
from json import dumps
from os import getpid
from os import listdir
from os import makedirs
from os import mkdir
from os import remove as _remove_file
from os import rename as _rename_file
from os import SEEK_END
from os import stat as os_stat
from os.path import dirname
from os.path import getmtime
from re import compile as regex_compile
from uuid import uuid4
import warnings

from colander import Invalid
from pyramid.httpexceptions import HTTPError

from ines import DOMAIN_NAME
from ines import DEFAULT_RETRY_ERRNO
from ines import OPEN_BLOCK_SIZE
from ines.cleaner import normalize_full_name
from ines.convert import camelcase
from ines.convert import force_string
from ines.convert import force_unicode
from ines.convert import maybe_integer
from ines.convert.codes import make_sha256_no_cache
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


class MissingDictSet(MissingDict):
    _base_type = MissingSet

    def add_item(self, key, value):
        self[key][value] = set()


class InfiniteDict(MissingDict):
    @property
    def _base_type(self):
        return InfiniteDict


def make_uuid_hash():
    return force_unicode(uuid4().hex)


def make_unique_hash(length=64):
    code = u''
    while len(code) < length:
        code += make_sha256_no_cache('.'.join((
            uuid4().hex,
            str(NOW()),
            str(PROCESS_ID),
            str(DOMAIN_NAME))))
    return code[:length]


def last_read_file_time(path, retries=3, retry_errno=DEFAULT_RETRY_ERRNO):
    retries = (maybe_integer(retries) or 3) + 1
    while retries:
        try:
            last_read_time = int(os_stat(path).st_atime)
        except OSError as error:
            if error.errno is errno.ENOENT:
                return None
            elif error.errno in retry_errno:
                # Try again, or not!
                retries -= 1
            else:
                # Something goes wrong
                raise
        else:
            return last_read_time

    # After X retries, raise previous IOError
    raise


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


def file_modified_time(path, retries=3, retry_errno=DEFAULT_RETRY_ERRNO):
    retries = (maybe_integer(retries) or 3) + 1
    while retries:
        try:
            modified_time = getmtime(path)
        except OSError as error:
            if error.errno is errno.ENOENT:
                return None
            elif error.errno in retry_errno:
                # Try again, or not!
                retries -= 1
            else:
                # Something goes wrong
                raise
        else:
            return modified_time

    # After X retries, raise previous IOError
    raise


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


def remove_file(path, retries=3, retry_errno=DEFAULT_RETRY_ERRNO):
    retries = (maybe_integer(retries) or 3) + 1
    while retries:
        try:
            _remove_file(path)
        except OSError as error:
            if error.errno is errno.ENOENT:
                # Already deleted!
                return False
            elif error.errno in retry_errno:
                # Try again, or not!
                retries -= 1
            else:
                # Something goes wrong
                raise
        else:
            return True

    # After X retries, raise previous OSError
    raise


def remove_file_quietly(path, retries=3, retry_errno=DEFAULT_RETRY_ERRNO):
    try:
        remove_file(path, retries=retries, retry_errno=retry_errno)
    except (IOError, OSError):
        pass


def make_dir(path, mode=0777, make_dir_recursively=False):
    path = force_string(path)
    try:
        mkdir(path, mode)
    except OSError as error:
        if make_dir_recursively and error.errno is errno.ENOENT:
            makedirs(path, mode)
        elif error.errno is not errno.EEXIST:
            raise
    return path


def move_file(path, new_path, retries=3, retry_errno=DEFAULT_RETRY_ERRNO):
    retries = (maybe_integer(retries) or 3) + 1
    while retries:
        try:
            _rename_file(path, new_path)
        except OSError as error:
            if error.errno in retry_errno:
                # Try again, or not!
                retries -= 1
            else:
                # Something goes wrong
                raise
        else:
            return True

    # After X retries, raise previous OSError
    raise


def get_open_file(path, mode='rb', retries=3, retry_errno=DEFAULT_RETRY_ERRNO):
    retries = (maybe_integer(retries) or 3) + 1
    while retries:
        try:
            open_file = open(path, mode)
        except IOError as error:
            if error.errno is errno.ENOENT:
                if mode not in ('r', 'rb'):
                    # Missing folder, create and try again
                    make_dir(dirname(path))
                else:
                    raise
            elif error.errno in retry_errno:
                # Try again, or not!
                retries -= 1
            else:
                # Something goes wrong
                raise
        else:
            return open_file

    # After X retries, raise previous IOError
    raise


def get_file_binary(path, retries=3, retry_errno=DEFAULT_RETRY_ERRNO):
    retries = (maybe_integer(retries) or 3) + 1
    while retries:
        try:
            with open(path, 'rb') as f:
                binary = f.read()
        except IOError as error:
            if error.errno is errno.ENOENT:
                return None
            elif error.errno in retry_errno:
                # Try again, or not!
                retries -= 1
            else:
                # Something goes wrong
                raise
        else:
            return binary

    # After X retries, raise previous IOError
    raise


def put_binary_on_file(
        path,
        binary,
        mode='wb',
        retries=3,
        retry_errno=DEFAULT_RETRY_ERRNO,
        make_dir_recursively=False):

    retries = (maybe_integer(retries) or 3) + 1
    while retries:
        try:
            with open(path, mode) as f:
                f.write(binary)
        except IOError as error:
            if error.errno is errno.ENOENT:
                # Missing folder, create and try again
                make_dir(dirname(path), make_dir_recursively=make_dir_recursively)
            elif error.errno in retry_errno:
                # Try again, or not!
                retries -= 1
            else:
                # Something goes wrong
                raise
        else:
            return True

    # After X retries, raise previous IOError
    raise


def get_dir_filenames(path, retries=3, retry_errno=DEFAULT_RETRY_ERRNO):
    retries = (maybe_integer(retries) or 3) + 1
    while retries:
        try:
            filenames = listdir(path)
        except OSError as error:
            if error.errno is errno.ENOENT:
                return []
            if error.errno in retry_errno:
                # Try again, or not!
                retries -= 1
            else:
                # Your path ends here!
                break
        else:
            return filenames

    # After X retries, raise previous OSError
    raise


def path_unique_code(path, block_size=OPEN_BLOCK_SIZE):
    with open(path, 'rb') as f:
        unique_code = file_unique_code(f, block_size=block_size)
    return unique_code


def file_unique_code(open_file, block_size=OPEN_BLOCK_SIZE):
    h = sha256()
    open_file.seek(0)
    block = open_file.read(block_size)

    while block:
        h.update(block)
        block = open_file.read(block_size)

    open_file.seek(0)
    return force_unicode(h.hexdigest())


def string_unique_code(value):
    value = force_string(value)
    return force_unicode(sha256(value).hexdigest())
