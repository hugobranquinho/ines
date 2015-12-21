# -*- coding: utf-8 -*-

from calendar import monthrange
from collections import defaultdict
import datetime
import errno
from hashlib import sha256
from io import IOBase
from math import ceil
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
import re as REGEX
from uuid import uuid4
import warnings

from colander import Invalid
from pyramid.httpexceptions import HTTPError
from six import PY3
from six import u

try:
    from deform import ValidationFailure
except ImportError:
    pass

from ines import DOMAIN_NAME
from ines import DEFAULT_RETRY_ERRNO
from ines import NOW
from ines import OPEN_BLOCK_SIZE
from ines.cleaner import clean_phone_number
from ines.cleaner import normalize_full_name
from ines.convert import bytes_join
from ines.convert import camelcase
from ines.convert import compact_dump
from ines.convert import to_bytes
from ines.convert import to_string
from ines.convert import to_unicode
from ines.convert import maybe_integer
from ines.convert.codes import make_sha256_no_cache
from ines.i18n import translate_factory
from ines.url import open_json_url


DATE = datetime.date
TIMEDELTA = datetime.timedelta
PROCESS_ID = getpid()

# See: http://www.regular-expressions.info/email.html
EMAIL_REGEX = REGEX.compile(
    "[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*"
    "@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?")

# Skype REGEX validation
SKYPE_USERNAME_REGEX = REGEX.compile('^[a-z][a-z0-9\.,\-_]{5,31}$')


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


def infinitedict():
    return defaultdict(defaultdict)


def make_uuid_hash():
    return to_unicode(uuid4().hex)


def make_unique_hash(length=64):
    code = u('')
    while len(code) < length:
        code += make_sha256_no_cache(
            bytes_join(
                '.',
                (uuid4().hex,
                 str(NOW()),
                 str(PROCESS_ID),
                 str(DOMAIN_NAME))))
    return code[:length]


def last_read_file_time(path, retries=3, retry_errno=DEFAULT_RETRY_ERRNO):
    try:
        last_read_time = int(os_stat(path).st_atime)
    except OSError as error:
        if error.errno is errno.ENOENT:
            return None
        elif error.errno in retry_errno:
            # Try again, or not!
            retries -= 1
            if retries:
                return last_read_file_time(path, retries=retries, retry_errno=retry_errno)

        # Something goes wrong
        raise
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

        errors = defaultdict(list)
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
        message = getattr(error, 'msg', getattr(error, 'message', u('Undefined')))

    values = {
        'status': status,
        'property': key,
        'message': translate(message)}
    if kwargs:
        values.update(kwargs)
    return values


def format_error_to_json(error, kwargs=None, request=None):
    return compact_dump(format_error_to_json_values(error, kwargs, request=request))


def format_error_response_to_json(*args, **kwargs):
    return to_bytes(format_error_to_json(*args, **kwargs))


def file_modified_time(path, retries=3, retry_errno=DEFAULT_RETRY_ERRNO):
    try:
        modified_time = getmtime(path)
    except OSError as error:
        if error.errno is errno.ENOENT:
            return None
        elif error.errno in retry_errno:
            # Try again, or not!
            retries -= 1
            if retries:
                return file_modified_time(path, retries=retries, retry_errno=retry_errno)

        # Something goes wrong
        raise
    else:
        return modified_time


def validate_email(value):
    value = to_string(value)
    return bool(EMAIL_REGEX.match(value))


def maybe_email(value):
    if validate_email(value):
        return to_unicode(value)


def get_content_type(value):
    if value:
        return to_string(value).split(';', 1)[0].strip()


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
    for i in range(deep):
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
    month = value.month - 1 + int(months)
    year = value.year + int(month / 12)
    month = (month % 12) + 1
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
    try:
        _remove_file(path)
    except OSError as error:
        if error.errno is errno.ENOENT:
            # Already deleted!
            return False
        elif error.errno in retry_errno:
            # Try again, or not!
            retries -= 1
            if retries:
                return remove_file(path, retries=retries, retry_errno=retry_errno)

        # Something goes wrong
        raise
    else:
        return True


def remove_file_quietly(path, retries=3, retry_errno=DEFAULT_RETRY_ERRNO):
    try:
        remove_file(path, retries=retries, retry_errno=retry_errno)
    except (IOError, OSError):
        pass


def make_dir(path, mode=0o777, make_dir_recursively=False):
    path = to_string(path)
    try:
        mkdir(path, mode)
    except OSError as error:
        if make_dir_recursively and error.errno is errno.ENOENT:
            makedirs(path, mode)
        elif error.errno is not errno.EEXIST:
            raise
    return path


def move_file(path, new_path, retries=3, retry_errno=DEFAULT_RETRY_ERRNO):
    try:
        _rename_file(path, new_path)
    except OSError as error:
        if error.errno in retry_errno:
            # Try again, or not!
            retries -= 1
            if retries:
                return move_file(path, new_path, retries=retries, retry_errno=retry_errno)

        # Something goes wrong
        raise
    else:
        return True


def get_open_file(path, mode='rb', retries=3, retry_errno=DEFAULT_RETRY_ERRNO):
    try:
        open_file = open(path, mode)
    except IOError as error:
        if error.errno is errno.ENOENT:
            if 'r' not in mode:
                # Missing folder, create and try again
                make_dir(dirname(path))
            else:
                raise

        elif error.errno not in retry_errno:
            raise

        else:
            # Try again, or not!
            retries -= 1
            if not retries:
                raise

        return get_open_file(path, mode=mode, retries=retries, retry_errno=retry_errno)
    else:
        return open_file


def get_file_binary(path, mode='rb', retries=3, retry_errno=DEFAULT_RETRY_ERRNO):
    try:
        with open(path, mode) as f:
            binary = f.read()
    except IOError as error:
        if error.errno is errno.ENOENT:
            return None
        elif error.errno in retry_errno:
            # Try again, or not!
            retries -= 1
            if retries:
                return get_file_binary(path, mode=mode, retries=retries, retry_errno=retry_errno)

        # Something goes wrong
        raise
    else:
        return binary


def put_binary_on_file(
        path,
        binary,
        mode='wb',
        retries=3,
        retry_errno=DEFAULT_RETRY_ERRNO,
        make_dir_recursively=False):

    if 'b' in mode:
        binary = to_bytes(binary)
    else:
        binary = to_string(binary)

    try:
        with open(path, mode) as f:
            f.write(binary)
    except IOError as error:
        if error.errno is errno.ENOENT:
            # Missing folder, create and try again
            make_dir(dirname(path), make_dir_recursively=make_dir_recursively)
        elif error.errno not in retry_errno:
            raise
        else:
            # Try again, or not!
            retries -= 1
            if not retries:
                raise

        return put_binary_on_file(
            path,
            binary,
            mode=mode,
            retries=retries,
            retry_errno=retry_errno,
            make_dir_recursively=make_dir_recursively)
    else:
        return True


def get_dir_filenames(path, retries=3, retry_errno=DEFAULT_RETRY_ERRNO):
    try:
        filenames = listdir(path)
    except OSError as error:
        if error.errno is errno.ENOENT:
            return []
        if error.errno in retry_errno:
            # Try again, or not!
            retries -= 1
            if retries:
                return get_dir_filenames(path, retries=retries, retry_errno=retry_errno)

        # Something goes wrong
        raise
    else:
        return filenames


def path_unique_code(path, block_size=OPEN_BLOCK_SIZE):
    with open(path, 'rb') as f:
        unique_code = file_unique_code(f, block_size=block_size)
    return unique_code


def file_unique_code(open_file, block_size=OPEN_BLOCK_SIZE):
    h = sha256()
    open_file.seek(0)
    block = open_file.read(block_size)
    convert_to_bytes = bool('b' not in open_file.mode)

    while block:
        if convert_to_bytes:
            block = to_bytes(block)

        h.update(block)
        block = open_file.read(block_size)

    open_file.seek(0)
    return to_unicode(h.hexdigest())


def string_unique_code(value):
    value = to_bytes(value)
    return to_unicode(sha256(value).hexdigest())


def validate_skype_username(username, validate_with_api=False):
    if username:
        username = to_string(username)
        if SKYPE_USERNAME_REGEX.match(username):
            if not validate_with_api:
                return True

            response = open_json_url(
                'https://login.skype.com/json/validator',
                data={'new_username': username},
                method='get')
            if response['data']['markup'].lower() == 'skype name not available':
                return True

    return False


def validate_phone_number(number):
    if number:
        number = clean_phone_number(to_unicode(number))
        if 21 > len(number or '') > 3:
            return number


# See: http://golf.shinh.org/p.rb?Find+the+nearest+Prime
def is_prime(number):
    return pow(2, number - 1, number) < 2 > pow(13, number - 1, number)


def find_next_prime(number):
    number += 1
    number += not number % 2
    while True:
        if is_prime(number):
            return number
        number += 2


class PaginationClass(list):
    def __init__(self, page=1, limit_per_page=20):
        super(PaginationClass, self).__init__()

        if str(limit_per_page).lower() == 'all':
            self.limit_per_page = 'all'
        else:
            self.limit_per_page = maybe_integer(limit_per_page)
            if not self.limit_per_page or self.limit_per_page < 1:
                self.limit_per_page = 20

        self.number_of_results = 0

        self.page = maybe_integer(page)
        if not self.page or self.page < 1:
            self.page = 1
        self.last_page = self.page

    @property
    def number_of_page_results(self):
        return len(self)

    def set_number_of_results(self, number_of_results):
        self.number_of_results = int(number_of_results)

        if self.limit_per_page == 'all':
            self.last_page = 1
        else:
            self.last_page = int(ceil(number_of_results / float(self.limit_per_page))) or 1

        if self.page > self.last_page:
            self.page = self.last_page


def is_file_type(value):
    if PY3:
        return isinstance(value, IOBase)
    else:
        return isinstance(value, file)


def sort_with_none(iterable, key, reverse=False):
    def sort_key(item):
        value = getattr(item, key)
        return (value is None, value)
    iterable.sort(key=sort_key, reverse=reverse)


# http://detectmobilebrowsers.com/
MOBILE_REGEX_B = REGEX.compile(
    r"(android|bb\\d+|meego).+mobile|avantgo|bada\\/|blackberry|blazer|compal|elaine|fennec|hiptop|iemobile|ip("
    r"hone|od)|iris|kindle|lge |maemo|midp|mmp|mobile.+firefox|netfront|opera m(ob|in)i|palm( os)?|phone|p(ixi|r"
    r"e)\\/|plucker|pocket|psp|series(4|6)0|symbian|treo|up\\.(browser|link)|vodafone|wap|windows ce|xda|xiino",
    REGEX.I|REGEX.M)

MOBILE_REGEX_V = REGEX.compile(
    r"1207|6310|6590|3gso|4thp|50[1-6]i|770s|802s|a wa|abac|ac(er|oo|s\\-)|ai(ko|rn)|al(av|ca|co)|amoi|an(ex|ny|"
    r"yw)|aptu|ar(ch|go)|as(te|us)|attw|au(di|\\-m|r |s )|avan|be(ck|ll|nq)|bi(lb|rd)|bl(ac|az)|br(e|v)w|bumb|bw"
    r"\\-(n|u)|c55\\/|capi|ccwa|cdm\\-|cell|chtm|cldc|cmd\\-|co(mp|nd)|craw|da(it|ll|ng)|dbte|dc\\-s|devi|dica|dm"
    r"ob|do(c|p)o|ds(12|\\-d)|el(49|ai)|em(l2|ul)|er(ic|k0)|esl8|ez([4-7]0|os|wa|ze)|fetc|fly(\\-|_)|g1 u|g560|gene"
    r"|gf\\-5|g\\-mo|go(\\.w|od)|gr(ad|un)|haie|hcit|hd\\-(m|p|t)|hei\\-|hi(pt|ta)|hp( i|ip)|hs\\-c|ht(c(\\-| |_|a"
    r"|g|p|s|t)|tp)|hu(aw|tc)|i\\-(20|go|ma)|i230|iac( |\\-|\\/)|ibro|idea|ig01|ikom|im1k|inno|ipaq|iris|ja(t|v)a|"
    r"jbro|jemu|jigs|kddi|keji|kgt( |\\/)|klon|kpt |kwc\\-|kyo(c|k)|le(no|xi)|lg( g|\\/(k|l|u)|50|54|\\-[a-w])|li"
    r"bw|lynx|m1\\-w|m3ga|m50\\/|ma(te|ui|xo)|mc(01|21|ca)|m\\-cr|me(rc|ri)|mi(o8|oa|ts)|mmef|mo(01|02|bi|de|do|t"
    r"(\\-| |o|v)|zz)|mt(50|p1|v )|mwbp|mywa|n10[0-2]|n20[2-3]|n30(0|2)|n50(0|2|5)|n7(0(0|1)|10)|ne((c|m)\\-|on|t"
    r"f|wf|wg|wt)|nok(6|i)|nzph|o2im|op(ti|wv)|oran|owg1|p800|pan(a|d|t)|pdxg|pg(13|\\-([1-8]|c))|phil|pire|pl(ay"
    r"|uc)|pn\\-2|po(ck|rt|se)|prox|psio|pt\\-g|qa\\-a|qc(07|12|21|32|60|\\-[2-7]|i\\-)|qtek|r380|r600|raks|rim9|"
    r"ro(ve|zo)|s55\\/|sa(ge|ma|mm|ms|ny|va)|sc(01|h\\-|oo|p\\-)|sdk\\/|se(c(\\-|0|1)|47|mc|nd|ri)|sgh\\-|shar|si"
    r"e(\\-|m)|sk\\-0|sl(45|id)|sm(al|ar|b3|it|t5)|so(ft|ny)|sp(01|h\\-|v\\-|v )|sy(01|mb)|t2(18|50)|t6(00|10|18)"
    r"|ta(gt|lk)|tcl\\-|tdg\\-|tel(i|m)|tim\\-|t\\-mo|to(pl|sh)|ts(70|m\\-|m3|m5)|tx\\-9|up(\\.b|g1|si)|utst|v400|"
    r"v750|veri|vi(rg|te)|vk(40|5[0-3]|\\-v)|vm40|voda|vulc|vx(52|53|60|61|70|80|81|83|85|98)|w3c(\\-| )|webc|whit"
    r"|wi(g |nc|nw)|wmlb|wonu|x700|yas\\-|your|zeto|zte\\-",
    REGEX.I|REGEX.M)

def user_agent_is_mobile(user_agent):
    if user_agent:
        user_agent = to_string(user_agent)
        return MOBILE_REGEX_B.search(user_agent) or MOBILE_REGEX_V.search(user_agent[0:4])
    return False


def resolve_deform_error(form, error):
    if not ValidationFailure:
        raise NotImplementedError('deform package missing')

    if isinstance(error, Invalid):
        key = error.node.name
        message = error.msg
    else:
        key = error.key
        message = error.message

    form_error = Invalid(form, message)
    for child in form.children:
        if child.name == key:
            form_error[key] = message
            break
    else:
        form.hide_global_error = False

    form.widget.handle_error(form, form_error)
    return ValidationFailure(form, form_error.value, form_error)
