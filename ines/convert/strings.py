# -*- coding: utf-8 -*-

import datetime
from decimal import Decimal
from decimal import InvalidOperation
from functools import lru_cache
from json import dumps
from re import compile as regex_compile
from time import mktime

from pyramid.compat import is_nonstr_iter

from ines import CAMELCASE_UPPER_WORDS


DATE = datetime.date
DATETIME = datetime.datetime
REPLACE_CAMELCASE_REGEX = regex_compile('[^A-Z0-9_.]').sub
CLEAR_SPACES_REGEX = regex_compile(' +').sub

NULLS = frozenset(['null', '', 'none'])

BYTES_REFERENCES = ['B', 'kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']

VOWEL = frozenset(('a', 'e', 'i', 'o', 'u'))
IGNORE_WORDS = frozenset(('by', ))


def to_string(value, encoding='utf-8', errors='strict'):
    if isinstance(value, str):
        return value
    elif isinstance(value, bytes):
        return value.decode(encoding, errors)
    else:
        return str(value)


def to_bytes(value, encoding='utf-8', errors='strict'):
    if isinstance(value, bytes):
        return value
    elif isinstance(value, str):
        return value.encode(encoding, errors)
    else:
        return str(value).encode(encoding, errors)


def encode_and_decode(value, encoding='utf-8', errors='strict'):
    return to_string(to_bytes(value, encoding, errors), encoding, errors)


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


def maybe_decimal(value, scale=2):
    if value is not None:
        try:
            result = Decimal(value)
        except InvalidOperation:
            pass
        else:
            return result.quantize(Decimal(10) ** -scale)


def maybe_null(value):
    if value is None:
        return None
    elif to_string(value).strip().lower() in NULLS:
        return None
    else:
        return value


def maybe_string(value, encoding='utf-8', errors='strict'):
    if value or value is 0:
        return to_string(value, encoding, errors)


def maybe_bytes(value, encoding='utf-8', errors='strict'):
    if value or value is 0:
        return to_bytes(value, encoding, errors)


@lru_cache(5000)
def _camelcase(value):
    value = to_string(value).strip()
    if not value:
        return ''

    words = [word for word in REPLACE_CAMELCASE_REGEX('_', value.upper()).split('_') if word]
    if not words:
        return ''

    camelcase_words = [words.pop(0).lower()]
    add_word = camelcase_words.append
    for word in words:
        if word in CAMELCASE_UPPER_WORDS:
            add_word(word)
        else:
            add_word(word.capitalize())

    return ''.join(camelcase_words)


def camelcase(value):
    value = to_string(value).strip()
    if not value:
        return value
    elif '+' in value:
        return '+'.join(map(_camelcase, value.split('+')))
    else:
        return _camelcase(value)


@lru_cache(5000)
def uncamelcase(value):
    count = 0
    words = {}
    previous_is_upper = False
    for letter in to_string(value):
        if letter.isupper() or letter.isnumeric():
            if not previous_is_upper:
                count += 1
            else:
                maybe_upper_name = (''.join(words[count]) + letter).upper()
                if maybe_upper_name not in CAMELCASE_UPPER_WORDS:
                    count += 1
            previous_is_upper = True

        else:
            if previous_is_upper:
                maybe_upper_name = (''.join(words[count]) + letter).upper()
                if maybe_upper_name not in CAMELCASE_UPPER_WORDS:
                    words[count + 1] = [words[count].pop()]
                    count += 1
            previous_is_upper = False

        words.setdefault(count, []).append(letter)

    words = sorted(words.items())

    final_words = []
    for count, letters in words:
        if letters:
            final_words.append(''.join(letters))

    return '_'.join(final_words).lower()


# See http://www.csse.monash.edu.au/~damian/papers/HTML/Plurals.html # Pluralizing algorithms
# @@ TODO: improve this
# https://github.com/blakeembrey/pluralize/blob/master/pluralize.js
def pluralizing_word(word):
    word = to_string(word)

    lower_word = word.lower()
    if lower_word.isnumeric():
        return word + 's'
    elif lower_word in IGNORE_WORDS:
        return word
    elif lower_word.endswith('ed'):
        return word

    # By default add "S"
    to_append = 's'

    if lower_word.endswith('ss'):
        to_append = 'es'

    elif (lower_word.endswith('y')
            and len(lower_word) > 1
            and lower_word[-2] not in VOWEL
            and lower_word != 'soliloquy'):
        word = word[:-1]
        to_append = 'ies'

    if word.isupper():
        return word + to_append.upper()
    else:
        return word + to_append


def pluralizing_key(key, only_last=True):
    if only_last:
        words = key.rsplit('_', 1)
        words[-1] = pluralizing_word(words[-1])
        return '_'.join(words)
    else:
        return '_'.join(map(pluralizing_word, key.split('_')))


def compact_dump(values, **kwargs):
    return dumps(values, separators=(',', ':'), **kwargs)


def json_dumps(value, minify=False, **kwargs):
    value = prepare_for_json(value, minify)
    return compact_dump(value, **kwargs)


def prepare_for_json(value, minify=False):
    if value is None:
        if minify:
            return ''
        else:
            return None

    elif hasattr(value, '__json__'):
        return value.__json__()

    elif isinstance(value, bool):
        if minify:
            return value and 1 or 0
        else:
            return value

    elif isinstance(value, (float, int)):
        return value

    elif isinstance(value, dict):
        return prepare_dict_for_json(value, minify)

    elif is_nonstr_iter(value):
        return prepare_iter_for_json(value, minify)

    elif isinstance(value, (DATE, DATETIME)):
        if minify:
            return int(mktime(value.timetuple()))
        else:
            return value.isoformat()

    else:
        return to_string(value)


def prepare_dict_for_json(values, minify=False):
    return {to_string(key): prepare_for_json(value, minify) for key, value in values.items()}


def prepare_iter_for_json(values, minify=False):
    return [prepare_for_json(value, minify) for value in values]


def clear_spaces(value):
    return CLEAR_SPACES_REGEX(' ', to_string(value))


def bytes_to_string(bytes, round_to=2):
    if not bytes:
        return '0 B'
    else:
        bytes_float = float(bytes)
        references_length = len(BYTES_REFERENCES) - 1
        for i in range(references_length):
            number = bytes_float / 1000
            if number < 1.0:
                break
            bytes_float = number
        else:
            i = references_length

        response = round(bytes_float, round_to)
        if response.is_integer():
            response = int(response)

        return '%s %s' % (response, BYTES_REFERENCES[i])
