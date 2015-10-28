# -*- coding: utf-8 -*-

import datetime
from json import dumps
from re import compile as regex_compile
from time import mktime

from pyramid.compat import is_nonstr_iter

from six import binary_type
from six import integer_types
from six import PY3
from six import text_type
from six import u

from ines import CAMELCASE_UPPER_WORDS
from ines import lru_cache


DATE = datetime.date
DATETIME = datetime.datetime
REPLACE_CAMELCASE_REGEX = regex_compile(u('[^A-Z0-9_.]')).sub
CLEAR_SPACES_REGEX = regex_compile(' +').sub

NULLS = frozenset([u('null'), u(''), u('none')])


def to_unicode(value, encoding='utf-8', errors='strict'):
    if isinstance(value, text_type):
        return value
    elif isinstance(value, binary_type):
        return value.decode(encoding, errors)
    else:
        return text_type(value)


def to_bytes(value, encoding='utf-8', errors='strict'):
    if isinstance(value, binary_type):
        return value
    elif isinstance(value, text_type):
        return value.encode(encoding, errors)
    elif PY3:
        return text_type(value).encode(encoding, errors)
    else:
        return binary_type(value)


if PY3:
    to_string = to_unicode
else:
    def to_string(value, encoding='utf-8', errors='strict'):
        if isinstance(value, str):
            return value
        elif isinstance(value, text_type):  # unicode py2.*
            return value.encode(encoding, errors)
        else:
            return str(value)


def unicode_join(sep, items):
    return to_unicode(sep).join(map(to_unicode, items))


def bytes_join(sep, items):
    return to_bytes(sep).join(map(to_bytes, items))


def string_join(sep, items):
    return to_string(sep).join(map(to_string, items))


def maybe_integer(value):
    try:
        result = int(value)
    except (TypeError, ValueError):
        pass
    else:
        return result


def maybe_null(value):
    if value is None:
        return None
    elif to_unicode(value).strip().lower() in NULLS:
        return None
    else:
        return value


def maybe_unicode(value, encoding='utf-8', errors='strict'):
    if value or value is 0:
        return to_unicode(value, encoding, errors)


def maybe_bytes(value, encoding='utf-8', errors='strict'):
    if value or value is 0:
        return to_bytes(value, encoding, errors)


def maybe_string(value, encoding='utf-8', errors='strict'):
    if value or value is 0:
        return to_string(value, encoding, errors)


@lru_cache(5000)
def _camelcase(value):
    value = to_unicode(value).strip()
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

    return unicode_join('', camelcase_words)


def camelcase(value):
    value = to_unicode(value).strip()
    if not value:
        return value
    elif '+' in value:
        return unicode_join('+', map(_camelcase, value.split('+')))
    else:
        return _camelcase(value)


@lru_cache(5000)
def uncamelcase(value):
    value = to_unicode(value)

    count = 0
    words = {}
    previous_is_upper = False
    for letter in to_unicode(value):
        if letter.isupper() or letter.isnumeric():
            if not previous_is_upper:
                count += 1
            else:
                maybe_upper_name = (unicode_join('', words[count]) + letter).upper()
                if maybe_upper_name not in CAMELCASE_UPPER_WORDS:
                    count += 1
            previous_is_upper = True

        else:
            if previous_is_upper:
                maybe_upper_name = (unicode_join('', words[count]) + letter).upper()
                if maybe_upper_name not in CAMELCASE_UPPER_WORDS:
                    words[count + 1] = [words[count].pop()]
                    count += 1
            previous_is_upper = False

        words.setdefault(count, []).append(letter)

    words = sorted(words.items())

    final_words = []
    for count, letters in words:
        if letters:
            final_words.append(unicode_join('', letters))

    return unicode_join('_', final_words).lower()


VOWEL = frozenset(('a', 'e', 'i', 'o', 'u('))
IGNORE_WORDS = frozenset(('by', ))


# See http://www.csse.monash.edu.au/~damian/papers/HTML/Plurals.html # Pluralizing algorithms
# @@ TODO: improve this
# https://github.com/blakeembrey/pluralize/blob/master/pluralize.js
def pluralizing_word(word):
    word = to_unicode(word)

    lower_word = word.lower()
    if lower_word.isnumeric():
        return word + 's'
    elif lower_word in IGNORE_WORDS:
        return word
    elif lower_word.endswith('ed'):
        return word

    # By default add "S"
    to_append = u('s')

    if lower_word.endswith('ss'):
        to_append = u('es')

    elif (lower_word.endswith('y')
            and len(lower_word) > 1
            and lower_word[-2] not in VOWEL
            and lower_word != 'soliloquy'):
        word = word[:-1]
        to_append = u('ies')

    if word.isupper():
        return word + to_append.upper()
    else:
        return word + to_append


def pluralizing_key(key, only_last=True):
    if only_last:
        words = key.rsplit('_', 1)
        words[-1] = pluralizing_word(words[-1])
        return unicode_join('_', words)
    else:
        return unicode_join('_', map(pluralizing_word, key.split('_')))


def json_dumps(value, none_as_str=False):
    value = prepare_for_json(value, none_as_str)
    return dumps(value)


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

    elif isinstance(value, (float, integer_types)):
        return value

    elif isinstance(value, dict):
        return prepare_dict_for_json(value, minify)

    elif is_nonstr_iter(value):
        return prepare_iter_for_json(value, minify)

    elif isinstance(value, (DATE, DATETIME)):
        if minify:
            return mktime(value.timetuple())
        else:
            return value.isoformat()

    else:
        return to_unicode(value)


def prepare_dict_for_json(values, minify=False):
    return dict(
        (to_unicode(key), prepare_for_json(value, minify))
        for key, value in values.items())


def prepare_iter_for_json(values, minify=False):
    return [prepare_for_json(value, minify) for value in values]


def clear_spaces(value):
    return CLEAR_SPACES_REGEX(' ', to_string(value))
