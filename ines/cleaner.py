# -*- coding: utf-8 -*-

import os
from string import digits as base_digits
from string import ascii_letters as base_letters

from six import u

from ines import IGNORE_FULL_NAME_WORDS
from ines.convert import maybe_unicode
from ines.convert import to_unicode
from ines.convert import unicode_join


STRING_TO_DICT = lambda s: dict((l, l) for l in to_unicode(s))

LOWER_MAPPING = (u('àáâãäåæÆßçèéêëƒìíîïñðòóôõöšýùúüûž'), u('aaaaaaaabceeeefiiiinoooooosyyuuuuz'))
UPPER_MAPPING = (LOWER_MAPPING[0].upper(), LOWER_MAPPING[1].upper())
MAPPING = dict(zip(*LOWER_MAPPING))
MAPPING.update(zip(*UPPER_MAPPING))
MAPPING.update({u('Ø'): u('O'), u('ø'): u('0'), u('þ'): u('b'), u(' '): u(' ')})
MAPPING.update(STRING_TO_DICT(base_letters + base_digits))

FILENAME_MAPPING = MAPPING.copy()
FILENAME_MAPPING.update(STRING_TO_DICT('.-_'))

PHONE_MAPPING = STRING_TO_DICT('x+' + base_digits)


def clean_unicode(value, replace_case=None, mapping_dict=None):
    value = to_unicode(value)
    get_letter = (mapping_dict or MAPPING).get
    if replace_case is not None:
        replace_case = to_unicode(replace_case)
        return unicode_join('', (get_letter(letter, replace_case) for letter in value))
    else:
        return unicode_join('', (get_letter(letter, letter) for letter in value))


def clean_filename(filename):
    value = to_unicode(filename, errors='ignore')
    return clean_unicode(value, replace_case='_', mapping_dict=FILENAME_MAPPING)


def clean_phone_number(value):
    value = to_unicode(value or '', errors='ignore').lower()
    return clean_unicode(value, replace_case='', mapping_dict=PHONE_MAPPING)


def normalize_full_name(value):
    value = maybe_unicode(value)
    if value:
        words = []
        for word in clean_unicode(value, replace_case='_').lower().split():
            if word not in IGNORE_FULL_NAME_WORDS:
                words.append(word)
        if words:
            return unicode_join(' ', words)

    return u('')


def clean_empty_folders(path):
    while True:
        deleted_folders = False
        for root, dirs, files in os.walk(path):
            if not dirs and not files:
                os.rmdir(root)
                deleted_folders = True
        if not deleted_folders:
            break


def reduce_string(value, max_length=None):
    if not value or not max_length:
        return value
    elif len(value) > max_length:
        return u('%s...') % value[:max_length]
    else:
        return value
