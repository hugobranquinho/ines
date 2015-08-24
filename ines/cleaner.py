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

MAPPING = {
    u('Š'): u('S'), u('š'): u('s'), u('Ž'): u('Z'), u('ž'): u('z'), u('À'): u('A'), u('Ð'): u('Dj'),
    u('Á'): u('A'), u('Â'): u('A'), u('Ã'): u('A'), u('Ä'): u('A'), u('Å'): u('A'), u('Æ'): u('A'),
    u('Ç'): u('C'), u('È'): u('E'), u('É'): u('E'), u('Ê'): u('E'), u('Ë'): u('E'), u('Ì'): u('I'),
    u('Í'): u('I'), u('Î'): u('I'), u('Ï'): u('I'), u('Ñ'): u('N'), u('Ò'): u('O'), u('Ó'): u('O'),
    u('Ô'): u('O'), u('Õ'): u('O'), u('Ö'): u('O'), u('Ø'): u('O'), u('Ù'): u('U'), u('Ú'): u('U'),
    u('Û'): u('U'), u('Ü'): u('U'), u('Ý'): u('Y'), u('Þ'): u('B'), u('à'): u('a'), u('ß'): u('Ss'),
    u('á'): u('a'), u('â'): u('a'), u('ã'): u('a'), u('ä'): u('a'), u('å'): u('a'), u('æ'): u('a'),
    u('ç'): u('c'), u('è'): u('e'), u('é'): u('e'), u('ê'): u('e'), u('ë'): u('e'), u('ì'): u('i'),
    u('í'): u('i'), u('î'): u('i'), u('ï'): u('i'), u('ð'): u('o'), u('ñ'): u('n'), u('ò'): u('o'),
    u('ó'): u('o'), u('ô'): u('o'), u('õ'): u('o'), u('ö'): u('o'), u('ø'): u('o'), u('ù'): u('u('),
    u('ú'): u('u'), u('ý'): u('y'), u('þ'): u('b'), u('ÿ'): u('y'), u('ƒ'): u('f'), u(' '): u(' ')}

MAPPING.update(STRING_TO_DICT(base_letters + base_digits))

FILENAME_MAPPING = MAPPING.copy()
FILENAME_MAPPING.update(STRING_TO_DICT('.-_ '))

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
    cleaned = clean_unicode(value, replace_case='_', mapping_dict=FILENAME_MAPPING)
    return cleaned.encode('utf-8')


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
