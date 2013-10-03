# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
# 
# @author Hugo Branquinho <hugobranq@gmail.com>
# 
# $Id$

from re import compile
from string import digits as BASE_DIGITS
from string import letters as BASE_LETTERS

from ines.convert import force_string
from ines.convert import force_unicode


CLEAN_PERCENTAGE_SUB = compile('%+').sub

MAPPING = {
    u'Š': u'S', u'š': u's', u'Ž': u'Z', u'ž': u'z', u'À': u'A', u'Ð': u'Dj',
    u'Á': u'A', u'Â': u'A', u'Ã': u'A', u'Ä': u'A', u'Å': u'A', u'Æ': u'A',
    u'Ç': u'C', u'È': u'E', u'É': u'E', u'Ê': u'E', u'Ë': u'E', u'Ì': u'I',
    u'Í': u'I', u'Î': u'I', u'Ï': u'I', u'Ñ': u'N', u'Ò': u'O', u'Ó': u'O',
    u'Ô': u'O', u'Õ': u'O', u'Ö': u'O', u'Ø': u'O', u'Ù': u'U', u'Ú': u'U',
    u'Û': u'U', u'Ü': u'U', u'Ý': u'Y', u'Þ': u'B', u'à': u'a', u'ß': u'Ss',
    u'á': u'a', u'â': u'a', u'ã': u'a', u'ä': u'a', u'å': u'a', u'æ': u'a',
    u'ç': u'c', u'è': u'e', u'é': u'e', u'ê': u'e', u'ë': u'e', u'ì': u'i',
    u'í': u'i', u'î': u'i', u'ï': u'i', u'ð': u'o', u'ñ': u'n', u'ò': u'o',
    u'ó': u'o', u'ô': u'o', u'õ': u'o', u'ö': u'o', u'ø': u'o', u'ù': u'u',
    u'ú': u'u', u'û': u'u', u'ý': u'y', u'ý': u'y', u'þ': u'b', u'ÿ': u'y',
    u'ƒ': u'f', u' ': u' '}
MAPPING.update(dict((unicode(L), unicode(L)) for L in BASE_LETTERS))
MAPPING.update(dict((unicode(D), unicode(D)) for D in BASE_DIGITS))

FILENAME_MAPPING = MAPPING.copy()
FILENAME_MAPPING.update({u'.': u'.', u'-': u'-', u' ': u'_', u'_': u'_'})


def clean_filename(filename):
    """ Clean filename replacing non-ascii chars with ascii chars.
    Invalid chars will be replaced with ``_``.
    Always return a ``str`` type.


    Arguments
    =========

    ``filename``
        Filename to be cleaned.

    """
    value = force_unicode(filename, 'ignore')
    cleaned = clean_unicode(value,
                            replace_letter=u'_',
                            mapping=FILENAME_MAPPING)

    return force_string(cleaned)


def clean_unicode(value, replace_letter=None, mapping=MAPPING):
    get_letter = mapping.get
    if replace_case is not None:
        return u''.join(get_letter(letter, replace_letter) for letter in value)
    else:
        return u''.join(get_letter(letter, letter) for letter in value)


def clean_percentage(value):
    value = force_string(value)
    return CLEAN_PERCENTAGE_SUB('%', value)
