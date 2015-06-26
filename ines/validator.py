# -*- coding: utf-8 -*-

from json import loads
from string import ascii_lowercase

import colander
from colander import SchemaNode
from colander import String
from colander import Invalid

from ines import _
from ines.convert import force_unicode
from ines.utils import validate_skype_username


CODES = {}

PORTUGUESE_CC_LETTER_MAPPING = dict((unicode(D), D) for D in xrange(10))
PORTUGUESE_CC_LETTER_MAPPING.update(dict((unicode(L), i) for i, L in enumerate(ascii_lowercase, 10)))


def validate_code(name, validation, value):
    if validation:
        node = SchemaNode(String(), name=name)
        for key, key_options in loads(validation).items():
            validator = getattr(colander, key, None) or CODES[key]
            validator(**(key_options or {}))(node, value)


def register_code(class_):
    CODES[class_.__name__] = class_
    return class_


@register_code
class isInteger(object):
    def __call__(self, node, value):
        if not force_unicode(value).isnumeric():
            raise Invalid(node, _(u'Invalid number'))


# See: https://www.cartaodecidadao.pt/images/stories/Algoritmo_Num_Documento_CC.pdf
@register_code
class isPortugueseCC(object):
    def __call__(self, node, value):
        number = force_unicode(value).lower()
        if len(number) == 12:
            digit_sum = 0
            second_digit = False
            for letter in reversed(number):
                digit = PORTUGUESE_CC_LETTER_MAPPING.get(letter)
                if second_digit:
                    digit *= 2
                    if digit > 9:
                        digit -= 9
                digit_sum += digit
                second_digit = not second_digit

            if not digit_sum % 10:
                return True

        raise Invalid(node, _(u'Invalid number'))


@register_code
class isSkype(object):
    def __init__(self, validate_with_api=False):
        self.validate_with_api = validate_with_api

    def __call__(self, node, value):
        if not validate_skype_username(value, validate_with_api=self.validate_with_api):
            raise Invalid(node, _(u'Invalid "skype" username'))
