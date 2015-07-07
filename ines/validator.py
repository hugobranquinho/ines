# -*- coding: utf-8 -*-

from json import loads
from string import ascii_lowercase

import colander
from colander import SchemaNode
from colander import String
from colander import Invalid

from ines import _
from ines.convert import force_unicode
from ines.exceptions import Error
from ines.url import get_url_body
from ines.utils import find_next_prime
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


@register_code
class codeValidation(object):
    def __init__(self, length, reverse=False):
        self.length = int(length)
        self.prime = find_next_prime(self.length)
        self.reverse = reverse

    def __call__(self, node, value):
        number = force_unicode(value).lower()
        if number and number.isnumeric() and len(number) == self.length:
            last_number = int(number[-1])
            number = number[:-1]

            if self.reverse:
                number_list = reversed(number)
            else:
                number_list = list(number)

            check_digit = self.prime - (sum(i * int(d) for i, d in enumerate(number_list, 2)) % self.prime)
            if last_number == check_digit:
                return True

        raise Invalid(node, _(u'Invalid number'))


def validate_pt_post_address(postal_address):
    if u'-' not in postal_address:
        raise Error('postal_address', _(u'Invalid postal address'))

    cp4, cp3 = postal_address.split(u'-', 1)
    if not cp4.isnumeric() or not cp3.isnumeric():
        raise Error('postal_address', _(u'Invalid postal address'))

    cp4 = int(cp4)
    if cp4 < 1000 or cp4 > 9999:
        raise Error('postal_address', _(u'First number must be between 1000 and 9999'))
    elif len(cp3) != 3:
        raise Error('postal_address', _(u'Second number must have 3 digits'))

    response = get_url_body(
        url='https://www.ctt.pt/feapl_2/app/open/postalCodeSearch/postalCodeSearch.jspx',
        data={'method:searchPC2': 'Procurar', 'cp4': cp4, 'cp3': cp3},
        method='post')

    block = response.split(u'highlighted-result', 1)[1].split(u'</div>', 1)[0]
    line_1 = block.split(u'subheader">', 1)[1].split('<', 1)[0].title()
    locality = block.rsplit(u'subheader">', 1)[1].split('<', 1)[0].title()

    return {
        'line_1': line_1,
        'locality': locality}
