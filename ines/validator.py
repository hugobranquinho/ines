# -*- coding: utf-8 -*-

from functools import lru_cache
from json import loads
from string import ascii_lowercase

import colander
from colander import SchemaNode
from colander import String
from colander import Invalid
from six import string_types
from six import text_type
from six import u

from ines.convert import maybe_list
from ines.convert import to_unicode
from ines.exceptions import Error
from ines.i18n import _
from ines.url import get_url_body
from ines.utils import find_next_prime
from ines.utils import validate_phone_number
from ines.utils import validate_skype_username


CODES = {}

PORTUGUESE_CC_LETTER_MAPPING = dict((text_type(D), D) for D in range(10))
PORTUGUESE_CC_LETTER_MAPPING.update(dict((text_type(L), i) for i, L in enumerate(ascii_lowercase, 10)))


def parse_and_validate_code(name, validation, value):
    if validation:
        if isinstance(validation, string_types):
            validation = loads(validation)

        node = SchemaNode(String(), name=name)
        for key, key_options in validation.items():
            validator = getattr(colander, key, None) or CODES[key]
            new_code = validator(**(key_options or {}))(node, value)
            if new_code is not None and not isinstance(new_code, bool):
                value = new_code

    return value


def register_code(class_):
    CODES[class_.__name__] = class_
    return class_


@register_code
class isInteger(object):
    def __call__(self, node, value):
        if not to_unicode(value).isnumeric():
            raise Invalid(node, _('Invalid number'))


# See: https://www.cartaodecidadao.pt/images/stories/Algoritmo_Num_Documento_CC.pdf
@register_code
class isPortugueseCC(object):
    length = 12

    def __call__(self, node, value):
        number = to_unicode(value).lower()
        if len(number) != self.length:
            raise Invalid(node, _('Need to have ${length} chars', mapping={'length': self.length}))

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

        if digit_sum % 10:
            raise Invalid(node, _('Invalid code'))


@register_code
class isSkype(object):
    def __init__(self, validate_with_api=False):
        self.validate_with_api = validate_with_api

    def __call__(self, node, value):
        if not validate_skype_username(value, validate_with_api=self.validate_with_api):
            raise Invalid(node, _('Invalid "skype" username'))


@register_code
class isPhoneNumber(object):
    def __call__(self, node, value):
        number = validate_phone_number(value)
        if not number:
            raise Invalid(node, _('Invalid number'))
        else:
            return number


@register_code
class codeValidation(object):
    def __init__(self, length, reverse=False, startswith=None):
        self.length = int(length)
        self.prime = find_next_prime(self.length)
        self.reverse = reverse
        self.startswith = [str(n) for n in maybe_list(startswith)]

    def __call__(self, node, value):
        number = to_unicode(value).lower()
        if not number:
            raise Invalid(node, _('Required'))
        elif not number.isnumeric():
            raise Invalid(node, _('Need to be a integer'))
        elif len(number) != self.length:
            raise Invalid(node, _('Need to have ${length} digits', mapping={'length': self.length}))

        if self.startswith and str(number[0]) not in self.startswith:
            startswith_str = '"%s"' % '", "'.join(self.startswith)
            raise Invalid(node, _('Need to start with ${chars}', mapping={'chars': startswith_str}))

        last_number = int(number[-1])
        number = number[:-1]

        if self.reverse:
            number_list = reversed(number)
        else:
            number_list = list(number)

        check_digit = self.prime - (sum(i * int(d) for i, d in enumerate(number_list, 2)) % self.prime)
        if last_number != check_digit:
            raise Invalid(node, _('Invalid number'))


@lru_cache(1000)
def get_pt_postal_address(cp4, cp3):
    response = get_url_body(
        url='https://www.ctt.pt/feapl_2/app/open/postalCodeSearch/postalCodeSearch.jspx',
        data={'method:searchPC2': 'Procurar', 'cp4': cp4, 'cp3': cp3},
        method='post')

    block = response.split(u('highlighted-result'), 1)[1].split(u('</div>'), 1)[0]
    address = block.split(u('subheader">'), 1)[1].split('<', 1)[0].title()
    locality = block.rsplit(u('subheader">'), 1)[1].split('<', 1)[0].title()

    return address, locality


def validate_pt_post_address(postal_address, search_address=False):
    if u('-') not in postal_address:
        raise Error('postal_address', _('Invalid postal address'))

    cp4, cp3 = postal_address.split(u('-'), 1)
    if not cp4.isnumeric() or not cp3.isnumeric():
        raise Error('postal_address', _('Invalid postal address'))

    cp4 = int(cp4)
    if cp4 < 1000 or cp4 > 9999:
        raise Error('postal_address', _('First number must be between 1000 and 9999'))
    elif len(cp3) != 3:
        raise Error('postal_address', _('Second number must have 3 digits'))

    if search_address:
        return get_pt_postal_address(cp4, cp3)
