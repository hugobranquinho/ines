# -*- coding: utf-8 -*-

from json import loads
import re as REGEX
from string import ascii_lowercase

import colander
from colander import Invalid, SchemaNode, String

from ines.convert import maybe_list, maybe_string, to_string
from ines.exceptions import Error
from ines.i18n import _
from ines.utils import find_next_prime, maybe_phone_number, validate_skype_username


CODES = {}

PORTUGUESE_CC_LETTER_MAPPING = {str(D): D for D in range(10)}
PORTUGUESE_CC_LETTER_MAPPING.update((str(L), i) for i, L in enumerate(ascii_lowercase, 10))

GPS_REGEX = REGEX.compile('^N:[-]{0,1}[0-9.]{1,10} W:[-]{0,1}[0-9.]{1,10}$')


def parse_and_validate_code(name, validation, value):
    if validation:
        if isinstance(validation, str):
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
        if not to_string(value).isnumeric():
            raise Invalid(node, _('Invalid number'))


# See: https://www.cartaodecidadao.pt/images/stories/Algoritmo_Num_Documento_CC.pdf
@register_code
class isPortugueseCC(object):
    length = 12

    def __call__(self, node, value):
        number = to_string(value).lower()
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
        number = maybe_phone_number(value)
        if not number:
            raise Invalid(node, _('Invalid number'))
        else:
            return number


@register_code
class codeValidation(object):
    def __init__(self, length, reverse=False, startswith=None):
        self.length = int(length)
        self.reverse = reverse
        self.startswith = [str(n) for n in maybe_list(startswith)]

    def __call__(self, node, value):
        try:
            value = validate_code(node.name, value, self.length, reverse=self.reverse, startswith=self.startswith)
        except Error as error:
            raise Invalid(node, error.message)
        else:
            return value


def validate_code(key, value, length, reverse=False, startswith=None):
    value = maybe_string(value)
    if not value:
        raise Error(key, _('Required'))

    number = value.strip().lower()
    if not number.isnumeric():
        raise Error(key, _('Need to be a number'))
    elif len(number) != length:
        raise Error(key, _('Need to have ${length} digits', mapping={'length': length}))

    if startswith and str(number[0]) not in startswith:
        startswith_str = '"%s"' % '", "'.join(startswith)
        raise Error(key, _('Need to start with ${chars}', mapping={'chars': startswith_str}))

    last_number = int(number[-1])
    number = number[:-1]

    if reverse:
        number_list = reversed(number)
    else:
        number_list = list(number)

    prime = find_next_prime(length)
    check_digit = prime - (sum(i * int(d) for i, d in enumerate(number_list, 2)) % prime)
    if last_number != check_digit:
        raise Error(key, _('Invalid number'))
    else:
        return value


def validate_pt_nif(key, value):
    return validate_code(key, value, length=9, reverse=True)


def validate_pt_post_address(postal_address):
    if '-' not in postal_address:
        raise Error('postal_address', _('Invalid postal address'))

    cp4, cp3 = postal_address.split('-', 1)
    if not cp4.isnumeric() or not cp3.isnumeric():
        raise Error('postal_address', _('Invalid postal address'))

    cp4 = int(cp4)
    if cp4 < 1000 or cp4 > 9999:
        raise Error('postal_address', _('First number must be between 1000 and 9999'))
    elif len(cp3) != 3:
        raise Error('postal_address', _('Second number must have 3 digits'))


URL_PROTOCOLS = ['http://', 'https://']


def validate_url(key, value, required=False):
    url = maybe_string(value)
    if required and (not url or url in URL_PROTOCOLS):
        raise Error(key, 'Obrigatório')

    elif url and url not in URL_PROTOCOLS:
        for protocol in URL_PROTOCOLS:
            if url.startswith(protocol):
                if len(url[len(protocol):]) < 2:
                    raise Error(key, 'É necessário indicar um url')
                return url

        raise Error(key, 'O url tem de começar por "%s"' % '" ou "'.join(URL_PROTOCOLS))
