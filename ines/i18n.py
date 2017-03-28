# -*- coding: utf-8 -*-

from pyramid.i18n import get_localizer as base_get_localizer, make_localizer
from pyramid.interfaces import ILocalizer, ITranslationDirectories
from pyramid.threadlocal import get_current_request
from translationstring import TranslationString

from ines.convert import clear_price, maybe_string


ES_COUNTRIES = [
    'AD', 'AR', 'BO', 'BZ', 'CL', 'CO', 'CR', 'CU', 'DO', 'EC', 'EH', 'ES', 'GI', 'GQ', 'GT', 'HN', 'MX', 'NI', 'PA',
    'PE', 'PR', 'PY', 'SV', 'UY', 'VE',
]
FR_COUNTRIES = [
    'BE', 'BF', 'BI', 'CD', 'CF', 'CH', 'CI', 'CM', 'DJ', 'DZ', 'FR', 'GA', 'GP', 'HT', 'KM', 'LU', 'MA', 'MG', 'ML',
    'MQ', 'MU', 'RW', 'SC', 'SN', 'TG', 'TN', 'VU',
]
PT_COUNTRIES = [
    'AO', 'BR', 'CV', 'GW', 'MO', 'MZ', 'PT', 'ST', 'TL',
]
# TODO
# https://en.wikipedia.org/wiki/List_of_official_languages
# https://en.wikipedia.org/wiki/List_of_official_languages_by_country_and_territory
# http://www.internetworldstats.com/languages.htm


def InesTranslationStringFactory(factory_domain):
    def create(msgid, mapping=None, default=None, context=None):
        if isinstance(msgid, TranslationString):
            domain = msgid.domain or factory_domain
        else:
            msgid = maybe_string(msgid)
            domain = factory_domain
        return TranslationString(msgid, domain=domain, default=default, mapping=mapping, context=context)
    return create


_ = InesTranslationStringFactory('ines')

WEEKDAYS = {0: _('Monday'),
            1: _('Tuesday'),
            2: _('Wednesday'),
            3: _('Thursday'),
            4: _('Friday'),
            5: _('Saturday'),
            6: _('Sunday')}

MONTHS = {1: _('January'),
          2: _('February'),
          3: _('March'),
          4: _('April'),
          5: _('May'),
          6: _('June'),
          7: _('July'),
          8: _('August'),
          9: _('September'),
          10: _('October'),
          11: _('November'),
          12: _('December')}

# Points: year, month, day, hour, weekday. When 1, means is in use
DATE_POINTS = {
    11111: _('${week}, ${day} ${month} ${year} ${hour}'),
    11101: _('${week}, ${day} ${month} ${year}'),
    11110: _('${day} ${month} ${year} ${hour}'),
    11100: _('${day} ${month} ${year}'),
    11010: _('${month} ${year} ${hour}'),
    11000: _('${month} ${year}'),
    10010: _('${year} ${hour}'),
    10000: _('${year}'),
    11011: _('${week}, ${month} ${year} ${hour}'),
    11001: _('${week}, ${month} ${year}'),
    10111: _('${week}, ${day} ${year} ${hour}'),
    10101: _('${week}, ${day} ${year}'),
    10110: _('${day} ${year} ${hour}'),
    10100: _('${day} ${year}'),
    10011: _('${week}, ${year} ${hour}'),
    10001: _('${week}, ${year}'),
    1111: _('${week}, ${day} ${month} ${hour}'),
    1101: _('${week}, ${day} ${month}'),
    1110: _('${day} ${month} ${hour}'),
    1100: _('${day} ${month}'),
    1011: _('${week}, ${month} ${hour}'),
    1001: _('${week}, ${month}'),
    1010: _('${month} ${hour}'),
    1000: _('${month}'),
    111: _('${week}, ${day} ${hour}'),
    101: _('${week}, ${day}'),
    110: _('${day} ${hour}'),
    100: _('${day}'),
    11: _('${week} ${hour}'),
    1: _('${week}'),
    0: _('No data')}

# Metric options
METER = 'm'
METRICS = [
    ('ym', _('yoctometre')),
    ('zm', _('zeptometre')),
    ('am', _('attometre')),
    ('fm', _('femtometre')),
    ('pm', _('picometre')),
    ('nm', _('nanometre')),
    ('µm', _('micrometre')),
    ('mm', _('millimetre')),
    ('cm', _('centimetre')),
    ('dm', _('decimetre')),
    (METER, _('metre')),
    ('dam', _('decametre')),
    ('hm', _('hectometre')),
    ('km', _('kilometre')),
    ('Mm', _('megametre')),
    ('Gm', _('gigametre')),
    ('Tm', _('terametre')),
    ('Pm', _('petametre')),
    ('Em', _('exametre')),
    ('Zm', _('zettametre')),
    ('Ym', _('yottametre'))]

METRIC_I18N = dict(METRICS)
METRIC_KEYS = METRIC_I18N.keys()

METRIC_NUMBERS = dict(enumerate(METRIC_KEYS))
METRIC_TYPES = {v: k for k, v in METRIC_NUMBERS.items()}


def get_localizer(registry, locale_name):
    localizer = registry.queryUtility(ILocalizer, name=locale_name)
    if localizer is not None:
        return localizer

    tdirs = registry.queryUtility(ITranslationDirectories, default=[])
    localizer = make_localizer(locale_name, tdirs)
    registry.registerUtility(localizer, ILocalizer, name=locale_name)
    return localizer


def translate_factory(request, locale_name=None):
    if not locale_name or locale_name == request.locale_name:
        return base_get_localizer(request).translate

    translator = get_localizer(request.registry, locale_name).translate

    def method(tstring, **kwargs):
        default = kwargs.pop('default', None)
        if tstring is not None:
            return translator(tstring, **kwargs) or default
        else:
            return default
    return method


def translate(request, tstring, locale_name=None):
    return translate_factory(request, locale_name)(tstring)


def translate_date(request, year=None, month=None, day=None, hour=None, minute=None, weekday=None):
    translator = request.translator

    points = 0
    mapping = {}
    if year:
        points += 10000
        mapping['year'] = year
    if month:
        points += 1000
        mapping['month'] = translator(MONTHS[month])
    if day:
        points += 100
        mapping['day'] = day
    if hour or minute:
        points += 10
        if hour and minute:
            mapping['hour'] = '%s:%s' % (hour, minute)
        elif hour:
            mapping['hour'] = '%sh' % hour
        else:
            mapping['hour'] = '%smin' % minute
    if weekday:
        points += 1
        mapping['week'] = translator(WEEKDAYS[weekday])

    message = DATE_POINTS[points]
    if mapping:
        message.mapping = mapping

    return translator(message)


def translate_date_object(request, date, with_weekday=False):
    return translate_date(
        request,
        year=date.year, month=date.month, day=date.day,
        weekday=with_weekday and date.weekday() or None)


def translate_datetime(request, date):
    return translate_date(
        request,
        year=date.year, month=date.month, day=date.day,
        hour=date.hour, minute=date.minute,
        weekday=date.weekday())


def translate_month_factory(request):
    translator = request.translator
    return lambda month: translator(MONTHS[month])


def translate_month(request, month):
    return translate_month_factory(request or get_current_request())(month)


def translate_metric_factory(request, metric=METER, to_metric=None, round_to=None):
    metric_format = format_metric_factory(metric, to_metric, round_to)
    translator = request.translator
    get_title = METRIC_I18N.get

    if to_metric:
        title = get_title(to_metric)
        if title:
            title = translator(title)
        title = title or to_metric

        def method(value):
            number = metric_format(value)
            return ' '.join([number, title])

    else:
        def method(value):
            number, key = metric_format(value)

            title = get_title(key)
            if title:
                title = translator(title)
            else:
                title = key

            return ' '.join([number, title])

    return method


def translate_metric(request, value, metric=METER, to_metric=None, round_to=None):
    return translate_metric_factory(request, metric, to_metric, round_to)(value)


def format_metric_factory(metric=METER, to_metric=None, round_to=None):
    """ Return function to convert metric values.
    Tries return a ``int`` type, is not decimal numbers, else return a
    ``float`` type.

    The returned function receives a number, ``value``.
    If ``to_metric`` is defined, only returns the convert ``value``,
    else returns a tuple with converted ``value`` and ``value`` metric type,
    for example ``(10, 'km')``.


    Arguments
    =========

    ``metric``
        Metric type sent to function.
        By default metre type is defined.
        See :attr:`ucpa_apps.convert.metric_list` for possible metric types.

    ``to_metric``
        Metric type returned.
        If this arguments is not defined, the return will be the closer
        metric value between 1 and 9.
        See :attr:`ucpa_apps.convert.metric_list` for possible metric types.

    ``round_to``
        Number of decimal numbers to be returned.

    """
    get_number = METRIC_TYPES.get
    number = get_number(metric)
    if number is None:
        raise ValueError('Invalid metric type: %s' % metric)

    get_type = METRIC_NUMBERS.get
    if to_metric:
        to_number = get_number(to_metric)
        if to_number is None:
            raise ValueError('Invalid metric type: %s' % to_metric)

        elif to_number == number:
            method = lambda value: value

        else:
            if to_number < number:
                count_number = -1
                calc_method = lambda num: num * 10
            else:
                count_number = 1
                calc_method = lambda num: num / 10

            def method(value):
                check_number = number

                while True:
                    key = get_type(check_number + count_number)
                    check_number = get_number(key)
                    value = calc_method(value)
                    if key == to_metric:
                        break

                return value

        def replacer(value):
            value = float(value)
            value = method(value)
            value = float(value)

            if value.is_integer():
                value = int(value)
            elif round_to is not None:
                value = round(value, round_to)
                if value.is_integer():
                    value = int(value)

            return value
    else:
        options = 1, lambda num: num / 10
        options_reverse = -1, lambda num: num * 10

        def method(value):
            result_key = metric
            check_number = number

            if value < 1:
                count_number, calc_method = options_reverse
            else:
                count_number, calc_method = options

            while True:
                if 1 <= value <= 9:
                    break

                key = get_type(check_number + count_number)
                if not key:
                    break

                check_number = METRIC_TYPES[key]
                result_key = key
                value = calc_method(value)

            return value, result_key

        def replacer(value):
            value = float(value)
            value, key = method(value)

            if value.is_integer():
                value = int(value)
            elif round_to is not None:
                value = round(value, round_to)
                if value.is_integer():
                    value = int(value)

            return value, key

    return replacer


def format_metric(value, metric=METER, to_metric=None, round_to=None):
    """ Convert metric value.
    Check :meth:`format_metric_factory` for more details.

    .. note:: To speed things, cache :meth:`format_metric_factory`.
    """
    return format_metric_factory(metric, to_metric, round_to)(value)


def metric_to_string_factory(metric=METER, to_metric=None, round_to=None):
    """ Return function to convert metric value to ``str`` string.
    Check :meth:`format_metric_factory` for more details.

    The returned function receives a number, and the result of this is a
    ``str`` value with number and metric type, for example ``10 km``.
    """
    factory = format_metric_factory(metric, to_metric, round_to)

    if to_metric:
        pattern_value = '%s {0}'.format(to_metric)
        return lambda value: pattern_value % str(factory(value))
    else:
        return lambda value: '%s %s' % factory(value)


def metric_to_string(value, metric=METER, to_metric=None, round_to=None):
    """ Convert metric value to ``str`` string.
    Check :meth:`metric_to_string_factory` for more details.

    .. note:: To speed things, cache :meth:`metric_to_string_factory`.
    """
    return metric_to_string_factory(metric, to_metric, round_to)(value)


class NumberDescription(object):
    unit_i18n = {
        0: 'zero',
        1: 'um',
        2: 'dois',
        3: 'três',
        4: 'quatro',
        5: 'cinco',
        6: 'seis',
        7: 'sete',
        8: 'oito',
        9: 'nove',
        10: 'dez',
        11: 'onze',
        12: 'doze',
        13: 'treze',
        14: 'quatorze',
        15: 'quinze',
        16: 'dezesseis',
        17: 'dezessete',
        18: 'dezoito',
        19: 'dezanove',
    }

    ten_i18n = {
        2: 'vinte',
        3: 'trinta',
        4: 'quarenta',
        5: 'cinquenta',
        6: 'sessenta',
        7: 'setenta',
        8: 'oitenta',
        9: 'noventa',
    }

    hundred_plural_i18n = {
        1: 'cento',
        2: 'duzentos',
        3: 'trezentos',
        4: 'quatrocentos',
        5: 'quinhentos',
        6: 'seiscentos',
        7: 'setecentos',
        8: 'oitocentos',
        9: 'novecentos',
    }
    hundred_i18n = hundred_plural_i18n.copy()
    hundred_i18n.update({
        1: 'cem'
    })

    # Number = length of right blocks
    thousand_plural_i18n = {
        1: '%s mil',
        2: '%s milhões',
        3: '%s mil milhões',
        4: '%s biliões',
        5: '%s mil biliões',
        6: '%s triliões',
        7: '%s mil triliões',
        8: '%s quatriliões',
        9: '%s mil quatriliões',
        10: '%s quintiliões',
        11: '%s mil quintiliões',
        12: '%s sextiliões',
        13: '%s mil sextiliões',
        14: '%s septiliões',
        15: '%s mil septiliões'
    }
    thousand_i18n = thousand_plural_i18n.copy()
    thousand_i18n.update({
        1: 'mil',
        2: 'um milhão',
        3: 'mil milhões',
        4: 'um bilião',
        5: 'mil bilião',
        6: 'um trilião',
        7: 'mil triliões',
        8: 'um quatrilião',
        9: 'mil quatriliões',
        10: 'um quintilião',
        11: 'mil quintiliões',
        12: 'um sextilião',
        13: 'mil sextiliões',
        14: 'um septilião',
        15: 'mil septiliões'
    })

    def __init__(self, block_sep=' e ', thousand_sep=' e '):
        self.block_sep = block_sep
        self.thousand_sep = thousand_sep

    def __call__(self, number):
        number = int(number)
        description = self.unit_i18n.get(number)
        if description:
            return description

        descriptions = []
        number_str = str(number)
        next_blocks_length = -1

        while number_str:
            deep_number = int(number_str[-3:])
            number_str = number_str[:-3]
            next_blocks_length += 1

            if not deep_number:
                continue
            elif next_blocks_length and deep_number == 1:
                descriptions.insert(0, (self.thousand_i18n[next_blocks_length], False, next_blocks_length))
                continue

            deep_descriptions = []
            add_deep_description = deep_descriptions.append

            if deep_number > 99:
                deep_number_str = str(deep_number)
                deep_number = int(deep_number_str[1:])
                hundred_digit = int(deep_number_str[0])

                if deep_number:
                    add_deep_description(self.hundred_plural_i18n[hundred_digit])
                else:
                    add_deep_description(self.hundred_i18n[hundred_digit])

            with_hundred = deep_number > 19
            if with_hundred:
                deep_number_str = str(deep_number)
                deep_number = int(deep_number_str[1:])
                add_deep_description(self.ten_i18n[int(deep_number_str[0])])

            if deep_number:
                add_deep_description(self.unit_i18n[deep_number])

            deep_description = self.block_sep.join(deep_descriptions)
            if next_blocks_length:
                deep_description = self.thousand_plural_i18n[next_blocks_length] % deep_description

            descriptions.insert(0, (deep_description, with_hundred, next_blocks_length))

        response = descriptions.pop(0)[0]
        for i, (description, with_hundred, next_blocks_length) in enumerate(descriptions):
            if next_blocks_length:
                response += ', '
            elif not with_hundred:
                response += self.thousand_sep
            else:
                response += ' '
            response += description

        return response


find_number_description = NumberDescription()

CURRENCY_I18N = {
    'eur': ('euro', 'euros')
}


def translate_price(request, price, currency='eur', sep=' e '):
    number_str, decimal_str = str(clear_price(price)).split('.')

    number = int(number_str)
    currency_description = CURRENCY_I18N[currency]
    response = '%s %s' % (find_number_description(number), currency_description[int(number != 1)])

    decimal = int(decimal_str)
    if decimal:
        response += '%s%s cêntimos' % (sep, find_number_description(decimal))

    return response.capitalize()
