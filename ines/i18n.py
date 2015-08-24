# -*- coding: utf-8 -*-

from pyramid.i18n import get_localizer as base_get_localizer
from pyramid.i18n import make_localizer
from pyramid.interfaces import ILocalizer
from pyramid.interfaces import ITranslationDirectories
from six import u
from translationstring import TranslationString

from ines.convert.strings import maybe_unicode
from ines.convert import unicode_join


def InesTranslationStringFactory(factory_domain):
    def create(msgid, mapping=None, default=None, context=None):
        if isinstance(msgid, TranslationString):
            domain = msgid.domain or factory_domain
        else:
            msgid = maybe_unicode(msgid)
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

# Points: year, month, day, weekday. When 1 means is requested
DATE_POINTS = {
    1111: _('${week}, ${day} ${month}, ${year}'),
    1110: _('${day} ${month}, ${year}'),
    1100: _('${month} ${year}'),
    1000: _('${year}'),
    1101: _('${week}, ${month} ${year}'),
    1011: _('${week}, ${day} of ${year}'),
    1010: _('${day} of ${year}'),
    1001: _('${week}, ${year}'),
    111: _('${week}, ${day} ${month}'),
    110: _('${day} ${month}'),
    101: _('${week}, ${month}'),
    100: _('${month}'),
    11: _('${week}, ${day}'),
    10: _('${day}'),
    1: _('${week}'),
    0: _('No data')}

# Metric options
METER = u('m')
METRICS = [
    (u('ym'), _('yoctometre')),
    (u('zm'), _('zeptometre')),
    (u('am'), _('attometre')),
    (u('fm'), _('femtometre')),
    (u('pm'), _('picometre')),
    (u('nm'), _('nanometre')),
    (u('µm'), _('micrometre')),
    (u('mm'), _('millimetre')),
    (u('cm'), _('centimetre')),
    (u('dm'), _('decimetre')),
    (METER, _('metre')),
    (u('dam'), _('decametre')),
    (u('hm'), _('hectometre')),
    (u('km'), _('kilometre')),
    (u('Mm'), _('megametre')),
    (u('Gm'), _('gigametre')),
    (u('Tm'), _('terametre')),
    (u('Pm'), _('petametre')),
    (u('Em'), _('exametre')),
    (u('Zm'), _('zettametre')),
    (u('Ym'), _('yottametre'))]

METRIC_I18N = dict(METRICS)
METRIC_KEYS = METRIC_I18N.keys()

METRIC_NUMBERS = dict(enumerate(METRIC_KEYS))
METRIC_TYPES = dict((v, k) for k, v in METRIC_NUMBERS.items())


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


def translate_date(request, year=None, month=None, day=None, weekday=None):
    translator = request.translator

    points = 0
    mapping = {}
    if year:
        points += 1000
        mapping['year'] = year
    if month:
        points += 100
        mapping['month'] = translator(MONTHS[month])
    if day:
        points += 10
        mapping['day'] = day
    if weekday:
        points += 1
        mapping['week'] = translator(WEEKDAYS[weekday])

    message = DATE_POINTS[points]
    if mapping:
        message.mapping = mapping

    return translator(message)


def translate_datetime(request, date):
    return translate_date(request, year=date.year, month=date.month,
                          day=date.day, weekday=date.weekday())


def translate_month_factory(request):
    translator = request.translator
    return lambda month: translator(MONTHS[month])


def translate_month(request, month):
    return translate_month_factory(request)(month)


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
            return unicode_join(' ', [number, title])

    else:
        def method(value):
            number, key = metric_format(value)

            title = get_title(key)
            if title:
                title = translator(title)
            else:
                title = key

            return unicode_join(' ', [number, title])

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
        raise ValueError(u('Invalid metric type: %s') % metric)

    get_type = METRIC_NUMBERS.get
    if to_metric:
        to_number = get_number(to_metric)
        if to_number is None:
            raise ValueError(u('Invalid metric type: %s') % to_metric)

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


def metric_to_unicode_factory(metric=METER, to_metric=None, round_to=None):
    """ Return function to convert metric value to ``unicode`` string.
    Check :meth:`format_metric_factory` for more details.

    The returned function receives a number, and the result of this is a
    ``unicode`` value with number and metric type, for example ``10 km``.
    """
    factory = format_metric_factory(metric, to_metric, round_to)

    if to_metric:
        pattern_value = u('%s %s') % (u('%s'), to_metric)
        return lambda value: pattern_value % str(factory(value))
    else:
        return lambda value: u('%s %s') % factory(value)


def metric_to_unicode(value, metric=METER, to_metric=None, round_to=None):
    """ Convert metric value to ``unicode`` string.
    Check :meth:`metric_to_unicode_factory` for more details.

    .. note:: To speed things, cache :meth:`metric_to_unicode_factory`.
    """
    return metric_to_unicode_factory(metric, to_metric, round_to)(value)
