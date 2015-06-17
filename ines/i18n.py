# -*- coding: utf-8 -*-

from pyramid.i18n import get_localizer as base_get_localizer
from pyramid.i18n import make_localizer
from pyramid.interfaces import ILocalizer
from pyramid.interfaces import ITranslationDirectories

from ines import _
from ines.convert import format_metric_factory


WEEKDAYS = {0: _(u'Monday'),
            1: _(u'Tuesday'),
            2: _(u'Wednesday'),
            3: _(u'Thursday'),
            4: _(u'Friday'),
            5: _(u'Saturday'),
            6: _(u'Sunday')}

MONTHS = {1: _(u'January'),
          2: _(u'February'),
          3: _(u'March'),
          4: _(u'April'),
          5: _(u'May'),
          6: _(u'June'),
          7: _(u'July'),
          8: _(u'August'),
          9: _(u'September'),
          10: _(u'October'),
          11: _(u'November'),
          12: _(u'December')}

METRICS = {u'ym': _(u'yoctometre'),
           u'zm': _(u'zeptometre'),
           u'am': _(u'attometre'),
           u'fm': _(u'femtometre'),
           u'pm': _(u'picometre'),
           u'nm': _(u'nanometre'),
           u'µm': _(u'micrometre'),
           u'mm': _(u'millimetre'),
           u'cm': _(u'centimetre'),
           u'dm': _(u'decimetre'),
           u'm': _(u'metre'),
           u'dam': _(u'decametre'),
           u'hm': _(u'hectometre'),
           u'km': _(u'kilometre'),
           u'Mm': _(u'megametre'),
           u'Gm': _(u'gigametre'),
           u'Tm': _(u'terametre'),
           u'Pm': _(u'petametre'),
           u'Em': _(u'exametre'),
           u'Zm': _(u'zettametre'),
           u'Ym': _(u'yottametre')}


# Points: year, month, day, weekday. When 1 means is requested
DATE_POINTS = {
    1111: _(u'${week}, ${day} ${month}, ${year}'),
    1110: _(u'${day} ${month}, ${year}'),
    1100: _(u'${month} ${year}'),
    1000: _(u'${year}'),
    1101: _(u'${week}, ${month} ${year}'),
    1011: _(u'${week}, ${day} of ${year}'),
    1010: _(u'${day} of ${year}'),
    1001: _(u'${week}, ${year}'),
    111: _(u'${week}, ${day} ${month}'),
    110: _(u'${day} ${month}'),
    101: _(u'${week}, ${month}'),
    100: _(u'${month}'),
    11: _(u'${week}, ${day}'),
    10: _(u'${day}'),
    1: _(u'${week}'),
    0: _(u'No data')}


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


def translate_metric_factory(request, metric=u'm', to_metric=None, round_to=None):
    metric_format = format_metric_factory(metric, to_metric, round_to)
    translator = request.translator
    get_title = METRICS.get

    if to_metric:
        title = get_title(to_metric)
        if title:
            title = translator(title)
        title = title or to_metric

        def method(value):
            number = metric_format(value)
            return u'%s %s' % (str(number), title)

    else:
        def method(value):
            number, key = metric_format(value)

            title = get_title(key)
            if title:
                title = translator(title)
            else:
                title = key

            return u'%s %s' % (str(number), title)

    return method


def translate_metric(request, value, metric=u'm', to_metric=None, round_to=None):
    return translate_metric_factory(request, metric, to_metric, round_to)(value)
