# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
# 
# @author Hugo Branquinho <hugobranq@gmail.com>
# 
# $Id$

from ines import _


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

METRICS_LIST = METRICS.keys()
METRICS_NUMBERS = dict(enumerate(METRICS_LIST))
METRICS_TYPES = dict((value, key) for key, value in METRICS_NUMBERS.items())


def metric_as_unicode_factory(
        request,
        metric=u'm',
        to_metric=None,
        round_to=None,
        with_full_title=False):

    if with_full_title:
        translator = request.translator
        get_metric_title = METRICS.get

        def get_title(key):
            title = get_metric_title(key)
            if title:
                return translator(title)
            else:
                return key
    else:
        get_title = lambda title: title

    format_method = format_metric_factory(metric, to_metric, round_to)
    if to_metric:
        def convert_method(value):
            number = format_method(value)
            return u'%s %s' % (str(number), get_title(to_metric))

    else:
        def convert_method(value):
            number, title = format_method(value)
            return u'%s %s' % (str(number), get_title(title))

    return convert_method


def metric_as_unicode(request, value, *args, **kwargs):
    return metric_as_unicode_factory(request, *args, **kwargs)(value)


def format_metric_factory(metric=u'm', to_metric=None, round_to=None):
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
        See :attr:`ines.convert.METRICS_LIST` for possible metric types.

    ``to_metric``
        Metric type returned.
        If this arguments is not defined, the return will be the closer
        metric value between 1 and 9.
        See :attr:`ines.convert.METRICS_LIST` for possible metric types.

    ``round_to``
        Number of decimal numbers to be returned.

    """
    get_number = METRICS_TYPES.get
    get_type = METRICS_NUMBERS.get

    metric_number = get_number(metric)
    if metric_number is None:
        raise ValueError(u'Invalid metric type: %s' % metric)

    elif to_metric:
        to_metric_number = get_number(to_metric)
        if to_metric_number is None:
            raise ValueError(u'Invalid metric type: %s' % to_metric)

        elif to_metric_number == metric_number:
            convert_method = lambda value: value

        else:
            if to_metric_number < metric_number:
                count_number = -1
                next_metric_method = lambda number: number * 10
            else:
                count_number = 1
                next_metric_method = lambda number: number / 10

            def convert_method(value):
                check_number = metric_number

                while True:
                    key = get_type(check_number + count_number)
                    check_number = get_number(key)
                    value = next_metric_method(value)
                    if key == to_metric:
                        break

                return value

    else:
        options = 1, lambda number: number / 10
        options_reversed = -1, lambda number: number * 10

        def convert_method(value):
            result_key = metric
            check_number = metric_number

            if value < 1:
                count_number, next_metric_method = options_reversed
            else:
                count_number, next_metric_method = options

            while True:
                if 1 <= value <= 9:
                    break

                key = get_type(check_number + count_number)
                if not key:
                    break

                check_number = metric_types[key]
                result_key = key
                value = next_metric_method(value)

            return (value, result_key)

    if to_metric:
        def factory_method(value):
            value = float(value)
            value = convert_method(value)

            if value.is_integer():
                value = int(value)

            elif round_to is not None:
                value = round(value, round_to)
                if value.is_integer():
                    value = int(value)

            return value

    else:
        def factory_method(value):
            value = float(value)
            value, key = convert_method(value)

            if value.is_integer():
                value = int(value)

            elif round_to is not None:
                value = round(value, round_to)
                if value.is_integer():
                    value = int(value)

            return (value, key)

    return factory_method


def format_metric(value, *args, **kwargs):
    """ Convert metric value.
    Check :meth:`format_metric_factory` for more details.

    .. note:: To speed things, cache :meth:`format_metric_factory`.
    """
    return format_metric_factory(*args, **kwargs)(value)
