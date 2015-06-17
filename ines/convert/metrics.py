# -*- coding: utf-8 -*-

# Metric options
METRIC_KEYS = [
    u'ym', u'zm', u'am', u'fm', u'pm', u'nm', u'µm', u'mm', u'cm', u'dm', u'm', u'dam', u'hm', u'km',
    u'Mm', u'Gm', u'Tm', u'Pm', u'Em', u'Zm', u'Ym']
METRIC_NUMBERS = dict(enumerate(METRIC_KEYS))
METRIC_TYPES = dict((v, k) for k, v in METRIC_NUMBERS.items())


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
        raise ValueError(u'Invalid metric type: %s' % metric)

    get_type = METRIC_NUMBERS.get
    if to_metric:
        to_number = get_number(to_metric)
        if to_number is None:
            raise ValueError(u'Invalid metric type: %s' % to_metric)

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

    if to_metric:
        def replacer(value):
            value = float(value)
            value = method(value)

            if value.is_integer():
                value = int(value)
            elif round_to is not None:
                value = round(value, round_to)
                if value.is_integer():
                    value = int(value)

            return value

    else:
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


def format_metric(value, metric=u'm', to_metric=None, round_to=None):
    """ Convert metric value.
    Check :meth:`format_metric_factory` for more details.

    .. note:: To speed things, cache :meth:`format_metric_factory`.
    """
    return format_metric_factory(metric, to_metric, round_to)(value)


def metric_to_unicode_factory(metric=u'm', to_metric=None, round_to=None):
    """ Return function to convert metric value to ``unicode`` string.
    Check :meth:`format_metric_factory` for more details.

    The returned function receives a number, and the result of this is a
    ``unicode`` value with number and metric type, for example ``10 km``.
    """
    factory = format_metric_factory(metric, to_metric, round_to)

    if to_metric:
        pattern_value = u'%s %s' % (u'%s', to_metric)
        return lambda value: pattern_value % str(factory(value))
    else:
        return lambda value: u'%s %s' % factory(value)


def metric_to_unicode(value, metric=u'm', to_metric=None, round_to=None):
    """ Convert metric value to ``unicode`` string.
    Check :meth:`metric_to_unicode_factory` for more details.

    .. note:: To speed things, cache :meth:`metric_to_unicode_factory`.
    """
    return metric_to_unicode_factory(metric, to_metric, round_to)(value)
