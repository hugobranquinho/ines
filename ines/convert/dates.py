# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
# 
# @author Hugo Branquinho <hugobranq@gmail.com>
# 
# $Id$

from ines import _


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


def datetime_as_unicode(request, date_value):
    return date_as_unicode(
               request,
               year=date_value.year,
               month=date_value.month,
               day=date_value.day,
               weekday=date_value.weekday())


def month_as_unicode(request, month):
    return request.translate(MONTHS[month])


def date_as_unicode(request, year=None, month=None, day=None, weekday=None):
    translator = request.translator

    if year:
        if month:
            if day:
                if weekday:
                    message = _(u'${week}, ${day} ${month}, ${year}')
                else:
                    message = _(u'${day} ${month}, ${year}')
            elif weekday:
                message = _(u'${week}, ${month} ${year}')
            else:
                message = _(u'${month} ${year}')
        elif day:
            if weekday:
                message = _(u'${week}, ${day} of ${year}')
            else:
                message = _(u'${day} of ${year}')
        elif weekday:
            message = _(u'${week}, ${year}')
        else:
            message = _(u'${year}')

    elif month:
        if day:
            if weekday:
                message = _(u'${week}, ${day} ${month}')
            else:
                message = _(u'${day} ${month}')
        elif weekday:
            message = _(u'${week}, ${month}')
        else:
            message = _(u'${month}')

    elif day:
        if weekday:
            message = _(u'${week}, ${day}')
        else:
            message = _(u'${day}')

    elif weekday:
        message = _(u'${week}')

    else:
        message = _(u'Without date')
        return translator(message)

    mapping = {}
    if year:
        mapping['year'] = year
    if month:
        mapping['month'] = translator(MONTHS[month])
    if day:
        mapping['day'] = day
    if weekday:
        mapping['week'] = translator(WEEKDAYS[weekday])

    message.mapping = mapping
    return translator(message)
