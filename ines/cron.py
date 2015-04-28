# -*- coding: utf-8 -*-
# Copyright (C) University of Coimbra. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

from calendar import monthrange
import datetime

from ines.convert import force_unicode
from ines.convert import maybe_integer
from ines.exceptions import NoMoreDates
from ines.utils import add_months
from ines.utils import last_day_of_month_for_weekday


NOW_DATE = datetime.datetime.now
MINYEAR = datetime.MINYEAR
MAXYEAR = datetime.MAXYEAR
TIMEDELTA = datetime.timedelta

DATES_RANGES = {
    'year': (MINYEAR, MAXYEAR),
    'month': (1, 12),
    'day': (1, 31),
    'hour': (0, 23),
    'minute': (0, 59),
    'second': (0, 59),
    'weekday': (0, 6)}


class AllYears(object):
    def __init__(self, start_year):
        self.start_year = int(start_year)
        if self.start_year < MINYEAR:
            self.start_year = MINYEAR

    def __iter__(self):
        last_year = self.start_year
        yield last_year

        while True:
            last_year += 1
            if last_year > MAXYEAR:
                break
            yield last_year


def get_nearest(value, options, range_value):
    for options_value in options:
        if options_value >= value:
            return options_value - value
    return options[0] - value + range_value


def find_seconds(options):
    def finder(value):
        nearest = get_nearest(value.second, options, 60)
        if nearest:
            return value + TIMEDELTA(seconds=nearest)
    return finder


def find_minutes(options):
    def finder(value):
        nearest = get_nearest(value.minute, options, 60)
        if nearest:
            return value.replace(second=0) + TIMEDELTA(minutes=nearest)
    return finder


def find_hours(options):
    def finder(value):
        nearest = get_nearest(value.hour, options, 24)
        if nearest:
            return value.replace(minute=0, second=0) + TIMEDELTA(hours=nearest)
    return finder


def find_days(options):
    def finder(value):
        options_copy = list(options)
        month_days = monthrange(value.year, value.month)[1]
        if max(options) > month_days:
            while max(options_copy) > month_days:
                options_copy.pop()
        return get_nearest(value.day, options_copy, month_days)
    return finder


def find_last_day(value):
    month_days = monthrange(value.year, value.month)[1]
    return get_nearest(value.day, [month_days], month_days)


def find_weekdays(options):
    return lambda value: get_nearest(value.weekday(), options, 7)


def find_last_weekday_of_month(weekday):
    def finder(value):
        last_weekday = last_day_of_month_for_weekday(value.year, value.month, weekday).day
        if last_weekday > value.day:
            return last_weekday - value.day
        elif last_weekday < value.day:
            next_month = datetime.date(value.year, value.month, 1)
            next_month = add_months(next_month, 1)
            last_weekday = last_day_of_month_for_weekday(
                next_month.year,
                next_month.month,
                weekday).day
            month_days = monthrange(value.year, value.month)[1]
            return last_weekday - value.day + month_days
        else:
            return 0
    return finder


def find_days_and_weekdays(
        days=None,
        last_day_of_month=False,
        last_weekday_of_month=None,
        weekdays=None):

    finders = []
    if days:
        finders.append(find_days(days))

    if last_day_of_month:
        finders.append(find_last_day)

    if weekdays:
        finders.append(find_weekdays(weekdays))

    if last_weekday_of_month is not None:
        finders.append(find_last_weekday_of_month(last_weekday_of_month))

    if finders:
        def finder(value):
            found_days = set()
            for find in finders:
                found_days.add(find(value))
            if found_days:
                nearest = min(found_days)
                if nearest:
                    value += TIMEDELTA(days=nearest)
                    return value.replace(hour=0, minute=0, second=0)
        return finder


def find_months(options):
    def finder(value):
        nearest = get_nearest(value.month, options, 12)
        if nearest:
            return add_months(value, nearest).replace(day=1, hour=0, minute=0, second=0)
    return finder


def format_crontab_options(**kwargs):
    for key in kwargs.keys():
        if key not in DATES_RANGES:
            raise ValueError('Invalid cron key %s' % key)

    options = {}
    for key, (start_range, end_range) in DATES_RANGES.items():
        values = kwargs.get(key)
        if values is None:
            continue

        if not hasattr(values, '__iter__'):
            values = [values]
        if u'*' in values:
            continue

        key_options = set()
        for value in set(values):
            value = force_unicode(value)

            if key == 'day' and value.lower() == u'l':
                options['last_day_of_month'] = True
                continue

            int_values = set()
            ignore_options_add = False

            if key == 'weekday' and value.lower().endswith(u'l'):
                weekday = maybe_integer(value[0])
                if weekday is None:
                    message = 'Invalid %s integer on "%s"' \
                              % (key, value)
                    raise ValueError(message)
                int_values.add(weekday)
                options['last_weekday_of_month'] = weekday
                ignore_options_add = True

            elif value.isnumeric():
                int_values.add(int(value))

            elif value == u'?':
                now = NOW_DATE()
                if key == 'weekday':
                    int_values.add(now.weekday())
                else:
                    int_values.add(getattr(now, key))

            elif u'/' in value:
                range_int, interval = value.split(u'/', 1)

                interval = maybe_integer(interval)
                if interval is None or interval < 1:
                    message = 'Invalid %s interval for "%s"' % (key, value)
                    raise ValueError(message)

                if range_int == u'*':
                    start = start_range
                    end = end_range
                elif u'-' in range_int:
                    start, end = range_int.split('-', 1)
                    start = maybe_integer(start)
                    end = maybe_integer(end)
                    if start is None or end is None:
                        message = 'Invalid %s integer on "%s"' \
                                  % (key, value)
                        raise ValueError(message)
                else:
                    message = 'Invalid %s format "%s"' % (key, value)
                    raise ValueError(message)

                int_values.update(range(start, end + 1, interval))

            elif u',' in value:
                for int_value in value.split(','):
                    int_value = maybe_integer(int_value)
                    if int_value is None:
                        message = 'Invalid %s integer on "%s"' % (key, value)
                        raise ValueError(message)
                    int_values.add(int_value)

            elif u'-' in value:
                start, end = value.split('-', 1)
                start = maybe_integer(start)
                end = maybe_integer(end)
                if start is None or end is None:
                    message = 'Invalid %s integer on "%s"' % (key, value)
                    raise ValueError(message)
                int_values.update(range(start, end + 1))

            else:
                message = 'Invalid %s integer "%s"' % (key, value)
                raise ValueError(message)

            for int_value in int_values:
                if start_range > int_value or int_value > end_range:
                    message = (
                        'Invalid %s "%s". Start: %s End: %s'
                        % (key, value, start_range, end_range))
                    raise ValueError(message)

            if not ignore_options_add:
                key_options.update(int_values)

        if key_options:
            options[key] = sorted(key_options)

    return options


class Cron(object):
    def __init__(self, **kwargs):
        self.options = format_crontab_options(**kwargs)
        self.finders = []

        # Define seconds
        seconds = self.options.get('second')
        if seconds:
            self.finders.append(find_seconds(seconds))

        # Define minutes
        minutes = self.options.get('minute')
        if minutes:
            self.finders.append(find_minutes(minutes))

        # Define hours
        hours = self.options.get('hour')
        if hours:
            self.finders.append(find_hours(hours))

        # Define days
        finder = find_days_and_weekdays(
            days=self.options.get('day'),
            last_day_of_month=self.options.get('last_day_of_month'),
            last_weekday_of_month=self.options.get('last_weekday_of_month'),
            weekdays=self.options.get('weekday'))
        if finder:
            self.finders.append(finder)

        # Define months
        months = self.options.get('month')
        if months:
            self.finders.append(find_months(months))

    def find_next(self, next_date=None):
        next_date = next_date or NOW_DATE().replace(microsecond=0)
        next_date += TIMEDELTA(seconds=1)
        if not self.finders:
            # Every second
            return next_date

        years = self.options.get('year') or AllYears(next_date.year)
        for year in years:
            if year < next_date.year:
                continue
            elif year != next_date.year:
                next_date = next_date.replace(year=year)

            while next_date.year == year:
                for find_next_value in self.finders:
                    new_next_date = find_next_value(next_date)
                    if new_next_date:
                        next_date = new_next_date
                        break
                else:
                    return next_date

        raise NoMoreDates('jobs', 'No more dates')

    def __iter__(self):
        last_date = None
        while True:
            last_date = self.find_next(last_date)
            yield last_date
