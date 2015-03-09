# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

import datetime
from math import ceil

from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import func
from sqlalchemy.orm.attributes import InstrumentedAttribute

from ines.api.core.database import CoreColumnParent
from ines.convert import camelcase


NOW_DATE = datetime.datetime.now


def define_pagination(query, page, limit_per_page):
    number_of_results = (
        query
        .with_entities(func.count(1))
        .first()[0])

    last_page = int(ceil(
        number_of_results / float(limit_per_page))) or 1

    if page > last_page:
        page = last_page

    end_slice = page * limit_per_page
    start_slice = end_slice - limit_per_page

    return QueryPagination(
        query.slice(start_slice, end_slice),
        page,
        limit_per_page,
        last_page,
        number_of_results)


class QueryPagination(object):
    def __init__(
            self,
            query,
            page,
            limit_per_page,
            last_page,
            number_of_results):
        self.query = query
        self.page = page
        self.limit_per_page = limit_per_page
        self.last_page = last_page
        self.number_of_results = number_of_results

    def __getattribute__(self, key):
        try:
            value = object.__getattribute__(self, key)
        except AttributeError:
            return getattr(self.query, key)
        else:
            return value


class CorePagination(list):
    def __init__(
            self,
            page,
            limit_per_page,
            last_page,
            number_of_results):
        super(CorePagination, self).__init__()
        self.page = page
        self.limit_per_page = limit_per_page
        self.last_page = last_page
        self.number_of_results = number_of_results


def detect_core_fields(
        table, route_name=None, url_key=None, params_key=None,
        ignore_keys=None, parent_name=None,
        ignore_core_keys=False):

    ignore_keys = set(ignore_keys or [])
    # Internal fields!
    ignore_keys.update(('id_core', 'type'))
    if ignore_core_keys:
        ignore_keys.update((
            'key', 'start_date', 'end_date',
            'updated_date', 'created_date'))

    values = {}
    for key, column in table.__dict__.items():
        if key in ignore_keys:
            continue
        elif isinstance(column, CoreColumnParent):
            column = column.get_core_column()
        elif not isinstance(column, InstrumentedAttribute):
            continue

        public_key = camelcase(key)
        if parent_name:
            key = '%s_%s' % (parent_name, key)

        if isinstance(column.type, (Date, DateTime)):
            values[public_key] = DateTimeField(key)
        else:
            values[public_key] = Field(key)

    if route_name and url_key:
        values['href'] = HrefField(
            route_name,
            url_key,
            params_key or url_key)

    return values
