# -*- coding: utf-8 -*-

import datetime
from math import ceil

from sqlalchemy import func


NOW = datetime.datetime.now


def define_pagination(query, page, limit_per_page):
    number_of_results = (
        query
        .with_entities(func.count(1))
        .first()[0])

    all_pages = str(limit_per_page).lower() == 'all'
    if all_pages:
        last_page = 1
        limit_per_page = 'all'
    else:
        last_page = int(ceil(number_of_results / float(limit_per_page))) or 1

    if page > last_page:
        page = last_page

    if not all_pages:
        end_slice = page * limit_per_page
        start_slice = end_slice - limit_per_page
        query = query.slice(start_slice, end_slice)

    return QueryPagination(
        query,
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

    @property
    def number_of_page_results(self):
        return len(self)
