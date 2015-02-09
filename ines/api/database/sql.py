# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

from math import ceil

from pyramid.decorator import reify
from pyramid.settings import asbool
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import func
from sqlalchemy import MetaData
from sqlalchemy import or_
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.pool import NullPool
from zope.sqlalchemy import ZopeTransactionExtension

from ines.api import BaseSessionManager
from ines.api.database import BaseSQLSession
from ines.convert import maybe_integer
from ines.convert import maybe_unicode
from ines.middlewares.repozetm import RepozeTMMiddleware
from ines.utils import MissingDict
from ines.utils import MissingSet


SQL_DBS = MissingDict()


class BaseDatabaseSessionManager(BaseSessionManager):
    __api_name__ = 'database'
    __middlewares__ = [RepozeTMMiddleware]

    def __init__(self, *args, **kwargs):
        super(BaseDatabaseSessionManager, self).__init__(*args, **kwargs)

        self.db_session = initialize_sql(
            self.config.application_name,
            **get_sql_settings_from_config(self.config))


class BaseDatabaseSession(BaseSQLSession):
    __api_name__ = 'database'


def initialize_sql(
        application_name,
        sql_path,
        encoding='utf8',
        mysql_engine='InnoDB',
        debug=False):

    sql_path = '%s?charset=%s' % (sql_path, encoding)
    SQL_DBS[application_name]['sql_path'] = sql_path
    is_mysql = sql_path.lower().startswith('mysql://')

    if is_mysql:
        base = SQL_DBS[application_name].get('base')
        if base is not None:
            append_arguments(base, 'mysql_charset', encoding)

    metadata = SQL_DBS[application_name].get('metadata')

    # Set defaults for MySQL tables
    if is_mysql and metadata:
        for table in metadata.sorted_tables:
            append_arguments(table, 'mysql_engine', mysql_engine)
            append_arguments(table, 'mysql_charset', encoding)

    SQL_DBS[application_name]['engine'] = engine = create_engine(
        sql_path,
        echo=debug,
        poolclass=NullPool,
        encoding=encoding)

    session_maker = sessionmaker(extension=ZopeTransactionExtension())
    session = scoped_session(session_maker)
    session.configure(bind=engine)
    SQL_DBS[application_name]['session'] = session

    indexed_columns = SQL_DBS[application_name]['indexed_columns'] = MissingSet()
    if metadata is not None:
        metadata.bind = engine
        metadata.create_all(engine)

        # Force indexes creation
        for table in metadata.sorted_tables:
            if table.indexes:
                for index in table.indexes:
                    for column in index.columns._all_columns:
                        indexed_columns[table.name].add(column.name)

                    try:
                        index.create()
                    except OperationalError:
                        pass

    return session


def get_sql_settings_from_config(config):
    sql_path = config.settings['sql.path']
    kwargs = {
        'sql_path': sql_path,
        'encoding': config.settings.get('sql.encoding', 'utf8')}

    if sql_path.startswith('mysql://'):
        kwargs['mysql_engine'] = config.settings.get(
            'sql.mysql_engine',
            'InnoDB')

    if 'sql.debug' in config.settings:
        kwargs['debug'] = asbool(config.settings['sql.debug'])
    else:
        kwargs['debug'] = config.debug

    return kwargs


def sql_declarative_base(application_name, encoding='utf8'):
    metadata = MetaData()
    metadata.application_name = application_name
    SQL_DBS[application_name]['metadata'] = metadata

    base = declarative_base(metadata=metadata)
    SQL_DBS[application_name]['base'] = base
    return base


def append_arguments(obj, key, value):
    arguments = getattr(obj, '__table_args__', None)
    if arguments is None:
        arguments = obj.__table_args__ = {key: value}

    elif isinstance(arguments, dict):
        if key not in arguments:
            arguments[key] = value

    elif isinstance(arguments, tuple):
        last_arguments_dict = None
        new_arguments = list(arguments)
        for argument in new_arguments:
            if isinstance(argument, dict):
                last_arguments_dict = argument
                if key in argument:
                    break
        else:
            if last_arguments_dict is None:
                new_arguments.append({key: value})
            else:
                last_arguments_dict[key] = value

            obj.__table_args__ = tuple(new_arguments)


def maybe_with_none(column, values, query=None):
    queries = []
    values = set(values)
    if None in values:
        values.remove(None)
        queries.append(column == None)

    if len(values) == 1:
        queries.append(column == values.pop())
    elif values:
        queries.append(column.in_(values))

    query_filter = None
    if len(queries) == 1:
        query_filter = queries[0]
    elif queries:
        query_filter = or_(*queries)

    if query is None:
        return query_filter
    elif query_filter is not None:
        return query.filter(query_filter)
    else:
        return query


def like_maybe_with_none(column, values, query=None):
    queries = []
    values = set(values)
    if None in values:
        values.remove(None)
        queries.append(column == None)
    for value in values:
        like_filter = create_like_filter(column, value)
        if like_filter is not None:
            queries.append(like_filter)

    query_filter = None
    if len(queries) == 1:
        query_filter = queries[0]
    elif queries:
        query_filter = or_(*queries)

    if query is None:
        return query_filter
    elif query_filter is not None:
        return query.filter(query_filter)
    else:
        return query


def create_like_filter(column, value):
    value = maybe_unicode(value)
    if value:
        words = value.split()
        if words:
            like_str = u'%%%s%%' % u'%'.join(words)
            return column.like(like_str)


def create_rlike_filter(column, value):
    value = maybe_unicode(value)
    if value:
        words = value.split()
        if words:
            rlike_str = u'(%s)' % u'|'.join(words)
            return column.op('rlike')(rlike_str)


class Pagination(list):
    def __init__(self, query, page, limit_per_page):
        super(Pagination, self).__init__()

        self.page = maybe_integer(page)
        if not self.page or self.page < 1:
            self.page = 1

        self.limit_per_page = maybe_integer(limit_per_page)
        if not self.limit_per_page or self.limit_per_page < 1:
            self.limit_per_page = 20
        elif self.limit_per_page > 5000:
            self.limit_per_page = 5000

        if query is None:
            self.number_of_results = 1
            self.last_page = 1
        else:
            self.number_of_results = (
                query
                .with_entities(func.count(1))
                .first()[0])
            self.last_page = int(ceil(
                self.number_of_results / float(self.limit_per_page))) or 1

        if self.page > self.last_page:
            self.page = self.last_page

        if query is not None:
            end_slice = self.page * self.limit_per_page
            start_slice = end_slice - self.limit_per_page
            self.extend(query.slice(start_slice, end_slice).all())


class TablesSet(set):
    def have(self, table):
        return table in self

    def __contains__(self, table):
        table = getattr(table, '__table__', None)
        if table is not None:
            return set.__contains__(self, table)
        else:
            return False


class TemporaryColumnsLabel(dict):
    def __init__(self, options):
        self.options = options

    def get(self, name, default=None):
        columns = {}
        for key, column in self.items():
            if column.name != key:
                column = column.label(key)
            columns[key] = column

        self.options.columns = columns
        return columns.get(name, default)

    def __getitem__(self, name):
        column = self.get(name)
        if column is None:
            return self.options.columns[name]
        else:
            return column


class Options(MissingDict):
    def clone(self):
        new = Options()
        new.add_columns(**self.columns)
        return new

    def add_columns(self, **columns):
        for key, column in columns.items():
            self.add_column(key, column)

    @reify
    def columns(self):
        return TemporaryColumnsLabel(self)

    def add_table(self, table, ignore=None, add_name=None):
        columns = table.__dict__.keys()
        for key in columns:
            maybe_column = getattr(table, key)
            if isinstance(maybe_column, (Column, InstrumentedAttribute)):
                if not ignore or key not in ignore:
                    if add_name:
                        key = '%s_%s' % (add_name, key)
                    self.add_column(key, maybe_column)

    def add_column(self, key, column):
        self.columns[key] = column

    def get(self, attributes=None):
        if not attributes:
            attributes = self.columns.keys()

        columns = Columns()
        for attribute in set(attributes):
            if attribute is not None and attribute in self.columns:
                column = self.columns[attribute]
                columns.append(column)
                columns.tables.update(get_object_tables(column))

        return columns

    def structure_order_by(self, *arguments):
        result = []
        add_order = result.append
        for argument in arguments:
            if isinstance(argument, (tuple, list)):
                column_name, reverse = argument
            else:
                column_name = argument
                reverse = False

            column = self.columns[column_name]
            if reverse:
                add_order(column.desc())
            else:
                add_order(column)

        return result


def get_object_tables(value):
    tables = set()
    table = getattr(value, 'table', None)
    if table is not None:
        tables.add(table)
    elif hasattr(value, '_element'):
        # Label column
        table = getattr(value._element, 'table', None)
        if table is not None:
            tables.add(table)
        else:
            tables.update(get_object_tables(value._element))
    else:
        # Function
        for clause in value.clauses:
            tables.update(get_object_tables(clause))
    return tables


class Columns(list):
    def __init__(self):
        super(Columns, self).__init__()
        self.tables = TablesSet()
