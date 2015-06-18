# -*- coding: utf-8 -*-

from math import ceil

from pyramid.compat import is_nonstr_iter
from pyramid.decorator import reify
from pyramid.settings import asbool
from sqlalchemy import and_
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy import MetaData
from sqlalchemy import not_
from sqlalchemy import or_
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.pool import NullPool
from sqlalchemy.sql.expression import false
from sqlalchemy.sql.expression import true
from sqlalchemy.util._collections import lightweight_named_tuple

from ines.api import BaseSessionManager
from ines.api import BaseSession
from ines.convert import maybe_integer
from ines.convert import maybe_set
from ines.convert import maybe_unicode
from ines.exceptions import Error
from ines.middlewares.repozetm import RepozeTMMiddleware
from ines.path import get_object_on_path
from ines.views.fields import FilterBy
from ines.utils import MissingDict
from ines.utils import MissingSet


SQL_DBS = MissingDict()


class BaseSQLSessionManager(BaseSessionManager):
    __api_name__ = 'database'
    __middlewares__ = [RepozeTMMiddleware]

    @reify
    def __database_name__(self):
        return self.config.application_name

    def __init__(self, *args, **kwargs):
        super(BaseSQLSessionManager, self).__init__(*args, **kwargs)

        import transaction
        self.transaction = transaction

        self.db_session = initialize_sql(
            self.__database_name__,
            **get_sql_settings_from_config(self.config))


class BaseSQLSession(BaseSession):
    __api_name__ = 'database'

    def flush(self):
        self.session.flush()
        self.api_session_manager.transaction.commit()

    @reify
    def session(self):
        return self.api_session_manager.db_session()

    def rollback(self):
        self.api_session_manager.transaction.abort()

    def direct_insert(self, obj):
        values = {}
        for column in obj.__table__.c:
            name = column.name
            value = getattr(obj, name, None)
            if value is None and column.default:
                values[name] = column.default.execute()
            else:
                values[name] = value

        return (
            obj.__table__
            .insert(values)
            .execute(autocommit=True))

    def direct_delete(self, obj, query):
        return bool(
            obj.__table__
            .delete(query)
            .execute(autocommit=True)
            .rowcount)

    def direct_update(self, obj, query, values):
        for column in obj.__table__.c:
            name = column.name
            if name not in values and column.onupdate:
                values[name] = column.onupdate.execute()

        return (
            obj.__table__
            .update(query)
            .values(values)
            .execute(autocommit=True))


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

    if 'sql.session_extension' in config.settings:
        kwargs['session_extension'] = get_object_on_path(config.settings['sql.session_extension'])

    return kwargs


def initialize_sql(
        application_name,
        sql_path,
        encoding='utf8',
        mysql_engine='InnoDB',
        session_extension=None,
        debug=False):

    sql_path = '%s?charset=%s' % (sql_path, encoding)
    SQL_DBS[application_name]['sql_path'] = sql_path
    is_mysql = sql_path.lower().startswith('mysql://')

    if is_mysql:
        if 'bases' in SQL_DBS[application_name]:
            for base in SQL_DBS[application_name]['bases']:
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

    if session_extension:
        if callable(session_extension):
            session_extension = session_extension()
        session_maker = sessionmaker(extension=session_extension)
    else:
        session_maker = sessionmaker()

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
                    for column in getattr(index.columns, '_all_columns'):
                        indexed_columns[table.name].add(column.name)

                    try:
                        index.create()
                    except OperationalError:
                        pass

    return session


def append_arguments(obj, key, value):
    arguments = getattr(obj, '__table_args__', None)
    if arguments is None:
        obj.__table_args__ = {key: value}

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


def sql_declarative_base(application_name, **kwargs):
    if application_name not in SQL_DBS:
        metadata = MetaData()
        metadata.application_name = application_name
        SQL_DBS[application_name]['metadata'] = metadata
    else:
        metadata = SQL_DBS[application_name]['metadata']

    base = declarative_base(metadata=metadata, **kwargs)
    SQL_DBS[application_name].setdefault('bases', []).append(base)
    return base


def filter_query_with_queries(queries, query=None):
    """Filter 'query' with none/single/multiple OR'ed queries"""
    queries = [q for q in queries if q is not None]
    if len(queries) == 1:
        query_filter = queries[0]
    elif queries:
        query_filter = or_(*queries)
    else:
        return query

    if query is None:
        return query_filter
    elif query_filter is not None:
        return query.filter(query_filter)
    else:
        return query


def maybe_with_none(column, values, query=None):
    queries = []
    values = maybe_set(values)

    if None in values:
        values.remove(None)
        queries.append(column.is_(None))
    if len(values) == 1:
        queries.append(column == values.pop())
    elif values:
        queries.append(column.in_(values))

    return filter_query_with_queries(queries, query)


def like_maybe_with_none(column, values, query=None):
    queries = []
    values = maybe_set(values)

    if None in values:
        values.remove(None)
        queries.append(column.is_(None))
    for value in values:
        like_filter = create_like_filter(column, value)
        if like_filter is not None:
            queries.append(like_filter)

    return filter_query_with_queries(queries, query)


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
    def __init__(self, query, page=1, limit_per_page=20):
        super(Pagination, self).__init__()

        self.limit_per_page = maybe_integer(limit_per_page)
        if not self.limit_per_page or self.limit_per_page < 1:
            self.limit_per_page = 20
        elif self.limit_per_page > 5000:
            self.limit_per_page = 5000

        if query is None:
            self.number_of_results = 0
            self.last_page = 1
            self.page = 1
        else:
            # See https://bitbucket.org/zzzeek/sqlalchemy/issue/3320
            entities = set(d['expr'] for d in query.column_descriptions if d.get('expr') is not None)
            self.number_of_results = (
                query
                .with_entities(func.count(1), *entities)
                .order_by(None)
                .first()[0])
            self.last_page = int(ceil(self.number_of_results / float(self.limit_per_page))) or 1

            self.page = maybe_integer(page)
            if not self.page or self.page < 1:
                self.page = 1
            elif self.page > self.last_page:
                self.page = self.last_page

            end_slice = self.page * self.limit_per_page
            start_slice = end_slice - self.limit_per_page
            self.extend(query.slice(start_slice, end_slice).all())

        self.number_of_page_results = len(self)


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
        super(TemporaryColumnsLabel, self).__init__()
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
    else:
        element = getattr(value, '_element', None)
        if element is not None:
            # Label column
            table = getattr(element, 'table', None)
            if table is not None:
                tables.add(table)
            else:
                tables.update(get_object_tables(element))
        else:
            clauses = getattr(value, 'clauses', None)
            if clauses is not None:
                # Function
                for clause in value.clauses:
                    tables.update(get_object_tables(clause))
            elif hasattr(value, '_orig'):
                for orig in value._orig:
                    tables.update(get_object_tables(orig))
    return tables


class Columns(list):
    def __init__(self, *args, **kwargs):
        super(Columns, self).__init__(*args, **kwargs)
        self.tables = TablesSet()


def active_filter(tables):
    if not is_nonstr_iter(tables):
        tables = [tables]

    and_queries = []
    for table in tables:
        and_queries.append(or_(table.start_date <= func.now(), table.start_date.is_(None)))
        and_queries.append(or_(table.end_date > func.now(), table.end_date.is_(None)))
    return and_(*and_queries)


def inactive_filter(tables):
    return not_(active_filter(tables))


def date_in_period_filter(table, start_date, end_date):
    return or_(
        and_(table.start_date.is_(None), table.end_date.is_(None)),
        not_(or_(table.end_date < start_date, table.start_date > end_date)))


def get_active_column(tables, active=True):
    if active is None:
        return active_filter(tables).label('active')
    elif active:
        return true().label('active')
    else:
        return false().label('active')


def get_active_filter(tables, active=True):
    if active:
        return active_filter(tables)
    else:
        return inactive_filter(tables)


def query_filter_by(query, column, values):
    filter_query = create_filter_by(column, values)
    if filter_query is not None:
        return query.filter(filter_query)
    else:
        return query


def create_filter_by(columns, values):
    if not is_nonstr_iter(columns):
        columns = [columns]

    if isinstance(values, FilterBy):
        filter_type = values.filter_type.lower()
        if filter_type == 'like':
            queries = [like_maybe_with_none(c, values.value) for c in columns]
            return filter_query_with_queries(queries)

        elif filter_type == '>':
            queries = [c > values.value for c in columns]
            return filter_query_with_queries(queries)

        elif filter_type == '>=':
            queries = [c >= values.value for c in columns]
            return filter_query_with_queries(queries)

        elif filter_type == '<':
            queries = [c < values.value for c in columns]
            return filter_query_with_queries(queries)

        elif filter_type == '<=':
            queries = [c <= values.value for c in columns]
            return filter_query_with_queries(queries)

        elif filter_type in ('=', '=='):
            queries = [c == values.value for c in columns]
            return filter_query_with_queries(queries)

        elif filter_type == 'or':
            or_queries = []
            for value in values.value:
                query = create_filter_by(columns, value)
                if query is not None:
                    or_queries.append(query)

            if len(or_queries) == 1:
                return or_queries[0]
            elif or_queries:
                return or_(*or_queries)

        elif filter_type == 'and':
            and_queries = []
            for value in values.value:
                query = create_filter_by(columns, value)
                if query is not None:
                    and_queries.append(query)

            if len(and_queries) == 1:
                return and_queries[0]
            elif and_queries:
                return and_(*and_queries)

        else:
            raise Error('filter_type', u'Invalid filter type %s' % values.filter_type)

    elif not is_nonstr_iter(values):
        queries = [c == values for c in columns]
        return filter_query_with_queries(queries)

    else:
        or_queries = []
        other_values = set()
        for value in values:
            if isinstance(value, FilterBy) or is_nonstr_iter(value):
                query = create_filter_by(columns, value)
                if query is not None:
                    or_queries.append(query)
            else:
                other_values.add(value)

        if other_values:
            or_queries.extend(maybe_with_none(c, other_values) for c in columns)
        if len(or_queries) == 1:
            return or_queries[0]
        elif or_queries:
            return or_(*or_queries)


def new_lightweight_named_tuple(response, *new_fields):
    return lightweight_named_tuple('result', response._real_fields + tuple(new_fields))
