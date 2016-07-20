# -*- coding: utf-8 -*-

from colander import drop
from collections import defaultdict

from pyramid.compat import is_nonstr_iter
from pyramid.decorator import reify
from pyramid.settings import asbool
from six import u
from six import _import_module
from sqlalchemy import and_
from sqlalchemy import cast
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import Enum
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import not_
from sqlalchemy import Numeric
from sqlalchemy import or_
from sqlalchemy import String
from sqlalchemy import TEXT
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.event import listen as sqlalchemy_listen
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.orm.descriptor_props import CompositeProperty
from sqlalchemy.orm.util import AliasedClass
from sqlalchemy.pool import NullPool
from sqlalchemy.schema import DDL
from sqlalchemy.sql.expression import false
from sqlalchemy.sql.expression import true
from sqlalchemy.sql.selectable import Alias
from sqlalchemy.util._collections import lightweight_named_tuple

from ines import NOW
from ines.api import BaseSessionManager
from ines.api import BaseSession
from ines.cleaner import clean_unicode
from ines.cleaner import LOWER_MAPPING
from ines.convert import maybe_list
from ines.convert import maybe_set
from ines.convert import maybe_unicode
from ines.convert import unicode_join
from ines.exceptions import Error
from ines.middlewares.repozetm import RepozeTMMiddleware
from ines.path import get_object_on_path
from ines.views.fields import FilterBy
from ines.views.fields import OrderBy
from ines.utils import compare_values
from ines.utils import PaginationClass


SQL_ENGINES = {}
SQL_DBS = defaultdict(dict)
SQLALCHEMY_NOW_TYPE = type(func.now())

POSTGRESQL_LOWER_AND_CLEAR = """
    CREATE OR REPLACE FUNCTION lower_and_clear(VARCHAR)
        RETURNS VARCHAR AS $$ SELECT translate(lower($1), '%s', '%s') $$
        LANGUAGE SQL""" % LOWER_MAPPING


def postgresql_non_ascii_and_lower(column, as_text=True):
    if not column_is_postgresql(column):
        return column

    elif hasattr(column, 'property'):
        columns = column.property.columns
        if len(columns) > 1:
            column = func.concat(*columns)
        else:
            column = column.property.columns[0]

    if isinstance(column.type, Enum):
        return column
    elif isinstance(column.type, String):
        return func.lower_and_clear(column)
    elif isinstance(column.type, Numeric):
        return column
    elif as_text:
        return func.text(column)
    else:
        return column


def column_is_postgresql(column):
    bind = getattr(column, 'bind', None)
    if bind is not None:
        return bind.name == 'postgresql'

    table = getattr(column, 'table', None)
    if table is not None:
        return column_is_postgresql(table)

    clause = getattr(column, 'clause', None)
    if clause is not None:
        return column_is_postgresql(clause)

    clauses = getattr(column, 'clauses', None)
    if clauses is not None:
        return column_is_postgresql(list(clauses)[0])

    parent = getattr(column, 'parent', None)
    if parent is not None:
        return column_is_postgresql(parent.mapped_table.metadata)

    from_objects = getattr(column, '_from_objects', None)
    if from_objects:
        return column_is_postgresql(from_objects[0])

    raise ValueError('Missing bind on %s. Check `column_is_postgresql`' % column)


class BaseSQLSessionManager(BaseSessionManager):
    __api_name__ = 'database'
    __middlewares__ = [RepozeTMMiddleware]

    @reify
    def __database_name__(self):
        return self.config.application_name

    def __init__(self, *args, **kwargs):
        super(BaseSQLSessionManager, self).__init__(*args, **kwargs)

        self.transaction = _import_module('transaction')

        session_extension = self.settings.get('session_extension')
        if session_extension is not None:
            session_extension = get_object_on_path(session_extension)

        self.db_session = initialize_sql(
            self.__database_name__,
            self.settings['sql_path'],
            encoding=self.settings.get('encoding', 'utf8'),
            mysql_engine=self.settings.get('mysql_engine') or 'InnoDB',
            session_extension=session_extension,
            debug=asbool(self.settings.get('debug', False)))


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
        for key, column in obj.__table__.c.items():
            value = getattr(obj, key, None)
            if value is None and column.default:
                value = column.default.execute()

            if value is not None:
                values[key] = value

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
        for key, column in obj.__table__.c.items():
            if key not in values and column.onupdate:
                values[key] = column.onupdate.execute()

        return (
            obj.__table__
            .update(query)
            .values(values)
            .execute(autocommit=True))

    def table_instance_as_dict(self, instance):
        return dict((k, getattr(instance, k)) for k in instance.__table__.c.keys())

    def set_filter_by_on_query(self, query, column, values):
        return query.filter(create_filter_by(column, values))

    def set_active_filter_on_query(self, query, tables, active=None, active_query=None):
        if active:
            query_filter = active_filter(tables)
        else:
            query_filter = inactive_filter(tables)

        if active_query is not None:
            query_filter = and_(query_filter, active_query)

        return query.filter(query_filter)

    def get_active_attribute(self, tables, active=None, active_query=None):
        if active is None:
            attribute = active_filter(tables)
            if active_query is not None:
                attribute = and_(active_query, attribute)
        elif active:
            attribute = true()
        else:
            attribute = false()

        return attribute.label('active')

    def _lookup_columns(self, table, attributes, active=None, active_tables=None, external=None, active_query=None):
        relate_with = set()
        if not attributes:
            columns = list(table.__table__.c.values())

            if active_tables:
                if active is None:
                    relate_with.update(t.__tablename__ for t in maybe_list(active_tables))

                column = self.get_active_attribute(active_tables, active=active, active_query=active_query)
                relate_with.update(t.name for t in get_object_tables(column))
                columns.append(column)
        else:
            external_names = []
            external_methods = {}
            external_attributes = defaultdict(lambda: defaultdict(list))
            if external:
                for external_table, external_method in external.items():
                    if isinstance(external_table, AliasedClass):
                        tablename = external_table.name.parent.name
                    else:
                        tablename = external_table.__tablename__

                    external_names.append((tablename, len(tablename)))
                    external_methods[tablename] = external_method
                external_names.sort(reverse=True)

            columns = []
            for attribute in maybe_list(attributes):
                column = getattr(table, attribute, None)
                if column is not None:
                    relate_with.update(t.name for t in get_object_tables(column))
                    columns.append(column)

                elif active_tables and attribute == 'active':
                    if active is None:
                        relate_with.update(t.__tablename__ for t in maybe_list(active_tables))

                    column = self.get_active_attribute(active_tables, active=active, active_query=active_query)
                    relate_with.update(t.name for t in get_object_tables(column))
                    columns.append(column)

                else:
                    for name, name_length in external_names:
                        if attribute[:name_length] == name:
                            # Index for posterior insert
                            external_attributes[name][attribute[name_length + 1:]].append(len(columns))
                            columns.append(attribute)
                            break
                    else:
                        raise AttributeError('Missing column attribute "%s" on "%s"' % (attribute, self.__api_name__))

            if external_attributes:
                for name, name_attributes in external_attributes.items():
                    external_columns = external_methods[name](name_attributes.keys(), active=active)
                    if external_columns:
                        relate_with.update(external_columns.relate_with)
                        relate_with.add(name)

                        for column in external_columns:
                            label_name = '%s_%s' % (name, column.key)
                            for column_idx in name_attributes[column.key]:
                                columns[column_idx] = column.label(label_name)

        return LookupAtributes(columns, relate_with)

    def _lookup_filters(self, table, filters, external=None, ignore_external_names=None):
        sa_filters = []
        relate_with = set()

        external_names = []
        external_methods = {}
        external_filters = defaultdict(dict)
        if external:
            for external_table, external_method in external.items():
                if isinstance(external_table, AliasedClass):
                    tablename = external_table.name.parent.name
                else:
                    tablename = external_table.__tablename__

                external_names.append((tablename, len(tablename)))
                external_methods[tablename] = external_method
            external_names.sort(reverse=True)

        for attribute, value in filters.items():
            if attribute == 'global_search':
                global_tables = [table]
                if external:
                    global_tables.extend(external.keys())

                global_filters = self.create_global_search_options(value, global_tables)
                if global_filters:
                    sa_filters.append(or_(*global_filters))
                    relate_with.update(
                        t.name
                        for f in global_filters
                        for t in get_object_tables(f))

                # Go to next attribute
                continue

            not_filter = attribute[:4] == 'not_'
            if not_filter:
                attribute = attribute[4:]

            is_like = is_ilike = is_none = False
            is_not_none = attribute[-12:] == '_is_not_none'
            if is_not_none:
                column = getattr(table, attribute[:-12], None)
            else:
                is_like = attribute[-8:] == '_is_like'
                if is_like:
                    column = getattr(table, attribute[:-8], None)
                else:
                    is_ilike = attribute[-9:] == '_is_ilike'
                    if is_ilike:
                        column = getattr(table, attribute[:-9], None)
                    else:
                        is_none = attribute[-8:] == '_is_none'
                        if is_none:
                            column = getattr(table, attribute[:-8], None)
                        else:
                            column = getattr(table, attribute, None)

            if column is not None:
                if is_not_none:
                    sa_filter = column.isnot(None)
                elif is_like:
                    sa_filter = like_maybe_with_none(column, value)
                elif is_ilike:
                    sa_filter = ilike_maybe_with_none(column, value)
                elif is_none:
                    sa_filter = column.is_(None)
                else:
                    sa_filter = create_filter_by(column, value)

                if sa_filter is not None:
                    relate_with.update(t.name for t in get_object_tables(column))

                    if not_filter:
                        sa_filters.append(not_(sa_filter))
                    else:
                        sa_filters.append(sa_filter)
            else:
                for name, name_length in external_names:
                    if attribute[:name_length] == name:
                        external_attribute_name = attribute[name_length + 1:]  # +1 = underscore
                        if not_filter:
                            external_attribute_name = 'not_%s' % external_attribute_name

                        external_filters[name][external_attribute_name] = value
                        break
                else:
                    raise AttributeError('Missing filter attribute "%s" on "%s"' % (attribute, self.__api_name__))

        if external_filters:
            for name, name_filters in external_filters.items():
                external_sa_filters = external_methods[name](name_filters)
                if external_sa_filters:
                    relate_with.update(external_sa_filters.relate_with)
                    relate_with.add(name)
                    sa_filters.extend(external_sa_filters)

        return LookupAtributes(sa_filters, relate_with)

    def _lookup_order_by(self, table, attributes, active=None, active_tables=None, external=None, active_query=None):
        order_by = []
        relate_with = set()

        external_names = []
        external_methods = {}
        external_order_by = defaultdict(list)
        if external:
            for external_table, external_method in external.items():
                if isinstance(external_table, AliasedClass):
                    tablename = external_table.name.parent.name
                else:
                    tablename = external_table.__tablename__

                external_names.append((tablename, len(tablename)))
                external_methods[tablename] = external_method
            external_names.sort(reverse=True)

        for attribute in maybe_list(attributes):
            as_desc = False
            if isinstance(attribute, OrderBy):
                as_desc = attribute.descendant
                attribute = attribute.column_name

            elif attribute.lower().endswith(' desc'):
                attribute = attribute[:-5]
                as_desc = True

            elif attribute.lower().endswith(' asc'):
                attribute = attribute[:-4]

            column = getattr(table, attribute, None)
            if column is not None:
                relate_with.update(t.name for t in get_object_tables(column))
                if as_desc:
                    order_by.append(postgresql_non_ascii_and_lower(column, as_text=False).desc())
                else:
                    order_by.append(postgresql_non_ascii_and_lower(column, as_text=False))

            elif active_tables and attribute == 'active':
                if active is None:
                    relate_with.update(t.__tablename__ for t in maybe_list(active_tables))

                column = self.get_active_attribute(active_tables, active=active, active_query=active_query)
                relate_with.update(t.name for t in get_object_tables(column))
                if as_desc:
                    order_by.append(column.desc())
                else:
                    order_by.append(column)

            else:
                for name, name_length in external_names:
                    if attribute[:name_length] == name:
                        external_order_by[name].append((
                            attribute[name_length + 1:],  # +1 = underscore
                            len(order_by),  # Index for posterior insert
                            attribute,
                            as_desc))
                        order_by.append(attribute)
                        break
                else:
                    raise AttributeError('Missing order by attribute "%s" on "%s"' % (attribute, self.__api_name__))

        if external_order_by:
            for name, name_attributes in external_order_by.items():
                external_keys = [k[0] for k in name_attributes]
                external_columns = external_methods[name](external_keys, active=active)
                if external_columns:
                    relate_with.update(external_columns.relate_with)
                    relate_with.add(name)

                    for i, column in enumerate(external_columns):
                        attribute_name, column_idx, label_name, as_desc = name_attributes[i]
                        column = column.label(label_name)

                        if as_desc:
                            order_by[column_idx] = column.desc()
                        else:
                            order_by[column_idx] = column

        return LookupAtributes(order_by, relate_with)

    def table_in_lookups(self, table, *lookups):
        if isinstance(table, AliasedClass):
            tablename = table._aliased_insp.name
        elif isinstance(table, Alias):
            tablename = table.name
        else:
            tablename = table.__tablename__

        for lookup in lookups:
            if tablename in lookup.relate_with:
                return True
        return False

    def create_global_search_options(self, search, tables):
        response = []
        search = clean_unicode(search)
        search_list = search.split()

        for table in maybe_list(tables):
            ignore_columns = getattr(table, '__ignore_on_global_search__', None)
            ignore_ids = getattr(table, '__ignore_ids_on_global_search__', True)

            for column_name in table._sa_class_manager.local_attrs.keys():
                if ((ignore_ids and (column_name == 'id' or column_name.endswith('_id')))
                        or (ignore_columns and column_name in ignore_columns)):
                    continue

                column = getattr(table, column_name)
                if hasattr(column, 'type'):
                    if isinstance(column.type, Enum):
                        for value in column.type.enums:
                            value_to_search = value
                            for s in search_list:
                                try:
                                    idx = value_to_search.index(s)
                                except ValueError:
                                    break
                                else:
                                    value_to_search = value_to_search[idx + len(s):]
                            else:
                                response.append(column == value)

                    elif isinstance(column.type, String):
                        value = create_like_filter(column, search)
                        if value is not None:
                            response.append(value)

                    elif isinstance(column.type, (Numeric, Integer, Date, DateTime)):
                        value = create_like_filter(cast(column, String), search)
                        if value is not None:
                            response.append(value)

                else:
                    clauses = getattr(column, 'clauses', None)
                    if clauses is not None:
                        value = create_like_filter(func.concat(*clauses), search)
                        if value is not None:
                            response.append(value)
                    else:
                        value = create_like_filter(cast(column, String), search)
                        if value is not None:
                            response.append(value)

        return response

    def fill_response_with_indexs(self, indexs, references, response):
        named_tuple = lightweight_named_tuple('result', list(response[0]._real_fields))
        for idx, value in enumerate(response):
            new_value = list(value)
            for key, index_list in indexs.items():
                for i in index_list:
                    new_value[i] = references[key][getattr(value, key)]
            response[idx] = named_tuple(new_value)


class LookupAtributes(list):
    def __init__(self, columns, relate_with=None):
        super(LookupAtributes, self).__init__(columns)
        self.relate_with = relate_with or set()


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

    # Add some postgresql functions
    if sql_path.lower().startswith('postgresql') and metadata:
        sqlalchemy_listen(metadata, 'before_create', DDL(POSTGRESQL_LOWER_AND_CLEAR))

    engine_pattern = '%s-%s' % (sql_path, encoding)
    if engine_pattern in SQL_ENGINES:
        engine = SQL_ENGINES[engine_pattern]
    else:
        SQL_ENGINES[engine_pattern] = engine = create_engine(
            sql_path,
            echo=debug,
            poolclass=NullPool,
            encoding=encoding)
    SQL_DBS[application_name]['engine'] = engine

    if session_extension:
        if callable(session_extension):
            session_extension = session_extension()
        session_maker = sessionmaker(extension=session_extension)
    else:
        session_maker = sessionmaker()

    session = scoped_session(session_maker)
    session.configure(bind=engine)
    SQL_DBS[application_name]['session'] = session

    indexed_columns = SQL_DBS[application_name]['indexed_columns'] = defaultdict(set)
    if metadata is not None:
        metadata.bind = engine
        metadata.create_all(engine)

        # Force indexes creation
        for table in metadata.sorted_tables:
            if table.indexes:
                for index in table.indexes:
                    for column in getattr(index.columns, '_all_columns'):
                        indexed_columns[table.name].add(column.key)

                    try:
                        index.create()
                    except (ProgrammingError, OperationalError):
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


def ilike_maybe_with_none(column, values, query=None):
    queries = []
    values = maybe_set(values)

    if None in values:
        values.remove(None)
        queries.append(column.is_(None))
    for value in values:
        like_filter = create_ilike_filter(column, value)
        if like_filter is not None:
            queries.append(like_filter)

    return filter_query_with_queries(queries, query)


def create_like_filter(column, value):
    value = maybe_unicode(value)
    if value:
        words = value.split()
        if words:
            like_str = u('%%%s%%') % '%'.join(clean_unicode(w) for w in words)
            column = postgresql_non_ascii_and_lower(column)
            return column.like(like_str.lower())


def create_ilike_filter(column, value):
    value = maybe_unicode(value)
    if value:
        words = value.split()
        if words:
            like_str = u('%%%s%%') % '%'.join(clean_unicode(w) for w in words)
            column = postgresql_non_ascii_and_lower(column)
            return column.ilike(like_str.lower())


def create_rlike_filter(column, value):
    value = maybe_unicode(value)
    if value:
        words = value.split()
        if words:
            rlike_str = u('(%s)') % unicode_join('|', words)
            column = postgresql_non_ascii_and_lower(column)
            return column.op('rlike')(rlike_str.lower())


class Pagination(PaginationClass):
    def __init__(
            self,
            query,
            page=1,
            limit_per_page=20,
            count_column=None,
            clear_group_by=False,
            ignore_count=False,
            extend_entitites=False):

        if query is None:
            super(Pagination, self).__init__(page=1, limit_per_page=limit_per_page)
        else:
            super(Pagination, self).__init__(page=page, limit_per_page=limit_per_page)

            if self.limit_per_page != 'all':
                if not ignore_count:
                    entities = set()
                    if not count_column or extend_entitites:
                        # See https://bitbucket.org/zzzeek/sqlalchemy/issue/3320
                        entities.update(d['expr'] for d in query.column_descriptions if d.get('expr') is not None)

                    count_query = query
                    if clear_group_by:
                        count_query._group_by = []

                    self.set_number_of_results(
                        sum(r[0] for r in (
                            count_query
                            .with_entities(func.count(count_column or 1), *entities)
                            .order_by(None)
                            .all())))

                end_slice = self.page * self.limit_per_page
                start_slice = end_slice - self.limit_per_page
                query = query.slice(start_slice, end_slice)

            self.extend(query.all())

            if self.limit_per_page == 'all':
                self.set_number_of_results(len(self))


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


class Options(dict):
    def __missing__(self, key):
        self[key] = {}
        return self[key]

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
    table = getattr(value, 'table', None)
    if table is not None:
        return set([table])

    element = getattr(value, '_element', None)
    if element is not None:
        # Label column
        table = getattr(element, 'table', None)
        if table is not None:
            return set([table])
        else:
            return get_object_tables(element)

    clauses = getattr(value, 'clauses', None)
    if clauses is not None:
        # Function
        tables = set()
        for clause in value.clauses:
            tables.update(get_object_tables(clause))
        return tables

    clause = getattr(value, 'clause', None)
    if clause is not None:
        return get_object_tables(clause)

    orig = getattr(value, '_orig', None)
    if orig is not None:
        tables = set()
        for o in orig:
            tables.update(get_object_tables(o))
        return tables

    get_children = getattr(value, 'get_children', None)
    if get_children is not None:
        tables = set()
        for child in get_children():
            tables.update(get_object_tables(child))
        return tables

    original_property = getattr(value, 'original_property', None)
    if original_property:
        # composite
        tables = set()
        for attr in original_property.attrs:
            tables.update(get_object_tables(attr))
        return tables

    raise ValueError('Missing tables for %s. Check `get_object_tables` method' % value)


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


def create_filter_by(column, values):
    if hasattr(column, 'property') and isinstance(column.property, CompositeProperty):
        column = func.concat(*column.property.columns)

    if isinstance(values, FilterBy):
        filter_type = values.filter_type.lower()

        if filter_type == 'or':
            or_queries = []
            for value in values.value:
                query = create_filter_by(column, value)
                if query is not None:
                    or_queries.append(query)

            if len(or_queries) == 1:
                return or_queries[0]
            elif or_queries:
                return or_(*or_queries)

        elif filter_type == 'and':
            and_queries = []
            for value in values.value:
                query = create_filter_by(column, value)
                if query is not None:
                    and_queries.append(query)

            if len(and_queries) == 1:
                return and_queries[0]
            elif and_queries:
                return and_(*and_queries)

        elif filter_type in ('like', 'contém'):
            return like_maybe_with_none(column, values.value)

        elif filter_type == '>':
            return column > values.value

        elif filter_type == '>=':
            return column >= values.value

        elif filter_type == '<':
            return column < values.value

        elif filter_type == '<=':
            return column <= values.value

        elif filter_type in ('=', '=='):
            return column == values.value

        elif filter_type in ('!=', '≠'):
            return column != values.value

        else:
            raise Error('filter_type', u('Invalid filter type %s') % values.filter_type)

    elif values is drop:
        return None

    elif not is_nonstr_iter(values):
        return column == values

    else:
        or_queries = []
        noniter_values = set()
        for value in values:
            if isinstance(value, FilterBy) or is_nonstr_iter(value):
                query = create_filter_by(column, value)
                if query is not None:
                    or_queries.append(query)
            elif value is not drop:
                noniter_values.add(value)
        if noniter_values:
            or_queries.append(maybe_with_none(column, noniter_values))

        if len(or_queries) == 1:
            return or_queries[0]
        elif or_queries:
            return or_(*or_queries)


def do_filter_by(response, key, values, clear_response=True):
    keep_index = set()
    if isinstance(values, FilterBy):
        filter_type = values.filter_type.lower()

        if filter_type == 'or':
            for value in values.value:
                keep_index.update(do_filter_by(response, key, value, clear_response=False))

        elif filter_type == 'and':
            valid_response = list(response)
            for value in values.value:
                valid_index = do_filter_by(valid_response, key, value, clear_response=False)
                if not valid_index:
                    valid_response.clear()
                else:
                    for i in reversed(list(enumerate(valid_response))):
                        if i not in valid_index:
                            valid_response.pop(i)
                if not valid_response:
                    break

            keep_index.update(i for i, r in enumerate(valid_response))

        elif filter_type in ('like', 'contém'):
            values = [clean_unicode(v).lower() for v in values.value.split()]
            for i, r in enumerate(response):
                r_value = r.get(key)
                if not r_value:
                    continue

                r_value = clean_unicode(r_value).lower()
                for value in values:
                    try:
                        ridx = r_value.index(value)
                    except ValueError:
                        break
                    else:
                        r_value = r_value[ridx + len(value):]
                else:
                    keep_index.add(i)

        elif filter_type == '>':
            keep_index.update(
                i for i, r in enumerate(response)
                if compare_values(r.get(key), values.value, '__gt__'))

        elif filter_type == '>=':
            keep_index.update(
                i for i, r in enumerate(response)
                if compare_values(r.get(key), values.value, '__ge__'))

        elif filter_type == '<':
            keep_index.update(
                i for i, r in enumerate(response)
                if compare_values(r.get(key), values.value, '__lt__'))

        elif filter_type == '<=':
            keep_index.update(
                i for i, r in enumerate(response)
                if compare_values(r.get(key), values.value, '__le__'))

        elif filter_type in ('=', '=='):
            keep_index.update(
                i for i, r in enumerate(response)
                if compare_values(r.get(key), values.value, '__eq__'))

        elif filter_type in ('!=', '≠'):
            keep_index.update(
                i for i, r in enumerate(response)
                if compare_values(r.get(key), values.value, '__ne__'))

        else:
            raise Error('filter_type', u('Invalid filter type %s') % values.filter_type)

    elif values is drop:
        pass

    elif not is_nonstr_iter(values):
        keep_index.update(
            i for i, r in enumerate(response)
            if r.get(key) == values)

    else:
        for value in values:
            if isinstance(value, FilterBy) or is_nonstr_iter(value):
                keep_index.update(do_filter_by(response, key, value, clear_response=False))
            elif value is not drop:
                keep_index.update(
                    i for i, r in enumerate(response)
                    if compare_values(r.get(key), value, '__eq__'))

    if not clear_response:
        return keep_index
    elif not keep_index:
        response.clear()
    else:
        for i, r in reversed(list(enumerate(response))):
            if i not in keep_index:
                response.pop(i)


def new_lightweight_named_tuple(response, *new_fields):
    return lightweight_named_tuple('result', response._real_fields + tuple(new_fields))


def get_orm_tables(database_name=None):
    references = {}
    for name, database in SQL_DBS.items():
        if not database_name or name == database_name:
            for base in database.get('bases') or []:
                references.update(get_tables_on_registry(base._decl_class_registry))
    return references


def get_tables_on_registry(decl_class_registry):
    references = {}
    for name, table in decl_class_registry.items():
        if name != '_sa_module_registry':
            references[table.__tablename__] = table
            table_alias = getattr(table, '__table_alias__', None)
            if table_alias:
                references.update((k, table) for k in table_alias)
    return references


def resolve_database_value(value):
    if isinstance(value, SQLALCHEMY_NOW_TYPE):
        return NOW()
    else:
        return value
