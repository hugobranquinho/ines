# -*- coding: utf-8 -*-

from collections import defaultdict
from functools import wraps
from time import time

from pyramid.decorator import reify
from sqlalchemy import and_, Column, func, not_, or_
from sqlalchemy.ext.declarative.api import DeclarativeMeta
from sqlalchemy.sql.elements import Label
from sqlalchemy.sql.expression import false, true
from sqlalchemy.sql.functions import FunctionElement
from sqlalchemy.sql.schema import Table
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.util._collections import lightweight_named_tuple

from ines import MARKER
from ines.api.database import SQL_DBS
from ines.convert import maybe_list
from ines.utils import get_from_breadcrumbs, PaginationClass


ORM_TABLES_CACHE = {}
TABLES_BACKREFS_CACHE = defaultdict(set)


def set_timer(log_in_seconds=None):
    def decorator(wrapped):
        @wraps(wrapped)
        def wrapper(self, *args, **kwargs):
            max_time = float(
                log_in_seconds
                or self.registry.settings.get('slow_queries_log_in_seconds')
                or 1)

            start_time = time()
            response = wrapped(self, *args, **kwargs)
            spent_time = float(time() - start_time)

            if spent_time >= max_time:
                self.logging.log_warning(
                    'slow_action',
                    'Slow action on %s.%s (take %s)' % (self.__api_name__, wrapped.__name__, spent_time))

            return response

        return wrapper
    return decorator


def get_column_table_relations(column):
    if isinstance(column, (Table, DeclarativeMeta)):
        return {get_schema_table(column)}
    if isinstance(column, (Column, InstrumentedAttribute)):
        return {column.table}
    elif isinstance(column, FunctionElement):
        return {
            table
            for clause in column.clauses
            for table in get_column_table_relations(clause)}
    elif isinstance(column, Label):
        return get_column_table_relations(column.element)

    raise ValueError('Cant find table(s) for column %s' % column)


def get_tables_on_registry(decl_class_registry):
    references = {}
    for name, table in decl_class_registry.items():
        if name != '_sa_module_registry':
            references[table.__tablename__] = table
            table_alias = getattr(table, '__table_alias__', None)
            if table_alias:
                references.update((k, table) for k in table_alias)
    return references


def get_orm_tables(database_name=None):
    references = {}
    for name, database in SQL_DBS.items():
        if not database_name or name == database_name:
            for base in database.get('bases') or []:
                references.update(get_tables_on_registry(base._decl_class_registry))
    return references


def maybe_table_schema(value):
    if isinstance(value, Table):
        return value
    elif hasattr(value, '__table__'):
        return value.__table__


def get_schema_table(table):
    if isinstance(table, Table):
        return table
    else:
        return table.__table__


def get_orm_table(table):
    if hasattr(table, '__table__'):
        return table

    cached_table = ORM_TABLES_CACHE.get(table.name)
    if cached_table is None:
        ORM_TABLES_CACHE.update(get_orm_tables())
        cached_table = ORM_TABLES_CACHE[table.name]

    return cached_table


def get_table_backrefs(table):
    table = get_schema_table(table)
    cached_backrefs = TABLES_BACKREFS_CACHE.get(table)
    if cached_backrefs is not None:
        return cached_backrefs

    for orm_table in get_orm_tables().values():
        for foreign_key in orm_table.__table__.foreign_keys:
            TABLES_BACKREFS_CACHE[foreign_key.column.table].add(foreign_key)

    return TABLES_BACKREFS_CACHE[table]


def get_table_column(table, column_name, default=MARKER):
    schema_table = get_schema_table(table)

    column = schema_table.columns.get(column_name)
    if column is None:
        orm_table = get_orm_table(table)
        column = getattr(orm_table, column_name, None)

    if column is not None:
        return column
    elif default is not MARKER:
        return default
    else:
        raise KeyError('Missing column %s on table %s' % (column_name, schema_table.name))


def get_table_columns(table):
    table = get_schema_table(table)
    return table.columns.values()


def build_sql_relations(table, relations):
    filters = []
    outer_joins = []
    table = get_schema_table(table)

    for foreign_key in table.foreign_keys:
        foreign_table = foreign_key.column.table
        if foreign_table in relations:
            if foreign_key.parent.nullable:
                outer_joins.append((foreign_table, foreign_key.parent == foreign_key.column))
            else:
                filters.append(foreign_key.parent == foreign_key.column)

            relations.remove(foreign_table)

            foreign_filters, foreign_outerjoin = build_sql_relations(foreign_table, relations)
            filters.extend(foreign_filters)

            if foreign_outerjoin:
                if not outer_joins:
                    outer_joins.append(table)
                outer_joins.extend(foreign_outerjoin)

    return filters, outer_joins


def get_active_column(table, active):
    if active is None:
        active_tables, active_filters = get_recursively_active_filters(table)
        if active_filters:
            active_column = and_(*active_filters)
        else:
            active_column = true()
    elif active:
        active_column = true()
        active_tables = set()
    else:
        active_column = false()
        active_tables = set()

    return active_column.label('active'), active_tables


def get_recursively_tables(table):
    tables = set()
    table = get_schema_table(table)
    for foreign_key in table.foreign_keys:
        foreign_table = foreign_key.column.table
        if foreign_table != table:
            tables.add(foreign_table)
            tables.update(get_recursively_tables(foreign_table))
    return tables


def get_recursively_active_filters(table):
    active_tables = set()
    active_filters = []

    table = get_schema_table(table)
    table_active_filters = get_active_filters(table)
    if table_active_filters:
        active_tables.add(table)
        active_filters.extend(table_active_filters)

    for foreign_key in table.foreign_keys:
        foreign_table = foreign_key.column.table
        if foreign_table != table:
            foreign_active_tables, foreign_active_filters = get_recursively_active_filters(foreign_table)
            active_tables.update(foreign_active_tables)
            active_filters.extend(foreign_active_filters)

    return active_tables, active_filters


def get_active_filters(table):
    and_queries = []
    table = get_schema_table(table)

    active_column = table.columns.get('active')
    if active_column is not None:
        and_queries.append(active_column)
    else:
        orm_table = get_orm_table(table)
        if hasattr(orm_table, 'active'):
            and_queries.append(orm_table.active)

    start_date_column = table.columns.get('start_date')
    if start_date_column is not None:
        if start_date_column.nullable:
            and_queries.append(or_(start_date_column <= func.now(), start_date_column.is_(None)))
        else:
            and_queries.append(start_date_column <= func.now())

    end_date_column = table.columns.get('end_date')
    if end_date_column is not None:
        if end_date_column.nullable:
            and_queries.append(or_(end_date_column >= func.now(), end_date_column.is_(None)))
        else:
            and_queries.append(end_date_column >= func.now())

    return and_queries


def get_active_filter(table):
    active_tables, active_filters = get_recursively_active_filters(table)
    if active_filters:
        return and_(*active_filters)
    else:
        return true()


def get_inactive_filter(tables):
    return not_(get_active_filter(tables))


def table_entry_as_dict(entry):
    return {key: getattr(entry, key) for key in get_schema_table(entry).columns.keys()}


def replace_response_columns(indexes, references, response):
    named_tuple = lightweight_named_tuple('result', list(response[0]._real_fields))

    for response_index, item in enumerate(response):
        item_as_list = list(item)
        for key, attribute_indexes in indexes.items():
            key_references = references[key]
            for i in attribute_indexes:
                item_as_list[i] = key_references[item[i]]

        response[response_index] = named_tuple(item_as_list)


def set_columns_as_named_tuple(columns, fields):
    named_tuple = lightweight_named_tuple('result', fields)
    return named_tuple(columns)


def get_api_first_method(api, table):
    return get_from_breadcrumbs(api.applications, get_orm_table(table).__first_method__.split('.'))


def get_api_all_method(api, table):
    return get_from_breadcrumbs(api.applications, get_orm_table(table).__all_method__.split('.'))















# TODO delete?
class TablesSet(set):
    def have(self, table):
        return table in self

    def __contains__(self, table):
        table = getattr(table, '__table__', None)
        if table is not None:
            return set.__contains__(self, table)
        else:
            return False


# TODO delete?
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


# TODO delete?
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
                columns.tables.update(get_column_table_relations(column))

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


# TODO delete?
class Columns(list):
    def __init__(self, *args, **kwargs):
        super(Columns, self).__init__(*args, **kwargs)
        self.tables = TablesSet()


class SQLPagination(PaginationClass):
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
            super(SQLPagination, self).__init__(page=1, limit_per_page=limit_per_page)
        else:
            super(SQLPagination, self).__init__(page=page, limit_per_page=limit_per_page)

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




# TODO
def __default_all_method(table, columns=None, replace=None, use_default=False):
    def decorator(wrapped):
        lookup_name = (
            'all_' in wrapped.__name__
            and '_%s' % wrapped.__name__.split('all_', 1)[1]
            or '')

        @wraps(wrapped)
        def wrapper(
                cls,
                only_one=False,
                group_by=False,
                **filters):

            if not columns:
                column_indexes = attributes = None
            else:
                column_indexes = defaultdict(dict)
                attributes = filters.pop('attributes', None)
                if attributes:
                    attributes = maybe_list(attributes)
                    for i, attribute in enumerate(list(attributes)):
                        column = columns.get(attribute)
                        if column is not None:
                            attributes.pop(i - len(column_indexes))
                            column_indexes[attribute][i] = column.label(attribute)

            if use_default:
                order_by = filters.pop('order_by', None)
                attributes = filters.pop('attributes', attributes)

                sa_columns = getattr(cls, 'lookup%s_columns' % lookup_name)(attributes, active=active)
                sa_filters = getattr(cls, 'lookup%s_filters' % lookup_name)(filters)
                sa_order_by = getattr(cls, 'lookup%s_order_by' % lookup_name)(order_by, active=active)

                if column_indexes:
                    for index_columns in column_indexes.values():
                        for i, column in index_columns.items():
                            sa_columns.insert(i, column)

                # Start query
                query = cls.api.session.query(*sa_columns)

                # Add more columns, filters and order by
                query = wrapped(
                    cls,
                    query,
                    sa_columns,
                    sa_filters,
                    sa_order_by,
                    in_lookups=lambda t: cls.table_in_lookups(t, sa_columns, sa_filters, sa_order_by))

                if sa_filters:
                    query = query.filter(*sa_filters)
                if sa_order_by:
                    query = query.order_by(*sa_order_by)
            else:
                if attributes:
                    filters['attributes'] = attributes
                if column_indexes:
                    filters['column_indexes'] = column_indexes

                query = wrapped(cls, active=active, **filters)

            if group_by:
                query = query.group_by(*maybe_list(group_by))

            middle_time = time()
            if columns and replace and response and column_indexes:
                if only_one:
                    response = [response]

                references = defaultdict(dict)
                for attribute, replace_method in replace.items():
                    if attribute in column_indexes:
                        ids = set(getattr(r, attribute) for r in response)
                        references[attribute] = replace_method(cls, ids)

                fill_response_with_indexs(column_indexes, references, response)

                if only_one:
                    response = response[0]

            return response

        return wrapper
    return decorator
