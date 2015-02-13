# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

import datetime
from math import ceil

from pyramid.compat import is_nonstr_iter
from pyramid.decorator import reify
from pyramid.settings import asbool
from sqlalchemy import and_
from sqlalchemy import Column
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import or_
from sqlalchemy import Unicode
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import aliased
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql.elements import ClauseElement

from ines.api import BaseSessionManager
from ines.api.database import BaseSQLSession
from ines.api.database.sql import initialize_sql
from ines.api.database.sql import get_object_tables
from ines.api.database.sql import get_sql_settings_from_config
from ines.api.database.sql import maybe_with_none
from ines.api.database.sql import Options
from ines.api.database.sql import SQL_DBS
from ines.api.database.sql import sql_declarative_base
from ines.convert import camelcase
from ines.convert import maybe_integer
from ines.exceptions import Error
from ines.middlewares.repozetm import RepozeTMMiddleware
from ines.views import DateTimeField
from ines.views import Field
from ines.views import HrefField
from ines.utils import make_uuid_hash
from ines.utils import MissingList


class CoreTypesMissing(dict):
    def __missing__(self, key):
        self[key] = {
            'table': None,
            'parent': None,
            'childs': set(),
            'branchs': set()}
        return self[key]


CORE_TYPES = CoreTypesMissing()
CORE_KEYS = set()
NOW_DATE = datetime.datetime.now
DeclarativeBase = sql_declarative_base('core')


class BaseCoreSessionManager(BaseSessionManager):
    __api_name__ = 'core'
    __middlewares__ = [RepozeTMMiddleware]

    def __init__(self, *args, **kwargs):
        super(BaseCoreSessionManager, self).__init__(*args, **kwargs)

        self.db_session = initialize_sql(
            'core',
            **get_sql_settings_from_config(self.config))


class BaseCoreSession(BaseSQLSession):
    __api_name__ = 'core'

    @reify
    def use_before_queries(self):
        return asbool(self.settings.get('api.core.use_before_queries', True))

    @reify
    def indexed_columns(self):
        return SQL_DBS['core']['indexed_columns']

    def get_core_query(
            self, columns, return_inactives=False,
            order_by=None, page=None, limit_per_page=None,
            ignore_before_queries=True,
            **filters):

        # Pagination
        with_pagination = bool(
            limit_per_page is not None
            or page is not None)
        if with_pagination:
            page = maybe_integer(page)
            if not page or page < 1:
                page = 1

            limit_per_page = maybe_integer(limit_per_page)
            if not limit_per_page or limit_per_page < 1:
                limit_per_page = 1000

        tables = set()
        tables_with_relations = set()
        query_columns = []
        tables_aliased = CoreAliased()

        # Create order by
        orders_by = []
        if order_by:
            if not is_nonstr_iter(order_by):
                order_by = [order_by]
            for column in order_by:
                for table in get_object_tables(column):
                    name = 'core'
                    if table.name != name:
                        name = table.name.split('core_', 1)[1]
                    table = CORE_TYPES[name]['table']
                    tables.add(table)

                    if not return_inactives:
                        # Find parent tables
                        tables.update(find_parent_tables(table))

                    # Find return columns
                    if isinstance(column, CoreColumnParent):
                        alias = tables_aliased[table.core_name]
                        column = column.get_alias_column(alias)
                    orders_by.append((table, column))

        # Define filters tables
        before_queries = MissingList()
        for core_name, core_filters in filters.items():
            table = CORE_TYPES[core_name]['table']
            tables.add(table)

            if ignore_before_queries or not self.use_before_queries:
                continue

            indexed = self.indexed_columns[core_name]
            for key in core_filters.keys():
                if key not in indexed:
                    column = getattr(table, key)
                    if isinstance(column, CoreColumnParent):
                        continue

                    values = core_filters.pop(key)
                    if isinstance(values, ClauseElement):
                        before_queries[core_name].append(values)
                    elif not is_nonstr_iter(values):
                        before_queries[core_name].append(column == values)
                    else:
                        values = set(values)
                        values_filter = maybe_with_none(column, values)
                        before_queries[core_name].append(values_filter)

        # Find return tables in columns
        for column in columns:
            for table in get_object_tables(column):
                name = 'core'
                if table.name != name:
                    name = table.name.split('core_', 1)[1]
                table = CORE_TYPES[name]['table']
                tables.add(table)

                if not return_inactives:
                    # Find parent tables
                    tables.update(find_parent_tables(table))

                # Find return columns
                if isinstance(column, CoreColumnParent):
                    alias = tables_aliased[table.core_name]
                    alias_column = column.get_alias_column(alias)
                    query_columns.append(alias_column)
                else:
                    query_columns.append(column)

        # Relations
        outerjoins = MissingList()
        queries = []
        for table in tables:
            core_relation = getattr(table, 'core_relation', None)
            if core_relation:
                relation, relation_table = core_relation
                if relation == 'branch':
                    outerjoins[relation_table].append((
                        table,
                        relation_table.id_core == table.id_core))
                    tables_with_relations.add(table)
                    continue

            is_core_table = bool(table.__tablename__ == 'core')
            if not is_core_table:
                # Relation core extesion with Core base
                tables_with_relations.add(table)
                alias = tables_aliased[table.core_name]
                queries.append(table.id_core == alias.id)
                queries.append(alias.type == table.core_name)

            # Ignore inactive objects if requested
            if not return_inactives:
                table_inactives = table
                if not is_core_table:
                    table_inactives = alias
                queries.append(
                    or_(table_inactives.start_date <= func.now(),
                        table_inactives.start_date == None))
                queries.append(
                    or_(table_inactives.end_date >= func.now(),
                        table_inactives.end_date == None))

            # Define parent relation if requested
            # or to validate if parent is active
            # Define branch relation
            if (core_relation
                and relation == 'parent'
                and (not return_inactives or relation_table in tables)):
                tables_with_relations.add(relation_table)
                queries.append(alias.parent_id == relation_table.id_core)

            core_foreign = getattr(table, 'core_foreign_key', None)
            if core_foreign is not None:
                column_key, foreign_column = core_foreign
                name = foreign_column.table.name
                if name.startswith('core_'):
                    name = name.split('core_', 1)[1]
                foreign_table = CORE_TYPES[name]['table']
                if foreign_table in tables:
                    tables_with_relations.add(foreign_table)
                    tables_with_relations.add(table)
                    queries.append(foreign_column == getattr(table, column_key))

        # Start query
        query = self.session.query(*query_columns)
        if outerjoins:
            for table, relations in outerjoins.items():
                query = query.select_from(table)
                for join_table, on_query in relations:
                    query = query.outerjoin(join_table, on_query)
        if queries:
            query = query.filter(and_(*queries))

        # Define filters
        normal_filters = MissingList()
        for core_name, core_filters in filters.items():
            table = CORE_TYPES[core_name]['table']
            for key, values in core_filters.items():
                if isinstance(values, ClauseElement):
                    normal_filters[core_name].append(values)
                else:
                    column = getattr(table, key)
                    is_alias = isinstance(column, CoreColumnParent)
                    if is_alias:
                        alias = tables_aliased[table.core_name]
                        column = column.get_alias_column(alias)

                    if not is_nonstr_iter(values):
                        key_filter = (column == values)
                    else:
                        values = set(values)
                        key_filter = maybe_with_none(column, values)

                    if is_alias:
                        query = query.filter(key_filter)
                    else:
                        normal_filters[core_name].append(key_filter)

        missing_relations = tables.difference(tables_with_relations)
        if Core in missing_relations:
            missing_relations.remove(Core)
            if len(tables) > 1:
                message = (
                    u'Dont use core with others cores tables. '
                    u'No relation possible.')
                raise Error('core', message)

        if missing_relations:
            message = (
                u'Missing tables (%s) relations'
                % u', '.join(t.__tablename__ for t in missing_relations))
            raise Error('core', message)

        # Optimization queries
        # Make others queries if requested!
        if before_queries:
            found_something = False
            before_cores_queries = []
            for core_name, before_filters in before_queries.items():
                table = CORE_TYPES[core_name]['table']

                before_query = (
                    self.session
                    .query(table.id_core)
                    .filter(and_(*before_filters)))
                if core_name in normal_filters:
                    before_query = before_query.filter(
                        and_(*normal_filters[core_name]))

                use_ids_if_less = 2501
                cores_ids = set(
                    c.id_core for c in (before_query
                        .slice(0, use_ids_if_less)
                        .all()))

                if not cores_ids:
                    found_something = True
                    continue
                elif len(cores_ids) < use_ids_if_less:
                    before_cores_queries.append(
                        table.id_core.in_(cores_ids))
                else:
                    query = (
                        query
                        .filter(and_(*before_filters))
                        .filter(and_(*normal_filters)))

            if len(before_cores_queries) == 1:
                query = query.filter(before_cores_queries[0])
            elif before_cores_queries:
                query = query.filter(or_(*before_cores_queries))
            elif found_something:
                return None

        else:
            for table_filters in normal_filters.values():
                query = query.filter(and_(*table_filters))

        # Set order by
        if orders_by:
            query = query.order_by(*(o for t, o in orders_by))

        # Pagination for main query
        if with_pagination:
            query = define_pagination(query, page, limit_per_page)

        return query

    def get_core(self, *args, **kwargs):
        query = self.get_core_query(*args, **kwargs)
        if query is not None:
            return query.first()

    def count_cores(self, core_name, group_by=None, **kwargs):
        columns = [func.count(CORE_TYPES[core_name]['table'].id_core)]
        if group_by:
            columns.insert(0, group_by)

        query = self.get_core_query(
            columns,
            **kwargs)

        if group_by:
            return dict(query.group_by(group_by).all())
        else:
            return query.first()[0] or 0

    def get_cores(
            self,
            columns,
            page=None, 
            limit_per_page=None,
            **kwargs):

        query = self.get_core_query(
            columns,
            page=page,
            limit_per_page=limit_per_page,
            **kwargs)

        if query is not None:
            if isinstance(query, QueryPagination):
                pagination = CorePagination(
                    query.page,
                    query.limit_per_page,
                    query.last_page,
                    query.number_of_results)
                pagination.extend(query.all())
                return pagination
            else:
                return query.all()
        elif limit_per_page is not None or page is not None:
            return CorePagination(
                page=1,
                limit_per_page=limit_per_page or 1000,
                last_page=1,
                number_of_results=0)
            return []
        else:
            return []

    def set_core(self, table, parent_key=None, branch_table=None):
        core_relation = getattr(table, 'core_relation', None)
        if core_relation:
            relation, relation_table = core_relation
            if relation == 'branch':
                raise 

        if branch_table:
            branch_relation, branch_relation_table = getattr(
                branch_table, 'core_relation', None)
            if (branch_relation != 'branch'
                or not isinstance(table, branch_relation_table)):
                raise Error('core', 'Invalid branch relation')

        # Set core values
        core = Core()
        for key in CORE_KEYS:
            value = getattr(table, key, None)
            if not isinstance(value, CoreColumnParent):
                setattr(core, key, value)

        # Force core values
        core.type = table.core_name
        if not core.key:
            core.key = core.make_key()

        # Find and define parent id
        if parent_key:
            if not core_relation:
                message = u'Define core_relation for %s' % table.core_name
                raise Error('parent_key', message)

            parent = self.get_core(
                [Core.id, Core.type],
                return_inactives=True,
                core={'key': parent_key})
            if not parent:
                message = u'Missing parent "%s"' % parent_key
                raise Error('parent_key', message)
            elif (relation == 'parent'
                and relation_table.core_name != parent.type):
                message = (
                    u'Cannot add parent "%s" with type "%s" to type "%s"'
                    % (parent_key, table.core_name, parent.type))
                raise Error('parent_key', message)
            core.parent_id = parent.id

        # Prevent SQLAlchemy pre-executed queries
        core.created_date = func.now()
        core.updated_date = func.now()

        # Insert core
        core_id = self.direct_insert(core).lastrowid

        # Insert direct relation
        table.id_core = core_id
        table.key = core.key
        try:
            self.direct_insert(table)
        except:
            self.direct_delete(Core, Core.id == core_id)
            raise

        # Insert branch relation, if sent!
        if branch_table:
            branch_table.id_core = core_id
            branch_table.key = core.key
            try:
                self.direct_insert(branch_table)
            except:
                self.direct_delete(Core, Core.id == core_id)
                self.direct_delete(table, table.id_core == core_id)
                raise

        self.flush()
        return core.key

    def update_core(self, core_name, values, update_inactives=False, **filters):
        if not values:
            return False

        table = CORE_TYPES[core_name]['table']

        columns = []
        core_columns = []
        for key in values.keys():
            column = getattr(table, key)
            if isinstance(column, CoreColumnParent):
                core_columns.append(column)
            else:
                columns.append(column)

        updated = False
        if columns:
            columns.append(table.id_core)
            response = self.get_core(
                columns,
                return_inactives=update_inactives,
                **filters)
            if not response:
                return False

            to_update = {}
            for key, value in values.items():
                response_value = getattr(response, key)
                if ((value is None or response_value is not None)
                    or response_value is None
                    or value != response_value):
                    to_update[key] = value

            if to_update:
                self.direct_update(
                    table,
                    table.id_core == response.id_core,
                    to_update)
                self.flush()
                updated = True

        if core_columns:
            core_columns.append(table.id_core)
            response = self.get_core(
                core_columns,
                return_inactives=update_inactives,
                **filters)
            if not response:
                return False

            to_update = {}
            for key, value in values.items():
                response_value = getattr(response, key)
                if ((value is None or response_value is not None)
                    or response_value is None
                    or value != response_value):
                    to_update[key] = value

            if to_update:
                # Prevent SQLAlchemy pre-executed queries
                to_update['updated_date'] = func.now()

                self.direct_update(
                    Core,
                    Core.id == response.id_core,
                    to_update)
                self.flush()
                updated = True

        return updated

    def inactive_core(self, id_core):
        return self.inactive_cores(id_core)

    def inactive_cores(self, ids):
        if not is_nonstr_iter(ids):
            ids = [ids]

        ids = set(ids)
        if ids:
            return bool(
                self.session
                .query(Core.id)
                .filter(Core.id.in_(ids))
                .update({'end_date': func.now()},
                        synchronize_session=False))
        else:
            return False

    def delete_core(self, core_name, id_core):
        options = CORE_TYPES[core_name]

        childs = MissingList()
        for child_id, child_type in (self.session
                .query(Core.id, Core.type)
                .filter(Core.parent_id == id_core)
                .all()):
            childs[child_type].append(child_id)

        if childs:
            for child_type, ids in childs.items():
                table = CORE_TYPES[child_type]['table']
                try:
                    (self.session
                        .query(table.id_core)
                        .filter(table.id_core.in_(ids))
                        .delete(synchronize_session=False))
                    (self.session
                        .query(Core.id)
                        .filter(Core.id.in_(ids))
                        .delete(synchronize_session=False))
                except:
                    self.rollback()
                    raise
                else:
                    self.flush()

        for branch in CORE_TYPES[core_name]['branchs']:
            (self.session
                .query(branch.id_core)
                .filter(branch.id_core == id_core)
                .delete(synchronize_session=False))
            self.flush()

        try:
            table = CORE_TYPES[core_name]['table']
            (self.session
                .query(table.id_core)
                .filter(table.id_core == id_core)
                .delete(synchronize_session=False))
            (self.session
                .query(Core.id)
                .filter(Core.id == id_core)
                .delete(synchronize_session=False))
        except:
            self.rollback()
            raise
        else:
            return True


class CoreAliased(dict):
    def __missing__(self, key):
        self[key] = aliased(Core)
        return self[key]


class Core(DeclarativeBase):
    __tablename__ = 'core'

    id = Column(Integer, primary_key=True, nullable=False)
    key = Column(Unicode(70), unique=True, index=True, nullable=False)
    # If null, means this object is a relation type
    type = Column(Unicode(50), index=True)
    parent_id = Column(Integer, ForeignKey('core.id'))

    start_date = Column(DateTime)
    end_date = Column(DateTime)
    updated_date = Column(
        DateTime,
        default=func.now(), onupdate=func.now(),
        nullable=False)
    created_date = Column(DateTime, default=func.now(), nullable=False)

    def make_key(self):
        return make_uuid_hash()

CORE_TYPES['core']['table'] = Core


class CoreColumnParent(object):
    def __init__(self, table, attribute):
        self._table = table
        self.attribute = attribute
        self.with_label = None

    def clone(self):
        return CoreColumnParent(self._table, self.attribute)

    @reify
    def table(self):
        return self._table.__table__

    @reify
    def name(self):
        return '%s.%s' % (self._table.__tablename__, self.attribute)

    def __repr__(self):
        return self.name

    def get_core_column(self):
        return getattr(Core, self.attribute)

    def get_alias_column(self, alias):
        column = getattr(alias, self.attribute)
        if self.with_label:
            return column.label(self.with_label)
        else:
            return column

    def label(self, name):
        self.with_label = name
        return self


def replace_core_attribute(wrapped):
    def decorator(self):
        name = wrapped.__name__
        value = CoreColumnParent(self, name)
        setattr(self, name, value)
        CORE_KEYS.add(name)
        return value
    return decorator


class CoreType(object):
    @declared_attr
    def __tablename__(self):
        if self.core_name in CORE_TYPES:
            message = u'Core "%s" already defined' % self.core_name
            raise Error('core', message)
        else:
            CORE_TYPES[self.core_name]['table'] = self

            core_relation = getattr(self, 'core_relation', None)
            if core_relation:
                relation, relation_table = core_relation
                if relation == 'parent':
                    CORE_TYPES[self.core_name]['parent'] = relation_table
                    CORE_TYPES[relation_table.core_name]['childs'].add(self)
                elif relation == 'branch':
                    CORE_TYPES[relation_table.core_name]['branchs'].add(self)
                else:
                    raise ValueError('Invalid relation type')

            tablename = 'core_%s' % self.core_name
            setattr(self, '__tablename__', tablename)
            return tablename

    @declared_attr
    @replace_core_attribute
    def key(self):
        pass

    @declared_attr
    @replace_core_attribute
    def type(self):
        pass

    @declared_attr
    @replace_core_attribute
    def start_date(self):
        pass

    @declared_attr
    @replace_core_attribute
    def end_date(self):
        pass

    @declared_attr
    @replace_core_attribute
    def updated_date(self):
        pass

    @declared_attr
    @replace_core_attribute
    def created_date(self):
        pass

    @declared_attr
    def id_core(self):
        return Column(
            Integer, ForeignKey(Core.id),
            primary_key=True, nullable=False)


class CoreOptions(Options):
    def add_table(self, table, ignore=None, add_name=None):
        Options.add_table(self, table, ignore=ignore, add_name=add_name)

        columns = table.__dict__.keys()
        for key in columns:
            maybe_column = getattr(table, key)
            if isinstance(maybe_column, CoreColumnParent):
                if not ignore or key not in ignore:
                    if add_name:
                        key = '%s_%s' % (add_name, key)
                    self.add_column(key, maybe_column.clone())


def find_parent_tables(table):
    # Find parent tables
    tables = set()
    while True:
        core_relation = getattr(table, 'core_relation', None)
        if not core_relation:
            break
        relation, table = core_relation
        tables.add(table)
    return tables


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


class CoreActiveField(Field):
    def __init__(self):
        super(CoreActiveField, self).__init__(
            'active',
            attributes=['start_date', 'end_date'])

    def __call__(self, request, value):
        now = NOW_DATE()
        start_date = getattr(value, 'start_date')
        end_date = getattr(value, 'end_date')
        if start_date and start_date > now:
            return False
        elif end_date and end_date < now:
            return False
        else:
            return True
