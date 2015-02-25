# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

from copy import deepcopy
from math import ceil

from pyramid.compat import is_nonstr_iter
from pyramid.decorator import reify
from pyramid.settings import asbool
from sqlalchemy import and_
from sqlalchemy import func
from sqlalchemy import or_
from sqlalchemy.orm import aliased
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy.sql.expression import true

from ines.api import BaseSessionManager
from ines.api.core.database import Core
from ines.api.core.database import CORE_KEYS
from ines.api.core.database import CORE_TYPES
from ines.api.core.database import CoreAliased
from ines.api.core.database import CoreColumnParent
from ines.api.core.database import find_parent_tables
from ines.api.core.views import CorePagination
from ines.api.core.views import define_pagination
from ines.api.core.views import QueryPagination
from ines.api.database import BaseSQLSession
from ines.api.database.sql import initialize_sql
from ines.api.database.sql import get_object_tables
from ines.api.database.sql import get_sql_settings_from_config
from ines.api.database.sql import maybe_with_none
from ines.api.database.sql import SQL_DBS
from ines.convert import maybe_integer
from ines.exceptions import Error
from ines.middlewares.repozetm import RepozeTMMiddleware
from ines.utils import MissingList
from ines.utils import MissingSet


class BaseCoreSessionManager(BaseSessionManager):
    __api_name__ = 'core'
    __middlewares__ = [RepozeTMMiddleware]

    def __init__(self, *args, **kwargs):
        super(BaseCoreSessionManager, self).__init__(*args, **kwargs)

        self.db_session = initialize_sql(
            'core',
            **get_sql_settings_from_config(self.config))


def not_inactives_filter(column):
    return and_(
        or_(Core.start_date <= func.now(), Core.start_date.is_(None)),
        or_(Core.end_date >= func.now(), Core.end_date.is_(None)))


class BaseCoreSession(BaseSQLSession):
    __api_name__ = 'core'

    def get_cores(
            self,
            core_name,
            attributes,
            order_by=None, page=None, limit_per_page=None,
            return_inactives=False,
            filters=None,
            only_one=False):

        # Pagination
        with_pagination = bool(
            limit_per_page is not None
            or page is not None)
        if with_pagination:
            only_one = False
            page = maybe_integer(page)
            if not page or page < 1:
                page = 1
            limit_per_page = maybe_integer(limit_per_page)
            if not limit_per_page or limit_per_page < 1:
                limit_per_page = 1000

        relate_with_core = not return_inactives
        table = CORE_TYPES[core_name]['table']

        if not attributes:
            attributes = table._sa_class_manager.values()
        elif isinstance(attributes, (tuple, list)):
            attributes = dict((k, None) for k in attributes)
        else:
            attributes = deepcopy(attributes)

        columns = set()
        for key in attributes.keys():
            if key == 'active':
                attributes.pop(key)  # Dont need this attribute anymore
                if return_inactives:
                    columns.add(not_inactives_filter(Core).label('active'))
                else:
                    columns.add(true().label('active'))
            elif hasattr(table, key):
                attributes.pop(key)  # Dont need this attribute anymore
                column = getattr(table, key)
                if isinstance(column, CoreColumnParent):
                    column = getattr(Core, key)
                    relate_with_core = True
                columns.add(column)

        branches_tables = MissingSet()
        for branch in CORE_TYPES[core_name]['branches']:
            for key in attributes.keys():
                if hasattr(branch, key):
                    branches_tables[branch].add(key)
        if branches_tables:
            for branch, keys in branches_tables.items():
                for key in keys:
                    attributes.pop(key, None)  # Dont need this attribute anymore
                    columns.add(getattr(branch, key))

        if not columns:
            columns.add(Core.key)

        relate_with_child = {}
        if attributes:
            for child in CORE_TYPES[core_name]['childs']:
                for key in attributes.keys():
                    if hasattr(child, key):
                        attributes.pop(key, None)  # Dont need this attribute anymore
                        columns.add(getattr(child, key))
                        relate_with_child[child.core_name] = (child, aliased(Core))
                        relate_with_core = True

        if attributes:
            childs_names = [t.core_name for t in CORE_TYPES[core_name]['childs']]
            for key in attributes.keys():
                if key not in childs_names:
                    raise ValueError(
                        'Attribute %s is not a child of %s'
                        % (key, core_name))
            columns.add(Core.id)

        query = self.session.query(*columns)

        if branches_tables:
            query = query.select_from(table)
            for branch in branches_tables.keys():
                query = query.outerjoin(branch, branch.id_core == table.id_core)

        filters = deepcopy(filters)
        if filters:
            if core_name in filters:
                for key, values in filters.pop(core_name).items():
                    column = getattr(table, key)
                    if isinstance(column, CoreColumnParent):
                        column = getattr(Core, key)
                        relate_with_core = True

                    if not is_nonstr_iter(values):
                        query  = query.filter(column == values)
                    else:
                        values = set(values)
                        query  = query.filter(maybe_with_none(column, values))

            for child_core_name, values in filters.items():
                if child_core_name in relate_with_child:
                    child, alias_child = relate_with_child[child_core_name]

                    for key, values in filters.pop(child_core_name).items():
                        column = getattr(child, key)
                        if isinstance(column, CoreColumnParent):
                            column = getattr(alias_child, key)
                        if not is_nonstr_iter(values):
                            query  = query.filter(column == values)
                        else:
                            values = set(values)
                            query  = query.filter(maybe_with_none(column, values))

        if relate_with_child:
            for child, alias_child in relate_with_child.values():
                query = (
                    query
                    .filter(child.id_core == alias_child.id)
                    .filter(alias_child.parent_id == Core.id))
                if not return_inactives:
                    query = query.filter(not_inactives_filter(alias_child))

        if relate_with_core:
            query = query.filter(table.id_core == Core.id)
        if not return_inactives:
            query = query.filter(not_inactives_filter(Core))

        # Set order by
        if order_by:
            query = query.order_by(order_by)

        # Pagination for main query
        if with_pagination:
            number_of_results = (
                query
                .with_entities(func.count(1))
                .first()[0])

            last_page = int(ceil(number_of_results / float(limit_per_page))) or 1
            if page > last_page:
                page = last_page
            end_slice = page * limit_per_page
            start_slice = end_slice - limit_per_page

            result = CorePagination(
                page,
                limit_per_page,
                last_page,
                number_of_results)
            result.extend(query.slice(start_slice, end_slice).all())
        elif only_one:
            result = query.first()
        else:
            result = query.all()

        print attributes.keys(), query
        if not attributes or not result:
            return result

        if only_one:
            result = [result]

        labels = set(result[0]._labels)
        labels.update(attributes.keys())
        labels = tuple(labels)

        references = {}
        for value in result:
            value._labels = labels
            references[value.id] = value
            for key in attributes.keys():
                setattr(value, key, [])

        for key, key_attributes in attributes.items():
            key_filters = deepcopy(filters)
            if key_filters:
                key_parents_ids = references.keys()
                if key in key_filters and 'parent_id' in key_filters[key]:
                    key_parents_ids = set(key_parents_ids)
                    if not is_nonstr_iter(key_filters[key]['parent_id']):
                        key_parents_ids.add(key_filters[key]['parent_id'])
                    else:
                        key_parents_ids.update(key_filters[key]['parent_id'])

                key_filters[key] = {'parent_id': key_parents_ids}
            else:
                key_filters = {key: {'parent_id': references.keys()}}

            key_attributes['parent_id'] = None
            for value in self.get_cores(
                    key,
                    key_attributes,
                    return_inactives=return_inactives,
                    filters=key_filters):
                getattr(references[value.parent_id], key).append(value)

        if only_one:
            return result[0]
        else:
            return result



    def get_core(self, core_name, attributes, return_inactives=False, filters=None):
        return self.get_cores(
            core_name,
            attributes,
            return_inactives=return_inactives,
            filters=filters,
            only_one=True)







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
            relation, relation_table = getattr(table, 'core_relation', (None, None))
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
                        table_inactives.start_date.is_(None)))
                queries.append(
                    or_(table_inactives.end_date >= func.now(),
                        table_inactives.end_date.is_(None)))

            # Define parent relation if requested
            # or to validate if parent is active
            # Define branch relation
            if relation == 'parent' and (not return_inactives or relation_table in tables):
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
                    c.id_core for c in (
                        before_query
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

    def old_get_core(self, *args, **kwargs):
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

    def old_get_cores(
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
        else:
            return []

    def set_core(self, table, parent_key=None, branch_table=None):
        relation, relation_table = getattr(table, 'core_relation', (None, None))
        if relation == 'branch':
            raise

        if branch_table:
            branch_relation, branch_relation_table = getattr(branch_table, 'core_relation', (None, None))
            if branch_relation != 'branch' or not isinstance(table, branch_relation_table):
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
            if not relation:
                message = u'Define core_relation for %s' % table.core_name
                raise Error('parent_key', message)

            parent = self.get_core(
                'core',
                attributes=['id', 'type'],
                return_inactives=True,
                filters={'core': {'key': parent_key}})
            if not parent:
                message = u'Missing parent "%s"' % parent_key
                raise Error('parent_key', message)
            elif relation == 'parent' and relation_table.core_name != parent.type:
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
        childs = MissingList()
        for child_id, child_type in (
                self.session
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

        for branch in CORE_TYPES[core_name]['branches']:
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
