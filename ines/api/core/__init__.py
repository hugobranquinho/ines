# -*- coding: utf-8 -*-

import datetime
from copy import deepcopy
from math import ceil

from pyramid.compat import is_nonstr_iter
from pyramid.decorator import reify
from pyramid.settings import asbool
from sqlalchemy import and_
from sqlalchemy import Column
from sqlalchemy import false
from sqlalchemy import func
from sqlalchemy import not_
from sqlalchemy import or_
from sqlalchemy import true
from sqlalchemy.orm import aliased
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql.elements import BinaryExpression
from sqlalchemy.sql.elements import BindParameter
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy.sql.schema import Table

from ines.api.core.database import Base
from ines.api.core.database import Core
from ines.api.core.views import CorePagination
from ines.api.core.views import define_pagination
from ines.api.core.views import QueryPagination
from ines.api.database.sql import active_filter
from ines.api.database.sql import BaseSQLSession
from ines.api.database.sql import BaseSQLSessionManager
from ines.api.database.sql import create_filter_by
from ines.api.database.sql import get_active_filter
from ines.api.database.sql import get_object_tables
from ines.api.database.sql import maybe_with_none
from ines.api.database.sql import SQL_DBS
from ines.api.database.sql import SQLALCHEMY_VERSION
from ines.convert import force_string
from ines.convert import maybe_integer
from ines.convert import maybe_list
from ines.exceptions import Error
from ines.views.fields import OrderBy
from ines.utils import different_values
from ines.utils import MissingDict
from ines.utils import MissingList
from ines.utils import MissingSet


DATETIME = datetime.datetime
TIMEDELTA = datetime.timedelta

if SQLALCHEMY_VERSION >= '1.0':
    SQLALCHEMY_LABELS_KEY = '_fields'
else:
    SQLALCHEMY_LABELS_KEY = '_labels'


class BaseCoreSessionManager(BaseSQLSessionManager):
    __api_name__ = 'core'
    __database_name__ = 'ines.core'


class ORMQuery(object):
    def __init__(self, api_session, *attributes, **kw_attributes):
        self.api_session = api_session

        # First line attribute, add to this query, second+ lines make more queries
        self.tables = set()
        self.outerjoin_tables = MissingDict()
        self.queries = []
        self.attributes = []
        self.children_raw = MissingList()
        self.parent_raw = MissingList()
        self.parent_raw_attributes = {}

        self.raw_attributes = maybe_list(attributes)
        if kw_attributes:
            self.raw_attributes.append(kw_attributes)
        for attribute in self.raw_attributes:
            self.add_attribute(attribute)

    def __repr__(self):
        return repr(self.construct_query(active=True))

    @reify
    def sql_options(self):
        return SQL_DBS[self.api_session.api_session_manager.__database_name__]

    @reify
    def metadata(self):
        return self.sql_options['metadata']

    @reify
    def orm_tables(self):
        references = {}
        for base in self.sql_options['bases']:
            for name, table in base._decl_class_registry.items():
                if name == '_sa_module_registry':
                    continue

                references[table.__tablename__] = table
                table_alias = getattr(table, '__table_alias__', None)
                if table_alias:
                    references.update((k, table) for k in table_alias)

        return references

    def get_table(self, maybe_name):
        if isinstance(maybe_name, Table):
            return maybe_name

        elif hasattr(maybe_name, '__tablename__'):
            return maybe_name.__table__

        elif maybe_name in self.metadata.tables:
            return self.metadata.tables[maybe_name]

        elif maybe_name in self.orm_tables:
            return self.orm_tables[maybe_name].__table__

        else:
            raise AttributeError('Invalid table: %s' % force_string(maybe_name))

    def add_attribute(self, attribute, table_or_name=None):
        if hasattr(attribute, '__tablename__'):
            self.tables.add(attribute.__table__)
            self.attributes.append(attribute)

        elif isinstance(attribute, Table):
            self.tables.add(attribute)
            self.attributes.extend(attribute.c)

        elif isinstance(attribute, InstrumentedAttribute):
            self.tables.add(attribute.table)
            self.attributes.append(attribute)

        elif table_or_name is not None:
            table = self.get_table(table_or_name)
            if not isinstance(table_or_name, basestring):
                table_or_name = table.name

            if not attribute:
                self.tables.add(table)
                self.attributes.extend(table.c)

            elif isinstance(attribute, basestring):
                if '.' in attribute:
                    children_table_or_name, attribute = attribute.split('.', 1)
                    if table_or_name == children_table_or_name:
                        self.add_attribute(attribute, children_table_or_name)
                    else:
                        self.add_attribute({children_table_or_name: attribute}, table)
                else:
                     self.add_attribute({attribute: None}, table)

            elif isinstance(attribute, dict):
                for maybe_attribute, attributes in attribute.items():
                    add_for_pos_queries = False
                    if maybe_attribute in table.c:
                        self.tables.add(table)
                        self.attributes.append(table.c[maybe_attribute])
                    elif maybe_attribute == 'active':
                        self.tables.add(table)
                    else:
                        add_for_pos_queries = True
                        if not attributes:
                            orm_table = self.orm_tables[table.name]
                            if hasattr(orm_table, '__branches__'):
                                for orm_branch in orm_table.__branches__:
                                    branch_table = self.get_table(orm_branch)
                                    if maybe_attribute in branch_table.c:
                                        self.tables.add(table)
                                        self.outerjoin_tables[orm_table][orm_branch] = (orm_table.id == orm_branch.id)
                                        self.attributes.append(branch_table.c[maybe_attribute])
                                        add_for_pos_queries = False
                                        break

                    if add_for_pos_queries:
                        if not maybe_attribute:
                            raise AttributeError('Need to define a table for children / parent queries')

                        pos_table = self.get_table(maybe_attribute)
                        relation_name = maybe_attribute
                        if not isinstance(relation_name, basestring):
                            relation_name = pos_table.name

                        for foreign in table.foreign_keys:
                            if foreign.column.table is pos_table:
                                self.parent_raw[relation_name].append(attributes)
                                self.parent_raw_attributes[relation_name] = foreign.parent
                                break
                        else:
                            self.children_raw[relation_name].append(attributes)

            elif is_nonstr_iter(attribute):
                for maybe_attribute in attribute:
                    self.add_attribute(maybe_attribute, table)

            else:
                self.lookup_and_add_tables(attribute)
                self.tables.add(table)
                self.attributes.append(attribute)

        elif isinstance(attribute, basestring):
            if '.' in attribute:
                table_or_name, attribute = attribute.split('.', 1)
                self.add_attribute(attribute, table_or_name)
            else:
                self.add_attribute(None, attribute)

        elif isinstance(attribute, dict):
            for table_or_name, attributes in attribute.items():
                self.add_attribute(attributes, table_or_name)

        elif is_nonstr_iter(attribute):
            for table_or_name in attribute:
                self.add_attribute(None, table_or_name)

        else:
            self.lookup_and_add_tables(attribute)
            self.attributes.append(attribute)

        return self

    def filter(self, value):
        if isinstance(value, dict):
            for key, values in value.items():
                if isinstance(key, basestring):
                    if '.' in key:
                        table_name, attribute_name = key.split('.', 1)
                        table = self.get_table(table_name)
                        attribute = getattr(table.c, attribute_name)
                        self.tables.add(table)
                        self.queries.append(create_filter_by(attribute, values))

                    elif isinstance(values, dict):
                        table = self.get_table(key)
                        for table_key, attribute_value in values.items():
                            self.filter({getattr(table.c, table_key): attribute_value})

                    else:
                        raise AttributeError('Invalid filter column: %s' % force_string(key))

                elif isinstance(key, InstrumentedAttribute):
                    self.tables.add(key.table)
                    self.queries.append(create_filter_by(key, values))

                elif hasattr(key, '__tablename__'):
                    if isinstance(values, dict):
                        for attribute, attribute_value in values.items():
                            self.filter({getattr(table, attribute): attribute_value})
                    else:
                        raise AttributeError('Invalid filter values: %s' % force_string(values))

                elif isinstance(key, Table):
                    if isinstance(values, dict):
                        for attribute, attribute_value in values.items():
                            self.filter({getattr(table.c, attribute): attribute_value})
                    else:
                        raise AttributeError('Invalid filter values: %s' % force_string(values))

                else:
                    raise AttributeError('Invalid filter column: %s' % force_string(key))

        elif is_nonstr_iter(value):
            for deep_value in value:
                self.filter(deep_value)

        else:
            self.lookup_and_add_tables(value)
            self.queries.append(value)

        return self

    def lookup_and_add_tables(self, value):
        if isinstance(value, BinaryExpression):
            if isinstance(value.left, Column):
                self.tables.add(value.left.table)
            else:
                self.lookup_and_add_tables(value.left)

            if isinstance(value.right, Column):
                self.tables.add(value.right.table)
            else:
                self.lookup_and_add_tables(value.right)

        elif not isinstance(value, BindParameter):
            # @@TODO: Lookup for tables?
            pass

    def construct_query(
            self,
            active=True,
            ignore_active_attribute=False):

        queries = list(self.queries)
        outerjoins = MissingDict()
        tables = set(self.tables)
        attributes = list(self.attributes)

        # Extend outerjoins
        if self.outerjoin_tables:
            for key, values in self.outerjoin_tables.items():
                for deep_key, value in values.items():
                    outerjoins[key][deep_key] = value

        if self.children_raw:
            for child_name in self.children_raw.keys():
                child_orm_table = self.orm_tables[child_name]
                if hasattr(child_orm_table, '__parent__'):
                    child_parent_table = self.get_table(child_orm_table.__parent__)
                    if child_parent_table not in tables:
                        tables.add(child_parent_table)
                    child_label = child_parent_table.c.id.label('%s_child_id' % child_name)
                    if child_label not in attributes:
                        attributes.append(child_label)

        if self.parent_raw:
            for parent_name in self.parent_raw.keys():
                relation_column = self.parent_raw_attributes[parent_name]
                if relation_column.table not in tables:
                    tables.add(relation_column.table)
                parent_label = relation_column.label('%s_parent_id' % parent_name)
                if parent_label not in attributes:
                    attributes.append(parent_label)

        # Find active queries
        active_tables = set()
        active_table_children = set()

        def _actives(orm_table, as_outerjoin=False):
            if orm_table.__core_type__ == 'branch':
                as_outerjoin = True

            self_children = False
            for foreign in orm_table.__table__.foreign_keys:
                foreign_orm_table = self.orm_tables[foreign.column.table.name]
                if foreign_orm_table.__core_type__ == 'active':
                    if foreign_orm_table is orm_table:
                        active_table_children.add(foreign_orm_table)
                        self_children = True
                    else:
                        if as_outerjoin or foreign.parent.nullable:
                            outerjoins[orm_table][foreign_orm_table] = (foreign.column == foreign.parent)
                            foreign_as_outerjoin = True
                        else:
                            queries.append(foreign.column == foreign.parent)
                            foreign_as_outerjoin = False
                        _actives(foreign_orm_table, as_outerjoin=foreign_as_outerjoin)

            if not self_children and orm_table.__core_type__ == 'active':
                active_tables.add(orm_table)

        for table in self.tables:
            _actives(self.orm_tables[table.name])

        # Create active query
        active_queries = []
        if active_tables:
            active_queries.append(active_filter(active_tables))
        if active_table_children:
            # @@TODO lookup for active items, ignore items without father
            pass

        if len(active_queries) == 1:
            active_query = active_queries[0]
        elif active_queries:
            active_query = and_(*active_queries)
        else:
            active_query = None

        # Add active column
        if not ignore_active_attribute:
            if active is None:
                if active_query is not None:
                    attributes.append(active_query.label('active'))
                else:
                    attributes.append(true().label('active'))
            elif active:
                attributes.append(true().label('active'))
            else:
                attributes.append(false().label('active'))

        # Create session query
        query = self.api_session.session.query(*attributes)

        # Relate with nullable foreign
        if outerjoins:
            for from_table, foreign_tables in outerjoins.items():
                query = query.select_from(from_table)
                for outerjoin_table, outerjoin_query in foreign_tables.items():
                    query = query.outerjoin(outerjoin_table, outerjoin_query)

        # Define active/inactive filter is requested
        if active is not None and active_query is not None:
            if active:
                query = query.filter(active_query)
            else:
                query = query.filter(not_(active_query))

        if queries:
            if len(queries) == 1:
                query = query.filter(queries[0])
            else:
                query = query.filter(and_(*queries))

        return query

    def parse_results(self, results, active=True):
        if results:
            update_orm = [
                i for i, t in enumerate(self.attributes)
                if hasattr(t, '__tablename__') and t.__core_type__ == 'active']
            if update_orm:
                for result in results:
                    for i in update_orm:
                        result[i]._active = result.active

            self.update_with_secondary_queries(results, active=active)

        return results

    def update_with_secondary_queries(self, results, active=True):
        if self.children_raw:
            for children_name, attributes in self.children_raw.items():
                if attributes:
                    if isinstance(attributes, dict):
                        if 'parent_id' not in attributes:
                            attributes['parent_id'] = None
                    else:
                        attributes = maybe_list(attributes)
                        if 'parent_id' not in attributes:
                            attributes.append('parent_id')

                query = ORMQuery(self.api_session, {children_name: attributes})
                orm_table = self.orm_tables[children_name]
                if hasattr(orm_table, '__parent__'):
                    name = '%s_child_id' % children_name
                    children_ids = set(getattr(r, name) for r in results)
                    if children_ids:
                        references = MissingList()
                        for reference in query.filter(orm_table.parent_id.in_(children_ids)).all(active=active):
                            references[reference.parent_id].append(reference)
                        for result in results:
                            setattr(result, children_name, references[getattr(result, name)])
                    else:
                        for result in results:
                            setattr(result, children_name, [])
                else:
                    parent_results = query.all(active=active)
                    for result in results:
                        setattr(result, children_name, parent_results)

        if self.parent_raw:
            for parent_name, attributes in self.parent_raw.items():
                if attributes:
                    if isinstance(attributes, dict):
                        if 'id' not in attributes:
                            attributes['id'] = None
                    else:
                        attributes = maybe_list(attributes)
                        if 'id' not in attributes:
                            attributes.append('id')

                query = ORMQuery(self.api_session, {parent_name: attributes})
                name = '%s_parent_id' % parent_name
                parent_ids = set(getattr(r, name) for r in results)
                if parent_ids:
                    orm_table = self.orm_tables[parent_name]
                    references = dict((r.id, r) for r in query.filter(orm_table.id.in_(parent_ids)).all(active=active))
                    for result in results:
                        setattr(result, parent_name, references.get(getattr(result, name)))
                else:
                    for result in results:
                        setattr(result, parent_name, None)

        return results

    def one(self, active=True):
        result = self.construct_query(active=active).one()
        return self.parse_results([result], active=active)[0]

    def first(self, active=True):
        result = self.construct_query(active=active).first()
        if result is not None:
            return self.parse_results([result], active=active)[0]

    def all(self, active=True):
        return self.parse_results(self.construct_query(active=active).all(), active=active)

    def count(self, active=True):
        query = self.construct_query(active=active, ignore_active_attribute=True)
        entities = set(d['entity'] for d in query.column_descriptions if d['entity'])
        return (
            query
            .with_entities(func.count(1), *entities)
            .order_by(None)  # Ignore defined orders
            .first())[0]

    def delete(self, active=None, synchronize_session=False):
        if len(self.attributes) != 1:
            raise AttributeError('Define only the column you want to delete')

        delete_ids = [r[0] for r in self.construct_query(active=active, ignore_active_attribute=True).all()]
        if not delete_ids:
            return False
        else:
            result = (
                self.api_session.session
                .query(self.attributes[0])
                .filter(self.attributes[0].in_(delete_ids))
                .delete(synchronize_session=synchronize_session))

            self.api_session.session.flush()
            # @@TODO add actions
            return result

    def update(self, values, active=None, synchronize_session=False):
        result = self.construct_query(active=active).update(values, synchronize_session=synchronize_session)
        self.api_session.session.flush()
        # @@TODO add actions
        return result

    def disable(self, active=True):
        return self.update({'end_date': func.now()}, active=active)


class BaseCoreSession(BaseSQLSession):
    __api_name__ = 'core'

    def query(self, *attributes, **kw_attributes):
        return ORMQuery(self, *attributes, **kw_attributes)

    def add(self, orm_object):
        self.add_all([orm_object])

    def add_all(self, orm_objects):
        for orm_object in orm_objects:
            if isinstance(orm_object, Base) and not orm_object.key:
                orm_object.key = orm_object.make_key()

        self.session.add_all(orm_objects)
        self.session.flush()
        # @@TODO add actions








    def get_cores(
            self,
            core_name,
            attributes,
            order_by=None,
            page=None,
            limit_per_page=None,
            filters=None,
            active=True,
            key=None,
            start_date=None,
            end_date=None,
            updated_date=None,
            created_date=None,
            only_one=False):

        # Set Core defaults
        filters = filters or {}
        if key:
            filters.setdefault(core_name, {})['key'] = key
        if start_date:
            filters.setdefault(core_name, {})['start_date'] = start_date
        if end_date:
            filters.setdefault(core_name, {})['end_date'] = end_date
        if updated_date:
            filters.setdefault(core_name, {})['updated_date'] = updated_date
        if created_date:
            filters.setdefault(core_name, {})['created_date'] = created_date

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

        table = CORE_TYPES[core_name]['table']

        # Convert attributes do dict
        if not attributes:
            attributes = dict((k, None) for k in table._sa_class_manager.keys())
        elif isinstance(attributes, dict):
            attributes = deepcopy(attributes)
        elif not is_nonstr_iter(attributes):
            attributes = {attributes: None}
        else:
            attributes = dict((k, None) for k in attributes)

        # Lookup for table columns
        columns = set()
        for key in attributes.keys():
            if key == 'active':
                attributes.pop(key)  # Dont need this attribute anymore
                columns.add(get_active_column(Core, active))

            elif not is_nonstr_iter(key) and hasattr(table, key):
                attributes.pop(key)  # Dont need this attribute anymore
                column = getattr(table, key)
                if isinstance(column, CoreColumnParent):
                    column = getattr(Core, key)
                columns.add(column)

        # Lookup for branch columns
        branches_tables = MissingSet()
        for branch in CORE_TYPES[core_name]['branches']:
            for key in attributes.keys():
                if not is_nonstr_iter(key) and hasattr(branch, key):
                    branches_tables[branch].add(key)
        if branches_tables:
            for branch, keys in branches_tables.items():
                for key in keys:
                    attributes.pop(key, None)  # Dont need this attribute anymore
                    columns.add(getattr(branch, key))

        if not columns:
            columns.add(Core.key)

        # Lookup for child columns
        relate_with_child = {}
        relate_with_foreign = {}
        object_relations = {}
        if attributes:
            for child in CORE_TYPES[core_name]['childs']:
                for key in attributes.keys():
                    if is_nonstr_iter(key):
                        child_core_name, child_key, label_name = key
                        if child_core_name == child.core_name:
                            attributes.pop(key)  # Dont need this attribute anymore
                            if child.core_name not in relate_with_child:
                                alias_child = aliased(Core)
                                relate_with_child[child.core_name] = (child, alias_child)
                            else:
                                alias_child = relate_with_child[child.core_name][1]

                            column = getattr(child, child_key)
                            if isinstance(column, CoreColumnParent):
                                column = getattr(alias_child, child_key)
                            columns.add(column.label(label_name))

                    elif hasattr(child, key):
                        attributes.pop(key)  # Dont need this attribute anymore

                        if child.core_name not in relate_with_child:
                            alias_child = aliased(Core)
                            relate_with_child[child.core_name] = (child, alias_child)
                        else:
                            alias_child = relate_with_child[child.core_name][1]

                        column = getattr(child, key)
                        if isinstance(column, CoreColumnParent):
                            column = getattr(alias_child, key)
                        columns.add(column)

            core_foreigns = getattr(table, 'core_foreigns', None)
            if core_foreigns is not None:
                for column_key, foreign_table in core_foreigns.items():
                    foreign_possible_pattern = getattr(foreign_table, 'core_possible_pattern', None)
                    foreign_reference_name = getattr(foreign_table, 'core_reference_name', None)

                    for key in attributes.keys():
                        if is_nonstr_iter(key):
                            child_core_name, child_key, label_name = key
                            if child_core_name == foreign_table.core_name:
                                attributes.pop(key)  # Dont need this attribute anymore
                                if foreign_table.core_name not in relate_with_foreign:
                                    alias_child = aliased(Core)
                                    relate_with_foreign[foreign_table.core_name] = (
                                        foreign_table,
                                        alias_child,
                                        column_key)
                                else:
                                    alias_child = relate_with_foreign[foreign_table.core_name][1]

                                column = getattr(foreign_table, child_key)
                                if isinstance(column, CoreColumnParent):
                                    column = getattr(alias_child, child_key)
                                columns.add(column.label(label_name))

                        elif hasattr(foreign_table, key):
                            attributes.pop(key)  # Dont need this attribute anymore

                            if foreign_table.core_name not in relate_with_foreign:
                                alias_child = aliased(Core)
                                relate_with_foreign[foreign_table.core_name] = (
                                    foreign_table,
                                    alias_child,
                                    column_key)
                            else:
                                alias_child = relate_with_foreign[foreign_table.core_name][1]

                            column = getattr(foreign_table, key)
                            if isinstance(column, CoreColumnParent):
                                column = getattr(alias_child, key)
                            columns.add(column)

                        elif foreign_possible_pattern and key.startswith(foreign_possible_pattern):
                            new_key = key.split(foreign_possible_pattern, 1)[1]

                            if hasattr(foreign_table, new_key):
                                attributes.pop(key, None)  # Dont need this attribute anymore

                                if foreign_table.core_name not in relate_with_foreign:
                                    alias_child = aliased(Core)
                                    relate_with_foreign[foreign_table.core_name] = (
                                        foreign_table,
                                        alias_child,
                                        column_key)
                                else:
                                    alias_child = relate_with_foreign[foreign_table.core_name][1]

                                column = getattr(foreign_table, new_key)
                                if isinstance(column, CoreColumnParent):
                                    column = getattr(alias_child, new_key)
                                columns.add(column.label(key))

                        elif key == foreign_reference_name:
                            object_relations[key] = (foreign_table, column_key, attributes.pop(key))
                            columns.add(getattr(table, column_key))

        relate_with_father = False
        parent = CORE_TYPES[core_name]['parent']
        alias_parent = aliased(Core)
        parent_possible_names = getattr(parent, 'core_possible_names', None)
        if parent is not None and attributes:
            parent_possible_pattern = getattr(parent, 'core_possible_pattern', None)
            for key in attributes.keys():
                if is_nonstr_iter(key):
                    parent_core_name, parent_key, label_name = key
                    if parent_core_name == parent.core_name:
                        attributes.pop(key, None)  # Dont need this attribute anymore
                        relate_with_father = True

                        column = getattr(parent, parent_key)
                        if isinstance(column, CoreColumnParent):
                            column = getattr(alias_parent, parent_key)
                        columns.add(column.label(label_name))

                elif hasattr(parent, key):
                    attributes.pop(key, None)  # Dont need this attribute anymore
                    relate_with_father = True

                    column = getattr(parent, key)
                    if isinstance(column, CoreColumnParent):
                        column = getattr(alias_parent, key)
                    columns.add(column)

                elif parent_possible_pattern and key.startswith(parent_possible_pattern):
                    new_key = key.split(parent_possible_pattern, 1)[1]

                    if hasattr(parent, new_key):
                        attributes.pop(key, None)  # Dont need this attribute anymore
                        relate_with_father = True

                        column = getattr(parent, new_key)
                        if isinstance(column, CoreColumnParent):
                            column = getattr(alias_parent, new_key)
                        columns.add(column.label(key))

        if attributes:
            childs_names = [t.core_name for t in CORE_TYPES[core_name]['childs']]
            for key in attributes.keys():
                if key in childs_names:
                    continue

                elif parent and (
                        key == parent.core_name
                        or (parent_possible_names and key in parent_possible_names)):
                    columns.add(getattr(alias_parent, 'id').label('_parent_id_core'))
                    relate_with_father = True
                    continue

                raise ValueError(
                    'Attribute %s is not a child of %s'
                    % (key, core_name))
            columns.add(Core.id)

        queries = []
        if filters:
            filters = deepcopy(filters)
            if core_name in filters:
                for key, values in filters.pop(core_name).items():
                    column = getattr(table, key)
                    if isinstance(column, CoreColumnParent):
                        column = getattr(Core, key)

                    query_filter = create_filter_by(column, values)
                    if query_filter is not None:
                        queries.append(query_filter)

            core_foreigns = getattr(table, 'core_foreigns', None)
            if core_foreigns is not None:
                for column_key, foreign_table in core_foreigns.items():
                    foreign_name = foreign_table.core_name
                    if foreign_name.startswith('core_'):
                        foreign_name = foreign_name.split('core_', 1)[1]

                    if foreign_name in filters:
                        if foreign_name in relate_with_foreign:
                            (foreign_table, aliased_foreign,
                             column_key) = relate_with_foreign[foreign_name]
                        else:
                            foreign_table = CORE_TYPES[foreign_name]['table']
                            aliased_foreign = aliased(Core)
                            relate_with_foreign[foreign_name] = (
                                foreign_table,
                                aliased_foreign,
                                column_key)

                        for key, values in filters.pop(foreign_name).items():
                            column = getattr(foreign_table, key)
                            if isinstance(column, CoreColumnParent):
                                column = getattr(aliased_foreign, key)

                            query_filter = create_filter_by(column, values)
                            if query_filter is not None:
                                queries.append(query_filter)

            cores_ids = set()
            looked_cores = False
            for child_core_name, child_values in filters.items():
                if child_core_name in relate_with_child:
                    child, alias_child = relate_with_child[child_core_name]

                    child_query = self.session.query(alias_child.parent_id)
                    for key, values in child_values.items():
                        column = getattr(child, key)
                        if isinstance(column, CoreColumnParent):
                            column = getattr(alias_child, key)
                        if not is_nonstr_iter(values):
                            child_query = child_query.filter(column == values)
                        else:
                            values = set(values)
                            child_query = child_query.filter(maybe_with_none(column, values))

                    if active is not None:
                        child_query = (
                            child_query
                            .filter(alias_child.id == child.id_core)
                            .filter(get_active_filter(alias_child, active)))

                    cores_ids.update(q.parent_id for q in child_query.all())
                    looked_cores = True

                else:
                    # Look in childs
                    child_found = False
                    for child in CORE_TYPES[core_name]['childs']:
                        if child.core_name == child_core_name:
                            child_found = True
                            alias_child = aliased(Core)
                            relate_with_child[child_core_name] = (child, alias_child)

                            for key, values in filters.pop(child_core_name).items():
                                column = getattr(child, key)
                                if isinstance(column, CoreColumnParent):
                                    column = getattr(alias_child, key)

                                query_filter = create_filter_by(column, values)
                                if query_filter is not None:
                                    queries.append(query_filter)
                    if child_found:
                        continue

                    # Look in branches
                    branch_found = False
                    for branch in CORE_TYPES[core_name]['branches']:
                        if branch.core_name == child_core_name:
                            branch_found = True
                            branches_tables[branch] = None
                            for key, values in filters.pop(child_core_name).items():
                                column = getattr(branch, key)

                                query_filter = create_filter_by(column, values)
                                if query_filter is not None:
                                    queries.append(query_filter)
                    if branch_found:
                        continue

                    # Look parent
                    parent = CORE_TYPES[core_name]['parent']
                    if parent and parent.core_name == child_core_name:
                        if not relate_with_father:
                            alias_parent = aliased(Core)
                        relate_with_father = True

                        for key, values in filters.pop(child_core_name).items():
                            column = getattr(parent, key)
                            if isinstance(column, CoreColumnParent):
                                column = getattr(alias_parent, key)

                            query_filter = create_filter_by(column, values)
                            if query_filter is not None:
                                queries.append(query_filter)
                        continue

                    raise ValueError(
                        'Invalid filter %s for core %s'
                        % (child_core_name, core_name))

            if cores_ids:
                queries.append(maybe_with_none(table.id_core, cores_ids))
            elif looked_cores:
                # Pagination for main query
                if with_pagination:
                    return CorePagination(
                        page=1,
                        limit_per_page=limit_per_page,
                        last_page=1,
                        number_of_results=0)
                elif only_one:
                    return None
                else:
                    return []

        query = self.session.query(*columns)

        if relate_with_child or branches_tables:
            query = query.select_from(Core)
            for child, alias_child in relate_with_child.values():
                child_queries = [
                    alias_child.type == child.core_name,
                    alias_child.parent_id == Core.id]

                if active is not None:
                    child_queries.append(get_active_filter(alias_child, active))

                query = query.outerjoin(alias_child, and_(*child_queries))

                child_queries = [child.id_core == alias_child.id]
                #if hasattr(child, 'core_on_child_relation'):
                #    option = child.core_on_child_relation
                #    child_queries.append(getattr(child, option[0], option[1]))
                query = query.outerjoin(child, and_(*child_queries))

            for branch in branches_tables.keys():
                query = query.outerjoin(branch, branch.id_core == Core.id)

        if queries:
            query = query.filter(and_(*queries))

        if relate_with_foreign:
            for foreign_table, aliased_foreign, column_key in relate_with_foreign.values():
                query = query.filter(aliased_foreign.id == getattr(table, column_key))

                if active is not None:
                    query = query.filter(get_active_filter(aliased_foreign, active))

        if relate_with_father:
            query = (
                query
                .filter(parent.id_core == alias_parent.id)
                .filter(alias_parent.type == table_type(parent))
                .filter(Core.parent_id == alias_parent.id))
            if active is not None:
                query = query.filter(get_active_filter(alias_parent, active))

        if table is not Core:
            query = query.filter(table.id_core == Core.id).filter(Core.type == table_type(table))
        if active is not None:
            query = query.filter(get_active_filter(Core, active))

        # Set order by
        order_by = create_order_by(table, order_by)
        if order_by is not None:
            if is_nonstr_iter(order_by):
                order_by = [ob for ob in order_by if ob is not None] or None
                if order_by:
                    query = query.order_by(*order_by)
            else:
                query = query.order_by(order_by)
        if order_by is None and not only_one:
            query = query.order_by(table.id_core)

        # Pagination for main query
        if with_pagination:
            number_of_results = (
                query
                .with_entities(func.count(Core.id.distinct()))
                .first()[0])

            last_page = int(ceil(number_of_results / float(limit_per_page))) or 1
            if page > last_page:
                page = last_page
            end_slice = page * limit_per_page
            start_slice = end_slice - limit_per_page

        # Make sure if unique
        query = query.group_by(Core.id)

        if with_pagination:
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

        if not attributes or not result:
            return result

        if only_one:
            result = [result]

        labels = set(getattr(result[0], SQLALCHEMY_LABELS_KEY))
        labels.update(attributes.keys())
        labels = tuple(labels)

        if object_relations:
            for key, (key_table, column_key, key_attributes) in object_relations.items():
                references = MissingList()
                for value in result:
                    references[getattr(value, column_key)].append(value)
                key_attributes['id_core'] = None
                for value in self.get_cores(
                        key_table.core_name,
                        key_attributes,
                        active=active,
                        filters={key_table.core_name: {'id_core': references.keys()}}):
                    for child_value in references[value.id_core]:
                        setattr(child_value, key, value)

        references = {}
        parent_ids_reference = MissingList()
        for value in result:
            setattr(value, SQLALCHEMY_LABELS_KEY, labels)

            if hasattr(value, '_parent_id_core'):
                parent_ids_reference[value._parent_id_core].append(value)
            references[value.id] = value
            for key in attributes.keys():
                if parent and (
                        key == parent.core_name
                        or (parent_possible_names and key in parent_possible_names)):
                    setattr(value, key, None)
                else:
                    setattr(value, key, [])

        for key, key_attributes in attributes.items():
            key_filters = deepcopy(filters)

            if parent and (
                    key == parent.core_name
                    or (parent_possible_names and key in parent_possible_names)):
                key_filters = {parent.core_name: {'id_core': parent_ids_reference.keys()}}
                key_attributes['id_core'] = None
                for value in self.get_cores(
                        parent.core_name,
                        key_attributes,
                        active=active,
                        filters=key_filters):
                    for child_value in parent_ids_reference[value.id_core]:
                        setattr(child_value, key, value)
            else:
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
                        active=active,
                        filters=key_filters):
                    getattr(references[value.parent_id], key).append(value)

        if only_one:
            return result[0]
        else:
            return result

    def get_core(self, core_name, attributes, active=True, filters=None):
        return self.get_cores(
            core_name,
            attributes,
            active=active,
            filters=filters,
            only_one=True)

    @reify
    def application_time_zone(self):
        time_zone_hours = self.settings.get('time_zone.hours')
        time_zone_minutes = self.settings.get('time_zone.minutes')
        if time_zone_hours is not None or time_zone_minutes is not None:
            return TIMEDELTA(
                hours=int(time_zone_hours or 0),
                minutes=int(time_zone_minutes or 0))

    def set_core(self, table, parent_key=None, branch_table=None):
        relation, relation_table = getattr(table, 'core_relation', (None, None))
        if relation == 'branch':
            raise

        if branch_table:
            (branch_relation,
             branch_relation_table) = getattr(branch_table, 'core_relation', (None, None))
            if branch_relation != 'branch' or not isinstance(table, branch_relation_table):
                raise Error('core', 'Invalid branch relation')

        # Set core values
        core = Core()
        for key in CORE_KEYS:
            value = getattr(table, key, None)
            if not isinstance(value, CoreColumnParent):
                setattr(core, key, value)

        # Force core values
        core.type = table_type(table)
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
                active=None,
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

        # Convert time zones
        if core.end_date and core.end_date.utcoffset():
            core.end_date = core.end_date.replace(tzinfo=None) + core.end_date.utcoffset()
            if self.application_time_zone:
                core.end_date = core.end_date + self.application_time_zone
        if core.start_date and core.start_date.utcoffset():
            core.start_date = core.start_date.replace(tzinfo=None) + core.start_date.utcoffset()
            if self.application_time_zone:
                core.start_date = core.start_date + self.application_time_zone
        if core.start_date and core.end_date and core.end_date < core.start_date:
            message = u'Start date must be lower than end date'
            raise Error('start_date', message)

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

    def update_core(self, core_name, values, update_inactives=False, filters=None):
        if not values:
            return False

        if update_inactives:
            active = None
        else:
            active = True

        table = CORE_TYPES[core_name]['table']

        core_name_values = {}
        core_values = {}
        for key, value in values.items():
            column = getattr(table, key)
            if isinstance(column, CoreColumnParent):
                core_values[key] = value
            else:
                core_name_values[key] = value

        updated = False
        if core_name_values:
            attributes = set(core_name_values.keys())
            attributes.add('id_core')
            response = self.get_core(
                core_name,
                attributes,
                active=active,
                filters=filters)
            if not response:
                return False

            to_update = {}
            for key, value in core_name_values.items():
                response_value = getattr(response, key)
                if different_values(value, response_value):
                    to_update[key] = value

            if to_update:
                self.direct_update(
                    table,
                    table.id_core == response.id_core,
                    to_update)
                self.flush()
                updated = True

        if core_values or updated:
            core_attributes = set(core_values.keys())
            core_attributes.update(['start_date', 'end_date'])
            core_attributes.add('id_core')
            response = self.get_core(
                core_name,
                core_attributes,
                active=active,
                filters=filters)
            if not response:
                return False

            # Convert time zones
            if 'start_date' in core_values:
                start_date = core_values['start_date']
                if isinstance(start_date, DATETIME) and start_date.utcoffset():
                    start_date = start_date.replace(tzinfo=None) + start_date.utcoffset()
                    if self.application_time_zone:
                        start_date = start_date + self.application_time_zone
                    core_values['start_date'] = start_date

            if 'end_date' in core_values:
                end_date = core_values['end_date']
                if isinstance(end_date, DATETIME) and end_date.utcoffset():
                    end_date = end_date.replace(tzinfo=None) + end_date.utcoffset()
                    if self.application_time_zone:
                        end_date = end_date + self.application_time_zone
                    core_values['end_date'] = end_date

            to_update = {}
            for key, value in core_values.items():
                response_value = getattr(response, key)
                if different_values(value, response_value):
                    to_update[key] = value

            start_date = to_update.get('start_date', response.start_date)
            end_date = to_update.get('end_date', response.end_date)
            if start_date and end_date and end_date < start_date:
                message = u'Start date must be lower than end date'
                raise Error('start_date', message)

            if to_update or updated:
                # Prevent SQLAlchemy pre-executed queries
                to_update['updated_date'] = func.now()

                self.direct_update(
                    Core,
                    Core.id == response.id_core,
                    to_update)
                self.flush()
                updated = True

        return updated

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

    def inactive_core(self, id_core):
        return self.inactive_cores(id_core)

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

    def count_cores(self, core_name, group_by=None, active=True, other_filter=None):
        table = CORE_TYPES[core_name]['table']
        columns = [func.count(table.id_core)]
        if group_by is not None:
            columns.insert(0, group_by)

        query = self.session.query(*columns)
        if active is not None:
            query = (
                query
                .filter(get_active_filter(Core, active))
                .filter(table.id_core == Core.id))

        if other_filter:
            query = other_filter(query)

        if group_by is not None:
            return dict(query.group_by(group_by).all())
        else:
            return query.first()[0] or 0

    def get_dates_in_period(self, core_name, start_date, end_date, attributes=None):
        columns = get_core_columns(attributes)
        return (
            self.session
            .query(*columns)
            .filter(Core.type == core_name)
            .filter(or_(
                and_(Core.start_date.is_(None), Core.end_date.is_(None)),
                not_(or_(Core.end_date < start_date, Core.start_date > end_date))))
            .all())

    @reify
    def use_before_queries(self):
        return asbool(self.settings.get('api.core.use_before_queries', True))

    @reify
    def indexed_columns(self):
        return SQL_DBS['core']['indexed_columns']

    def old_get_core_query(
            self,
            columns,
            active=True,
            order_by=None,
            page=None,
            limit_per_page=None,
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
        order_by = []
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

                    if active:
                        # Find parent tables
                        tables.update(find_parent_tables(table))

                    # Find return columns
                    if isinstance(column, CoreColumnParent):
                        alias = tables_aliased[table.core_name]
                        column = column.get_alias_column(alias)
                    order_by.append((table, column))

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

                if active:
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
            if active:
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
            if relation == 'parent' and (active or relation_table in tables):
                tables_with_relations.add(relation_table)
                queries.append(alias.parent_id == relation_table.id_core)

            core_foreigns = getattr(table, 'core_foreigns', None)
            if core_foreigns is not None:
                for column_key, foreign_table in core_foreigns.items():
                    if foreign_table in tables:
                        tables_with_relations.add(foreign_table)
                        tables_with_relations.add(table)
                        queries.append(foreign_table.id_core == getattr(table, column_key))

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
        if order_by:
            query = query.order_by(*(o for t, o in order_by))

        # Pagination for main query
        if with_pagination:
            query = define_pagination(query, page, limit_per_page)

        return query

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


def table_type(table):
    if hasattr(table, 'core_relation'):
        relation_type, relation_table = table.core_relation
        if relation_type == 'branch':
            return table_type(relation_table)
    return table.core_name


def query_order_by(query, table, maybe_column):
    order = create_order_by(table, maybe_column)
    if order is not None:
        if is_nonstr_iter(order):
            order = [ob for ob in order if ob is not None]
            if order:
                return query.order_by(*order)
        else:
            return query.order_by(order)
    return query


def create_order_by(table, maybe_column, descendant=False):
    if isinstance(maybe_column, basestring):
        column = getattr(table, maybe_column, None)
        return create_order_by(table, column, descendant)

    elif isinstance(maybe_column, OrderBy):
        return create_order_by(table, maybe_column.column_name, maybe_column.descendant)

    elif is_nonstr_iter(maybe_column):
        if (len(maybe_column) == 2
                and not isinstance(maybe_column[1], OrderBy)
                and maybe_column[1].lower() == 'desc'):
            return create_order_by(table, maybe_column[0], descendant=True)
        else:
            return [create_order_by(table, ob, descendant) for ob in maybe_column]

    elif maybe_column is not None:
        if isinstance(maybe_column, CoreColumnParent):
            maybe_column = maybe_column.get_core_column()

        if descendant:
            return maybe_column.desc()
        else:
            return maybe_column


def get_core_columns(attributes=None):
    if not attributes:
        return Core._sa_class_manager.values()
    else:
        return [Core._sa_class_manager[k] for k in attributes]
