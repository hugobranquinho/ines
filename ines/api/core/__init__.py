# -*- coding: utf-8 -*-

from copy import deepcopy
import datetime

from pyramid.compat import is_nonstr_iter
from pyramid.decorator import reify
from sqlalchemy import and_
from sqlalchemy import Column
from sqlalchemy import false
from sqlalchemy import func
from sqlalchemy import not_
from sqlalchemy import true
from sqlalchemy.orm import aliased
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql.elements import BinaryExpression
from sqlalchemy.sql.elements import BindParameter
from sqlalchemy.sql.schema import Table

from ines import MARKER
from ines.api.core.database import ActiveBase
from ines.api.core.database import Base
from ines.api.core.database import Core
from ines.api.core.views import CorePagination
from ines.api.core.views import QueryPagination
from ines.api.database.sql import active_filter
from ines.api.database.sql import BaseSQLSession
from ines.api.database.sql import BaseSQLSessionManager
from ines.api.database.sql import create_filter_by
from ines.api.database.sql import date_in_period_filter
from ines.api.database.sql import new_lightweight_named_tuple
from ines.api.database.sql import Pagination
from ines.api.database.sql import SQL_DBS
from ines.convert import convert_timezone
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
ACTIVE_MARKER = object()


class BaseCoreSessionManager(BaseSQLSessionManager):
    __api_name__ = 'core'
    __database_name__ = 'ines.core'


class ORMQuery(object):
    def __init__(self, api_session, *attributes, **kw_attributes):
        self.api_session = api_session
        self.options = QueryLookup(api_session.api_session_manager.__database_name__)

        for attribute in attributes:
            self.add_attribute(attribute)
        if kw_attributes:
            self.add_attribute(kw_attributes)

    def add_attribute(self, attribute, table_or_name=None):
        self.options.add_attribute(attribute, table_or_name)
        return self

    def filter(self, value):
        self.options.add_filter(value)
        return self

    def query_relations(self, tables, relate_actives=False):
        active_tables = set()
        active_table_children = set()
        outerjoins = MissingDict()
        queries = []

        def set_relations(orm_table, as_outerjoin=False):
            if orm_table.__core_type__ == 'branch':
                as_outerjoin = True

            self_children = False
            related_tables = set()
            foreign_keys = list(orm_table.__table__.foreign_keys)
            if len(foreign_keys) > 1 and hasattr(orm_table, '__parent__'):
                # Set parent as first on the line!
                position = [i for i, f in enumerate(foreign_keys) if f.parent.name == 'parent_id'][0]
                foreign_keys.insert(0, foreign_keys.pop(position))

            for foreign in foreign_keys:
                foreign_orm_table = self.options.orm_tables[foreign.column.table.name]
                if foreign_orm_table in related_tables:
                    foreign_orm_table = aliased(foreign_orm_table)
                else:
                    related_tables.add(foreign_orm_table)
                foreign_column = getattr(foreign_orm_table, foreign.column.name)

                if relate_actives and foreign_orm_table.__core_type__ == 'active':
                    if foreign_orm_table is orm_table:
                        active_table_children.add(foreign_orm_table)
                        self_children = True
                    else:
                        if as_outerjoin or foreign.parent.nullable:
                            outerjoins[orm_table][foreign_orm_table] = (foreign_column == foreign.parent)
                            foreign_as_outerjoin = True
                        else:
                            queries.append(foreign_column == foreign.parent)
                            foreign_as_outerjoin = False
                        set_relations(foreign_orm_table, as_outerjoin=foreign_as_outerjoin)

                elif foreign_column.table in tables:
                    if as_outerjoin or foreign.parent.nullable:
                        outerjoins[orm_table][foreign_orm_table] = (foreign_column == foreign.parent)
                    else:
                        queries.append(foreign_column == foreign.parent)

                else:
                    set_relations(foreign_orm_table, as_outerjoin=as_outerjoin)

            if not self_children and relate_actives and orm_table.__core_type__ == 'active':
                active_tables.add(orm_table)

        for table in tables:
            set_relations(self.options.orm_tables[table.name])

        active_query = None
        if relate_actives:
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

        return outerjoins, queries, active_query

    def construct_query(self, active=True):
        queries = list(self.options.queries)
        tables = set(self.options.tables)
        attributes = list(self.options.attributes)
        order_by = list(self.options.order_by)

        # Extend outerjoins
        outerjoins = MissingDict()
        if self.options.outerjoin_tables:
            for key, values in self.options.outerjoin_tables.items():
                for deep_key, value in values.items():
                    outerjoins[key][deep_key] = value

        if self.options.secondary_children:
            for child_name in self.options.secondary_children.keys():
                child_orm_table = self.options.orm_tables[child_name]
                if hasattr(child_orm_table, '__parent__'):
                    child_parent_table = self.options.get_table(child_orm_table.__parent__)
                    if child_parent_table not in tables:
                        tables.add(child_parent_table)
                    child_label = child_parent_table.c.id.label('%s_child_id' % child_name)
                    if child_label not in attributes:
                        attributes.append(child_label)

        if self.options.secondary_parents:
            for parent_name in self.options.secondary_parents.keys():
                relation_column = self.options.secondary_parent_attributes[parent_name]
                if relation_column.table not in tables:
                    tables.add(relation_column.table)
                parent_label = relation_column.label('%s_parent_id' % parent_name)
                if parent_label not in attributes:
                    attributes.append(parent_label)

        active_in_attributes = False
        for attribute in attributes:
            if attribute is ACTIVE_MARKER:
                active_in_attributes = True
                break
        active_in_order_by = False
        if order_by:
            for ob in order_by:
                if ob is ACTIVE_MARKER:
                    active_in_order_by = True
                    break

        # Check if we need to check active tables
        relate_actives = bool(active is not None or active_in_attributes or active_in_order_by)

        related_outerjoins, related_queries, active_query = self.query_relations(tables, relate_actives)
        for t, fts in related_outerjoins.items():
            outerjoins[t].update(fts)
        queries.extend(related_queries)

        # Add active column
        if active_in_attributes:
            if active is None:
                if active_query is not None:
                    active_attribute = active_query
                else:
                    active_attribute = true()
            elif active:
                active_attribute = true()
            else:
                active_attribute = false()

            # Replace active attributes
            for i, attribute in enumerate(attributes):
                if attribute is ACTIVE_MARKER:
                    attributes[i] = active_attribute.label('active')

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

        if self.options.group_by:
            query = query.group_by(*self.options.group_by)

        if order_by:
            if active_in_order_by:
                if active_query is not None:
                    for i, ob in reversed(list(enumerate(order_by))):
                        if ob is ACTIVE_MARKER:
                            order_by.pop(i)
                else:
                    for i, ob in enumerate(order_by):
                        if ob is ACTIVE_MARKER:
                            order_by[i] = active_query

            query = query.order_by(*order_by)

        if self.options.slice:
            query = query.slice(*self.options.slice)

        return query

    def parse_results(self, results, active=True):
        if not results:
            return results

        update_orm = [
            i for i, t in enumerate(self.options.attributes)
            if hasattr(t, '__tablename__') and t.__core_type__ == 'active']
        if update_orm:
            for result in results:
                for i in update_orm:
                    result[i]._active = result.active

        self.set_secondary_children(results, active=active)
        self.set_secondary_parents(results, active=active)
        return results

    def set_secondary_children(self, results, active=True):
        for children_name, attributes in self.options.secondary_children.items():
            attribute_name = '%s_child_id' % children_name
            new_namedtuple = new_lightweight_named_tuple(results[0], children_name)

            orm_table = self.options.orm_tables[children_name]
            if hasattr(orm_table, '__parent__'):
                parent_ids = set(getattr(r, attribute_name) for r in results)
                if not parent_ids:
                    results[:] = [new_namedtuple(r + ([], )) for r in results]
                    continue
            else:
                parent_ids = None

            new_attributes = {'parent_id': None}
            for attr in attributes:
                if not isinstance(attr, dict):
                    new_attributes.update((k, None) for k in maybe_list(attr))
                else:
                    new_attributes.update(deepcopy(attr))

            references = MissingList()
            for child in getattr(self.api_session, 'get_%s' % orm_table.__tablename__)(
                    parent_id=parent_ids,
                    attributes=new_attributes,
                    order_by=self.options.secondary_children_order_by.get(children_name),
                    active=active):
                references[child.parent_id].append(child)

            results[:] = [
                new_namedtuple(r + (references[getattr(r, attribute_name)], ))
                for r in results]

    def set_secondary_parents(self, results, active=True):
        for parent_name, attributes in self.options.secondary_parents.items():
            attribute_name = '%s_parent_id' % parent_name
            new_namedtuple = new_lightweight_named_tuple(results[0], parent_name)

            child_ids = set(getattr(r, attribute_name) for r in results)
            if not child_ids:
                results[:] = [new_namedtuple(r + (None, )) for r in results]
            else:
                new_attributes = {'id': None}
                for attr in attributes:
                    if not isinstance(attr, dict):
                        new_attributes.update((k, None) for k in maybe_list(attr))
                    else:
                        new_attributes.update(deepcopy(attr))

                orm_table = self.options.orm_tables[parent_name]
                references = dict(
                    (p.id, p)
                    for p in getattr(self.api_session, 'get_%s' % orm_table.__tablename__)(
                            id=child_ids,
                            attributes=new_attributes,
                            order_by=self.options.secondary_children_order_by.get(parent_name),
                            active=active))

                results[:] = [
                    new_namedtuple(r + (references.get(getattr(r, attribute_name)), ))
                    for r in results]

    def slice(self, start, stop):
        self.options.slice = (start, stop)
        return self

    def one(self, active=True):
        response = self.construct_query(active=active).one()
        return self.parse_results([response], active=active)[0]

    def first(self, active=True):
        response = self.construct_query(active=active).first()
        if response is not None:
            return self.parse_results([response], active=active)[0]

    def all(self, active=True):
        query = self.construct_query(active=active)
        if self.options.page:
            response = Pagination(
                query,
                page=self.options.page,
                limit_per_page=self.options.limit_per_page)
        else:
            response = query.all()

        parsed_response = self.parse_results(response, active=active)
        if self.options.page and parsed_response is not response:
            # need to replace list to keep Pagination options
            response[:] = parsed_response
            return response
        else:
            return parsed_response

    def count(self, active=True):
        query = self.construct_query(active=active)
        entities = set(d['expr'] for d in query.column_descriptions if d.get('expr') is not None)
        return (
            query
            .with_entities(func.count(1), *entities)
            .order_by(None)  # Ignore defined orders
            .all())

    def simple_count(self, active=True):
        result = self.count(active=active)
        if result:
            return result[0][0]
        else:
            return 0

    def delete(self, active=None, synchronize_session=False):
        if len(self.options.attributes) != 1:
            raise AttributeError('Define only the column you want to delete')

        column = self.options.attributes[0]
        orm_table = self.options.orm_tables[column.table.name]

        query = ORMQuery(self.api_session)
        query.join_options(self.options)
        query.add_attribute(orm_table)
        response = query.construct_query(active=active).all()

        if not response:
            return False
        else:
            delete_ids = [r[0] for r in response]
            result = (
                self.api_session.session
                .query(column)
                .filter(column.in_(delete_ids))
                .delete(synchronize_session=synchronize_session))
            self.api_session.session.flush()

            type_name = column.table.name
            column_names = [c for c in column.table.c.keys() if c not in ('updated_date', 'created_date')]
            for type_id, table in response:
                self.api_session.add_activity(
                    action='delete',
                    type=type_name,
                    type_id=type_id,
                    context_id=getattr(table, 'parent_id', None),
                    data=dict((c, getattr(table, c)) for c in column_names))

            return result

    def join_options(self, options):
        self.options.join(options)
        return self

    def update(self, values, active=None, synchronize_session=False):
        if len(self.options.attributes) != 1:
            raise AttributeError('Define only the column you want to update')
        elif not values:
            return False

        column = self.options.attributes[0]
        attributes = set(values.keys())
        if 'parent_id' in column.table.c:
            attributes.add('parent_id')

        if 'start_date' in values or 'end_date' in values:
            attributes.update(['start_date', 'end_date'])

        response = (
            ORMQuery(self.api_session, {column.table: attributes})
            .join_options(self.options)
            .all(active=active))
        if not response:
            return False

        # Convert to string keys dict
        values = dict((k if isinstance(k, basestring) else k.name, v) for k, v in values.items())

        # Convert time zones
        if 'start_date' in values:
            values['start_date'] = convert_timezone(values['start_date'], self.api_session.application_time_zone)
        if 'end_date' in values:
            values['end_date'] = convert_timezone(values['end_date'], self.api_session.application_time_zone)

        update_ids = MissingSet()
        for orm_response in response:
            update_keys = []
            for key, value in values.items():
                response_value = getattr(orm_response, key)
                if different_values(value, response_value):
                    update_keys.append(key)

            if update_keys:
                update_keys.sort()
                update_ids[tuple(update_keys)].add(getattr(orm_response, column.name))

                if 'start_date' in update_keys or 'end_date' in update_keys:
                    start_date = values.get('start_date', orm_response.start_date)
                    end_date = values.get('end_date', orm_response.end_date)
                    if start_date and end_date and start_date < end_date:
                        message = u'Start date must be lower than end date'
                        raise Error('start_date', message)

        if not update_ids:
            return False

        parent_ids = {}
        type_name = column.table.name
        if 'parent_id' in column.table.c:
            parent_ids = dict((r.id, r.parent_id) for r in response)

        updated = False
        for keys, ids in update_ids.items():
            update_values = dict((k, values.get(k)) for k in keys)
            if 'updated_date' not in update_values:
                # Prevent SQLAlchemy pre-executed queries
                values['updated_date'] = func.now()

            if (self.api_session.session
                    .query(column)
                    .filter(column.in_(ids))
                    .update(update_values, synchronize_session=synchronize_session)):
                updated = True
                self.api_session.session.flush()

                updated_items = dict(
                    (k, v)
                    for k, v in update_values.items()
                    if k not in ('updated_date', 'created_date'))
                for type_id in ids:
                    self.api_session.add_activity(
                        action='update',
                        type=type_name,
                        type_id=type_id,
                        context_id=parent_ids.get(type_id),
                        data=updated_items)

        return updated

    def disable(self, active=True):
        return self.update({'end_date': func.now()}, active=active)

    def group_by(self, *arguments):
        self.options.add_group_by(*arguments)
        return self

    def order_by(self, *arguments, **kwargs):
        self.options.add_order_by(*arguments, **kwargs)
        return self

    def secondary_order_by(self, **kwargs):
        self.options.add_secondary_order_by(**kwargs)
        return self

    def on_page(self, page=1, limit_per_page=20):
        self.options.add_page(page, limit_per_page)
        return self

    def date_in_period(self, table, start_date, end_date):
        self.filter(date_in_period_filter(table, start_date, end_date))
        return self


def default_message_method(action, type, type_id, context_id, **data):
    type_name = u' '.join(type.split()).title()
    message = u'%s (%s) %s' % (type_name, type_id, action.lower())
    if context_id:
        message += u' from %s' % context_id
    return message


class BaseCoreSession(BaseSQLSession):
    __api_name__ = 'core'

    @reify
    def application_time_zone(self):
        time_zone_hours = self.settings.get('time_zone.hours')
        time_zone_minutes = self.settings.get('time_zone.minutes')
        if time_zone_hours is not None or time_zone_minutes is not None:
            return TIMEDELTA(
                hours=int(time_zone_hours or 0),
                minutes=int(time_zone_minutes or 0))

    def add_activity(self, action, type, type_id, context_id=None, data=None):
        message = default_message_method(action, type, type_id, context_id, **(data or {}))

        # Set to logging
        extra = dict(('extra_%s' % k, v) for k, v in (data or {}).items())
        extra.update({
            'action': action,
            'action_type': type,
            'action_type_id': type_id,
            'context_id': context_id})
        self.logging.log('%s_%s' % (type, action), message, extra=extra)

    def query(self, *attributes, **kw_attributes):
        return ORMQuery(self, *attributes, **kw_attributes)

    def add(self, orm_object):
        self.add_all([orm_object])

    def add_all(self, orm_objects):
        if not orm_objects:
            return False

        for value in orm_objects:
            if isinstance(value, Base):
                if not value.key:
                    value.key = value.make_key()

                # Prevent SQLAlchemy pre-executed queries
                value.created_date = func.now()
                value.updated_date = func.now()

            if isinstance(value, ActiveBase):
                # Convert time zones
                value.start_date = convert_timezone(value.start_date, self.application_time_zone)
                value.end_date = convert_timezone(value.end_date, self.application_time_zone)
                if value.start_date and value.end_date and value.end_date < value.start_date:
                    message = u'Start date must be lower than end date'
                    raise Error('start_date', message)

        self.session.add_all(orm_objects)
        self.session.flush()

        type_name = orm_objects[0].__table__.name
        column_names = [c for c in orm_objects[0].__table__.c.keys() if c not in ('updated_date', 'created_date')]
        for value in orm_objects:
            self.add_activity(
                action='add',
                type=type_name,
                type_id=value.id,
                context_id=getattr(value, 'parent_id', None),
                data=dict((c, getattr(value, c)) for c in column_names))


class QueryLookup(object):
    def __init__(self, database_name):
        self.database_name = database_name
        self.tables = set()
        self.attributes = []
        self.queries = []
        self.group_by = []
        self.order_by = []
        self.outerjoin_tables = MissingDict()
        self.secondary_parents = MissingList()
        self.secondary_parent_attributes = {}
        self.secondary_children = MissingList()
        self.secondary_children_order_by = MissingList()
        self.page = None
        self.limit_per_page = None
        self.slice = None

    def join(self, attribute_lookup):
        self.tables.update(attribute_lookup.tables)
        self.attributes.extend(attribute_lookup.attributes)
        self.queries.extend(attribute_lookup.queries)
        self.group_by.extend(attribute_lookup.group_by)
        self.order_by.extend(attribute_lookup.order_by)

        for orm_table, orm_branches in attribute_lookup.outerjoin_tables.items():
            self.outerjoin_tables[orm_table].update(orm_branches)

        for relation_name, attributes in attribute_lookup.secondary_parents.items():
            self.secondary_parents[relation_name].extend(attributes)
        self.secondary_parent_attributes.update(attribute_lookup.secondary_parent_attributes)

        for relation_name, attributes in attribute_lookup.secondary_children.items():
            self.secondary_children[relation_name].extend(attributes)

    def add_outerjoin(self, orm_table, orm_branch):
        self.outerjoin_tables[orm_table][orm_branch] = (orm_table.id == orm_branch.id)

    @reify
    def sql_options(self):
        return SQL_DBS[self.database_name]

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

    def lookup_table(self, value):
        tables = set()
        if isinstance(value, BinaryExpression):
            if isinstance(value.left, Column):
                tables.add(value.left.table)
            else:
                tables.update(self.lookup_table(value.left))

            if isinstance(value.right, Column):
                tables.add(value.right.table)
            else:
                tables.update(self.lookup_table(value.right))

        elif not isinstance(value, BindParameter):
            # @@TODO: Lookup for more tables?
            pass

        return tables

    def add_attribute(self, attribute, table_or_name=None):
        self.join(self.lookup_attribute(attribute, table_or_name))

    def lookup_attribute(self, attribute, table_or_name=None, all_in_one=False):
        options = QueryLookup(self.database_name)
        if hasattr(attribute, '__tablename__'):
            options.tables.add(attribute.__table__)
            options.attributes.append(attribute)

        elif isinstance(attribute, Table):
            options.tables.add(attribute)
            options.attributes.extend(attribute.c)

        elif isinstance(attribute, InstrumentedAttribute):
            options.tables.add(attribute.table)
            options.attributes.append(attribute)

        elif table_or_name is not None:
            table = self.get_table(table_or_name)
            if not isinstance(table_or_name, basestring):
                table_or_name = table.name

            if attribute is None:
                options.tables.add(table)
                options.attributes.extend(table.c)

            elif isinstance(attribute, basestring):
                if '.' in attribute:
                    child_table_or_name, attribute = attribute.split('.', 1)
                    if table_or_name == child_table_or_name:
                        options.join(self.lookup_attribute(attribute, child_table_or_name, all_in_one))
                    else:
                        options.join(self.lookup_attribute({child_table_or_name: attribute}, table, all_in_one))
                else:
                    options.join(self.lookup_attribute({attribute: None}, table, all_in_one))

            elif isinstance(attribute, dict):
                for maybe_attribute, attributes in attribute.items():
                    if maybe_attribute in table.c:
                        options.tables.add(table)
                        options.attributes.append(table.c[maybe_attribute])
                    elif maybe_attribute == 'active':
                        options.tables.add(table)
                        options.attributes.append(ACTIVE_MARKER)
                    else:
                        add_for_pos_queries = True
                        if not attributes:
                            orm_table = self.orm_tables[table.name]
                            if hasattr(orm_table, '__branches__'):
                                for orm_branch in orm_table.__branches__:
                                    branch_table = self.get_table(orm_branch)
                                    if maybe_attribute in branch_table.c:
                                        options.tables.add(table)
                                        options.add_outerjoin(orm_table, orm_branch)
                                        options.attributes.append(branch_table.c[maybe_attribute])
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
                                    options.secondary_parents[relation_name].append(attributes)
                                    options.secondary_parent_attributes[relation_name] = foreign.parent
                                    break
                            else:
                                options.secondary_children[relation_name].append(attributes)

            elif is_nonstr_iter(attribute):
                for maybe_attribute in attribute:
                    options.join(self.lookup_attribute(maybe_attribute, table, all_in_one))

            else:
                options.tables.add(table)
                options.attributes.append(attribute)
                options.tables.update(self.lookup_table(attribute))

        elif isinstance(attribute, basestring):
            if '.' in attribute:
                table_or_name, attribute = attribute.split('.', 1)
                options.join(self.lookup_attribute(attribute, table_or_name, all_in_one))
            else:
                options.join(self.lookup_attribute(None, attribute, all_in_one))

        elif isinstance(attribute, dict):
            for table_or_name, attributes in attribute.items():
                options.join(self.lookup_attribute(attributes, table_or_name, all_in_one))

        elif is_nonstr_iter(attribute):
            for maybe_attribute in attribute:
                options.join(self.lookup_attribute(maybe_attribute, None, all_in_one))

        else:
            options.tables.update(self.lookup_table(attribute))
            options.attributes.append(attribute)

        return options

    def add_filter(self, value):
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
                            self.add_filter({getattr(table.c, table_key): attribute_value})

                    else:
                        raise AttributeError('Invalid filter column: %s' % force_string(key))

                elif isinstance(key, InstrumentedAttribute):
                    self.tables.add(key.table)
                    self.queries.append(create_filter_by(key, values))

                elif hasattr(key, '__tablename__'):
                    if isinstance(values, dict):
                        for attribute, attribute_value in values.items():
                            self.add_filter({getattr(table, attribute): attribute_value})
                    else:
                        raise AttributeError('Invalid filter values: %s' % force_string(values))

                elif isinstance(key, Table):
                    if isinstance(values, dict):
                        for attribute, attribute_value in values.items():
                            self.add_filter({getattr(table.c, attribute): attribute_value})
                    else:
                        raise AttributeError('Invalid filter values: %s' % force_string(values))

                else:
                    raise AttributeError('Invalid filter column: %s' % force_string(key))

        elif isinstance(value, (tuple, list)):
            for deep_value in value:
                self.add_filter(deep_value)

        else:
            self.tables.update(self.lookup_table(value))
            self.queries.append(value)

    def lookup_order_by(self, attribute, table_or_name=None, descendant=False):
        options = OrderByLookup(descendant)
        if hasattr(attribute, '__tablename__'):
            options.tables.add(attribute.__table__)
            options.add_attribute(attribute.id)

        elif isinstance(attribute, Table):
            options.tables.add(attribute)
            options.add_attribute(attribute.c.id)

        if isinstance(attribute, InstrumentedAttribute):
            options.tables.add(attribute.table)
            options.add_attribute(attribute)

        elif table_or_name is not None:
            table = self.get_table(table_or_name)
            if not isinstance(table_or_name, basestring):
                table_or_name = table.name

            if attribute is None:
                options.tables.add(table)
                options.add_attribute(table.c.id)

            elif isinstance(attribute, OrderBy):
                options.join(self.lookup_order_by(attribute.column_name, table, attribute.descendant))

            elif isinstance(attribute, basestring):
                if '.' in attribute:
                    child_table_or_name, attribute = attribute.split('.', 1)
                    if child_table_or_name == table_or_name:
                        options.join(self.lookup_order_by(attribute, child_table_or_name, descendant))
                    else:
                        options.join(self.lookup_order_by({child_table_or_name: attribute}, table, descendant))
                else:
                    options.join(self.lookup_order_by({attribute: None}, table, descendant))

            elif isinstance(attribute, dict):
                for maybe_attribute, attributes in attribute.items():
                    if maybe_attribute in table.c:
                        options.tables.add(table)
                        options.add_attribute(table.c[maybe_attribute])
                    elif maybe_attribute == 'active':
                        options.tables.add(table)
                        options.add_attribute(ACTIVE_MARKER)
                    else:
                        add_for_pos_queries = True
                        if not attributes:
                            orm_table = self.orm_tables[table.name]
                            if hasattr(orm_table, '__branches__'):
                                for orm_branch in orm_table.__branches__:
                                    branch_table = self.get_table(orm_branch)
                                    if maybe_attribute in branch_table.c:
                                        options.tables.add(table)
                                        options.add_outerjoin(orm_table, orm_branch)
                                        options.add_attribute(branch_table.c[maybe_attribute])
                                        add_for_pos_queries = False
                                        break

                        if add_for_pos_queries:
                            if not maybe_attribute:
                                raise AttributeError('Need to define a table for children / parent queries')
                            options.join(self.lookup_order_by(attributes, maybe_attribute, descendant))

            elif is_nonstr_iter(attribute):
                for maybe_attribute in attribute:
                    options.join(self.lookup_order_by(maybe_attribute, table, descendant))

            else:
                options.tables.add(table)
                options.add_attribute(attribute)
                options.tables.update(self.lookup_table(attribute))

        elif isinstance(attribute, OrderBy):
            options.join(self.lookup_order_by(None, attribute.column_name, attribute.descendant))

        elif isinstance(attribute, basestring):
            if '.' in attribute:
                table_or_name, attribute = attribute.split('.', 1)
                options.join(self.lookup_order_by(attribute, table_or_name, descendant))
            else:
                options.join(self.lookup_order_by(None, attribute, descendant))

        elif isinstance(attribute, dict):
            for table_or_name, attributes in attribute.items():
                options.join(self.lookup_order_by(attributes, table_or_name, descendant))

        elif is_nonstr_iter(attribute):
            for maybe_attribute in attribute:
                options.join(self.lookup_order_by(maybe_attribute, None, descendant))

        else:
            options.tables.update(self.lookup_table(attribute))
            options.add_attribute(attribute)

        return options

    def add_order_by(self, *arguments, **kwargs):
        arguments = maybe_list(arguments)
        arguments.append(kwargs)
        descendant = kwargs.pop('descendant', False)

        for maybe_column in arguments:
            options = self.lookup_order_by(maybe_column, descendant=descendant)
            self.tables.update(options.tables)
            self.order_by.extend(options.attributes)
            for orm_table, orm_branches in options.outerjoin_tables.items():
                self.outerjoin_tables[orm_table].update(orm_branches)

    def add_secondary_order_by(self, **kwargs):
        for secondary_name, values in kwargs.items():
            self.secondary_children_order_by[secondary_name].append(values)

    def add_group_by(self, *arguments):
        for value in arguments:
            self.tables.update(self.lookup_table(value))
            self.group_by.append(value)

    def add_page(self, page, limit_per_page=20):
        self.page = maybe_integer(page) or 1
        if self.page < 1:
            self.page = 1

        self.limit_per_page = maybe_integer(limit_per_page) or 1
        if self.limit_per_page < 1:
            self.limit_per_page = 1


class OrderByLookup(object):
    def __init__(self, default_descendant=False):
        self.tables = set()
        self.attributes = []
        self.outerjoin_tables = MissingDict()
        self.default_descendant = default_descendant

    def join(self, value):
        self.tables.update(value.tables)
        self.attributes.extend(value.attributes)
        for orm_table, orm_branches in value.outerjoin_tables.items():
            self.outerjoin_tables[orm_table].update(orm_branches)

    def add_attribute(self, attribute, descendant=MARKER):
        if descendant is MARKER:
            descendant = self.default_descendant
        if descendant:
            attribute = attribute.desc()
        self.attributes.append(attribute)

    def add_outerjoin(self, orm_table, orm_branch):
        self.outerjoin_tables[orm_table][orm_branch] = (orm_table.id == orm_branch.id)
