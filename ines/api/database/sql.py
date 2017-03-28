# -*- coding: utf-8 -*-

from collections import defaultdict
from json import loads

from pyramid.decorator import reify
from pyramid.settings import asbool
from sqlalchemy import (
    and_, BigInteger, Boolean, create_engine, Date, DateTime, Enum, func, Integer, MetaData, not_, or_, SmallInteger,
    Unicode, UnicodeText)
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.exc import InternalError, OperationalError, ProgrammingError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.event import listen as sqlalchemy_listen
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.orm.util import AliasedClass
from sqlalchemy.pool import NullPool
from sqlalchemy.schema import DDL
from sqlalchemy.sql.selectable import Alias
from sqlalchemy.util._collections import lightweight_named_tuple

from ines import lazy_import_module, NOW
from ines.api import BaseSessionManager
from ines.api import BaseSession
from ines.api.database import SQL_DBS
from ines.api.database import SQL_ENGINES
from ines.api.database.filters import lookup_filter_builder
from ines.api.database.postgresql import POSTGRESQL_LOWER_AND_CLEAR
from ines.api.database.postgresql import postgresql_non_ascii_and_lower
from ines.api.database.postgresql import table_is_postgresql
from ines.api.database.utils import (
    build_sql_relations, get_active_column, get_active_filter, get_api_first_method, get_api_all_method,
    get_column_table_relations, get_inactive_filter, get_recursively_active_filters, get_recursively_tables,
    get_schema_table, get_table_backrefs, get_table_column, get_table_columns, maybe_table_schema,
    replace_response_columns, SQLPagination, table_entry_as_dict)
from ines.convert import maybe_date, maybe_datetime, maybe_integer, maybe_list, maybe_set, maybe_string
from ines.exceptions import Error
from ines.middlewares.repozetm import RepozeTMMiddleware
from ines.path import get_object_on_path
from ines.utils import NoneMaskObject, set_class_decorator, WrapperClass
from ines.views.fields import OrderBy

try:
    import cymysql
except ImportError:
    pass
else:
    cymysql.FIELD_TYPE.JSON = 245
    cymysql.converters.decoders[cymysql.FIELD_TYPE.JSON] = lambda data: maybe_string(data)


SQLALCHEMY_NOW_TYPE = type(func.now())
SQLALCHEMY_INT_LIMITS = {
    BigInteger: (-2**63, 2**63-1),
    Integer: (-2**31, 2**31-1),
    SmallInteger: (-2**15, 2**15-1),
    TINYINT: (0, 255),
}
SQLALCHEMY_CONVERT = {
    Boolean: asbool,
    Date: maybe_date,
    DateTime: maybe_datetime,
    Enum: maybe_string,
    Integer: maybe_integer,
    Unicode: maybe_string,
    UnicodeText: maybe_string,
}


class BaseSQLSessionManager(BaseSessionManager):
    __api_name__ = 'database'
    __middlewares__ = [RepozeTMMiddleware]

    @reify
    def __database_name__(self):
        return self.config.application_name

    def __init__(self, *args, **kwargs):
        super(BaseSQLSessionManager, self).__init__(*args, **kwargs)

        self.transaction = lazy_import_module('transaction')

        session_extension = self.settings.get('session_extension')
        if session_extension is not None:
            session_extension = get_object_on_path(session_extension)

        self.db_session = initialize_sql(
            self.__database_name__,
            self.settings['sql_path'],
            encoding=self.settings.get('encoding', 'utf8'),
            mysql_engine=self.settings.get('mysql_engine') or 'InnoDB',
            session_extension=session_extension,
            debug=asbool(self.settings.get('debug', False)),
            json_strict_decoder=asbool(self.settings.get('json_strict_decoder', True)))


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
        for key, column in get_schema_table(obj).columns.items():
            value = getattr(obj, key, None)
            if value is None and column.default:
                value = column.default.execute()

            if value is not None:
                values[key] = value

        return (
            get_schema_table(obj)
            .insert(values)
            .execute(autocommit=True))

    def direct_delete(self, obj, query):
        return bool(
            get_schema_table(obj)
            .delete(query)
            .execute(autocommit=True)
            .rowcount)

    def direct_update(self, obj, query, values):
        for key, column in get_schema_table(obj).columns.items():
            if key not in values and column.onupdate:
                values[key] = column.onupdate.execute()

        return (
            get_schema_table(obj)
            .update(query)
            .values(values)
            .execute(autocommit=True))

    # TODO move to static
    def table_instance_as_dict(self, instance):
        return {k: getattr(instance, k) for k in get_schema_table(instance).columns.keys()}

    # TODO delete
    def set_active_filter_on_query(self, query, table, active):
        if active:
            return query.filter(get_active_filter(table))
        else:
            return query.filter(get_inactive_filter(table))

    # TODO delete
    def _lookup_columns(self, table, attributes, active=None, active_tables=None, external=None, active_query=None):
        relate_with = set()
        if not attributes:
            columns = list(get_schema_table(table).columns.values())

            if active_tables:
                if active is None:
                    relate_with.update(t.__tablename__ for t in maybe_list(active_tables))

                column = get_active_column(table, active=active)[0]
                if active_query:
                    column = and_(active_query, column)
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
                if isinstance(attribute, str):
                    column = getattr(table, attribute, None)
                else:
                    column = attribute

                if column is not None:
                    relate_with.update(t.name for t in get_object_tables(column))
                    columns.append(column)

                elif active_tables and attribute == 'active':
                    if active is None:
                        relate_with.update(t.__tablename__ for t in maybe_list(active_tables))

                    column = get_active_column(table, active=active)[0]
                    if active_query:
                        column = and_(active_query, column)
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

    # TODO delete
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
                column_name = attribute[:-12]
            else:
                is_like = attribute[-8:] == '_is_like'
                if is_like:
                    column_name = attribute[:-8]
                else:
                    is_ilike = attribute[-9:] == '_is_ilike'
                    if is_ilike:
                        column_name = attribute[:-9]
                    else:
                        is_none = attribute[-8:] == '_is_none'
                        if is_none:
                            column_name = attribute[:-8]
                        else:
                            column_name = attribute

            column = getattr(table, column_name, None)
            if column is not None:
                if is_not_none:
                    sa_filter = column.isnot(None)
                elif is_like:
                    from ines.api.database.filters import like_maybe_with_none
                    sa_filter, xxx = like_maybe_with_none(table, column_name, value)
                elif is_ilike:
                    from ines.api.database.filters import ilike_maybe_with_none
                    sa_filter, xxx = ilike_maybe_with_none(table, column_name, value)
                elif is_none:
                    sa_filter = column.is_(None)
                else:
                    from ines.api.database.filters import default_filter_builder
                    sa_filter, xxx = default_filter_builder(table, column_name, value)

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

    # TODO delete
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

                if table_is_postgresql(table):
                    column = postgresql_non_ascii_and_lower(column, as_text=False)

                if as_desc:
                    order_by.append(column.desc())
                else:
                    order_by.append(column)

            elif active_tables and attribute == 'active':
                if active is None:
                    relate_with.update(t.__tablename__ for t in maybe_list(active_tables))

                column = get_active_column(table, active=active)[0]
                if active_query:
                    column = and_(active_query, column)
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

    # TODO move to static
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

    # TODO static?
    def create_global_search_options(self, search, tables):
        response = []
        for table in maybe_list(tables):
            ignore_columns = getattr(table, '__ignore_on_global_search__', None)
            ignore_ids = getattr(table, '__ignore_ids_on_global_search__', True)

            for column_name in table._sa_class_manager.local_attrs.keys():
                if ignore_ids:
                    if column_name == 'id' or column_name.endswith('_id'):
                        continue
                elif ignore_columns and column_name in ignore_columns:
                    continue

                from ines.api.database.filters import create_like_filter
                global_search_filter, xxx = create_like_filter(table, column_name, search)
                if global_search_filter is not None:
                    response.append(global_search_filter)

        return response

    # TODO move to static
    def fill_response_with_indexs(self, indexs, references, response):
        return fill_response_with_indexs(indexs, references, response)


class SetSQLPosColumns(WrapperClass):
    def __call__(self, cls, **kwargs):
        kwargs['return_pos_columns_index'] = True
        response, pos_columns_index = self.wrapped(cls, **kwargs)
        if not pos_columns_index or not response:
            return response

        unique_pos_columns_index = defaultdict(lambda: defaultdict(list))
        for i, (pos_columns_type, column) in pos_columns_index.items():
            unique_pos_columns_index[pos_columns_type][column].append(i)

        references = {}
        indexes = defaultdict(list)

        for pos_columns_type, columns in unique_pos_columns_index.items():
            for column, column_indexes in columns.items():
                response_ids = set(r[i] for i in column_indexes for r in response)
                if None in response_ids:
                    response_ids.remove(None)
                if not response_ids:
                    continue

                if pos_columns_type == 'length':
                    indexes[column].extend(column_indexes)
                    references[column] = defaultdict(int)
                    references[column].update(cls.api.session
                        .query(column, func.count(column.table.columns['id']))
                        .filter(column.in_(response_ids))
                        .group_by(column)
                        .all())
                else:
                    raise AttributeError('Invalid pos column index type "%s"' % pos_columns_type)

        replace_response_columns(indexes, references, response)
        return response


class SetSQLPagination(WrapperClass):
    def __init__(self, wrapped, count_column):
        super(SetSQLPagination, self).__init__(wrapped)
        self.count_column = count_column

    def __call__(self, *args, **kwargs):
        page = kwargs.pop('page', None)
        limit_per_page = kwargs.pop('limit_per_page', None)
        return_query = kwargs.pop('return_query', False)
        only_one = kwargs.pop('only_one', False)
        return_pos_columns_index = kwargs.get('return_pos_columns_index', False)

        if return_pos_columns_index:
            query, flat_positions, pos_columns_index = self.wrapped(*args, **kwargs)
        else:
            query, flat_positions = self.wrapped(*args, **kwargs)

        if return_query:
            response = query
        elif only_one:
            response = query.first()
        elif page is not None:
            response = SQLPagination(query, page, limit_per_page or 20, count_column=self.count_column)
        elif limit_per_page:
            response = query.slice(0, limit_per_page).all()
        else:
            response = query.all()

        if response and not return_query:
            if only_one:
                response = [response]

            named_tuples = {}
            fields = list(response[0]._real_fields)
            for name, start, end in reversed(flat_positions):
                named_tuples[name] = lightweight_named_tuple('result', fields[start:end])
                fields[start:end] = [name]

            named_tuple = lightweight_named_tuple('result', fields)

            for i, value in enumerate(response):
                new_value = list(value)
                for name, start, end in reversed(flat_positions):
                    child_value = named_tuples[name](new_value[start:end])
                    new_value[start:end] = [child_value.id is not None and child_value or None]
                response[i] = named_tuple(new_value)

            if only_one:
                response = response[0]

        if return_pos_columns_index:
            return response, pos_columns_index
        else:
            return response


class ParseSQLOrm(WrapperClass):
    def __init__(self, wrapped, orm_table, default_order_by=None):
        super(ParseSQLOrm, self).__init__(wrapped)
        self.orm_table = orm_table
        self.default_order_by = default_order_by

    def __call__(self, cls, **kwargs):
        kwargs['active'] = kwargs.get('active', True)
        filters = kwargs.copy()
        attributes = filters.pop('attributes', None)
        order_by = filters.pop('order_by', self.default_order_by)
        group_by = filters.pop('group_by', None)
        return_pos_columns_index = filters.pop('return_pos_columns_index', False)

        columns, related_tables, pos_columns_index, flat_positions = lookup_sql_columns(
            table=self.orm_table,
            attributes=attributes,
            active=kwargs['active'])

        sa_filters, filters_related_tables = lookup_sql_filters(self.orm_table, filters)
        related_tables.update(filters_related_tables)

        sa_order_by, order_by_related_tables = lookup_sql_order_by(self.orm_table, order_by, kwargs['active'])
        related_tables.update(order_by_related_tables)

        # Build query
        query = cls.api.session.query(*columns)

        # Set relations
        related_tables.remove(self.orm_table.__table__)
        if related_tables:
            related_filters, outer_joins = build_sql_relations(self.orm_table, related_tables)
            if related_tables:
                raise ValueError(
                    'Cant find relations for tables %s on table %s'
                    % ([t.name for t in related_tables], self.orm_table.__tablename__))

            if outer_joins:
                import pdb; pdb.set_trace()
                query = query.select_from(outer_joins[0])
                for outer_join_table, outer_join_on in outer_joins[1:]:
                    query = query.outerjoin(outer_join_table, outer_join_on)

            if related_filters:
                query = query.filter(*related_filters)

        if sa_filters:
            query = query.filter(*sa_filters)

        if sa_order_by:
            query = query.order_by(*sa_order_by)

        if group_by is not None:
            query = query.group_by(*maybe_list(group_by))

        query = self.wrapped(
            cls,
            query,
            columns=columns,
            filters=sa_filters,
            order_by=sa_order_by,
            related_tables=related_tables,
            pos_columns_index=pos_columns_index,
            kwargs=kwargs)

        print(query)

        if return_pos_columns_index:
            return query, flat_positions, pos_columns_index
        else:
            return query, flat_positions


class SetCount(WrapperClass):
    def __init__(self, wrapped, group_by, count_by, count_attribute='length'):
        super(SetCount, self).__init__(wrapped)
        self.group_by = group_by
        self.count_by = count_by
        self.count_attribute = count_attribute

    def __call__(self, cls, *args, **kwargs):
        sent_attributes = 'attributes' in kwargs
        attributes = maybe_list(kwargs.pop('attributes', None))
        attributes.append(func.count(self.count_by).label(self.count_attribute))

        response = self.wrapped(cls, attributes=attributes, group_by=self.group_by, **kwargs)
        if sent_attributes:
            return response
        elif isinstance(response, list):
            return response and sum(r[-1] for r in response) or 0
        else:
            return response and response[-1] or 0


class SetSQLBase(WrapperClass):
    activity_action = None
    missing_message = 'O item que não existe'

    def __init__(self, wrapped, orm_table, activity_tables=None):
        super(SetSQLBase, self).__init__(wrapped)
        self.orm_table = orm_table
        self.activity_tables = maybe_set(activity_tables)

    @reify
    def table(self):
        return get_schema_table(self.orm_table)

    @reify
    def activity_tables_as_set(self):
        return set(get_schema_table(t) for t in maybe_set(self.activity_tables))

    @reify
    def related_tables(self):
        return get_recursively_tables(self.table)

    @reify
    def related_tables_names(self):
        return ['%s_id' % t.name for t in self.related_tables if t not in self.activity_tables_as_set]

    @reify
    def attributes(self):
        response = list(get_table_columns(self.table))
        response.append('active')
        response.extend(self.activity_tables_as_set)
        response.extend(
            t.columns['id'].label('%s_id' % t.name)
            for t in self.related_tables
            if t not in self.activity_tables_as_set)
        return response

    def build_activity_info(self, response):
        activity_ids = {}

        if hasattr(response, '_asdict'):
            data = response._asdict()
        else:
            data = table_entry_as_dict(response)
            for key in getattr(response, '_activity_fields', []):
                data[key] = getattr(response, key)

        for activity_table in self.activity_tables_as_set:
            activity_item = data.pop(activity_table.name)
            if activity_item is None:
                data[activity_table.name] = None
            else:
                data[activity_table.name] = activity_item._asdict()
                activity_ids['%s_id' % activity_table.name] = activity_item.id

        for related_table_name_id in self.related_tables_names:
            activity_ids[related_table_name_id] = getattr(response, related_table_name_id)

        return activity_ids, data, response

    def before_wrapped(self, cls, key, **kwargs):
        response = get_api_first_method(cls.api, self.orm_table)(key=key, attributes=self.attributes)
        if not response:
            raise Error('key', self.missing_message)
        else:
            return response

    def call_wrapped(self, cls, response, kwargs):
        method_kwargs = kwargs.copy()
        method_kwargs.update((t.name, getattr(response, t.name)) for t in self.activity_tables_as_set)
        method_kwargs.update({self.table.name: response})
        return self.wrapped(cls, **method_kwargs)

    def after_wrapped(self, cls, response):
        raise NotImplemented

    def sql_action(self, cls, response):
        raise NotImplemented

    def __call__(self, cls, *args, **kwargs):
        response = self.before_wrapped(cls, *args, **kwargs)
        callback = self.call_wrapped(cls, response, kwargs)
        if self.after_wrapped(cls, response):
            return callback()

        self.sql_action(cls, response)
        cls.api.session.flush()

        activity_kwargs, data, item = self.build_activity_info(response)
        activity_kwargs[self.activity_action == 'delete' and 'previous_data' or 'data'] = data
        cls.api.add_activity(self.table, action=self.activity_action, type_id=item.id, **activity_kwargs)

        return callback()


class SetDelete(SetSQLBase):
    activity_action = 'delete'
    missing_message = 'O item que pretende apagar não existe'

    def __init__(self, wrapped, orm_table, activity_tables=None, primary_order_by=None):
        super(SetDelete, self).__init__(wrapped, orm_table, activity_tables)
        self.primary_order_by = primary_order_by

    def after_wrapped(self, cls, item):
        for backref_foreign_key in get_table_backrefs(self.table):
            backref = (
                cls.api.session
                .query(func.count(backref_foreign_key.parent.table.columns['id']).label('length'))
                .filter(backref_foreign_key.parent == item.id)
                .group_by(backref_foreign_key.parent)
                .first())
            if backref and backref.length:
                if backref.length == 1:
                    message = 'Não pode eliminar o item porque tem %s filho associado'
                else:
                    message = 'Não pode eliminar o item porque tem %s filhos associados'
                raise Error('key', message % backref.length)

    def sql_action(self, cls, item):
        if self.primary_order_by:
            # Need to set another item as primary
            if item.is_primary:
                method = get_api_first_method(cls.api, self.orm_table)
                primary_kwargs = {
                    foreign_key: getattr(item, foreign_key)
                    for foreign_key in get_primary_relations(self.orm_table).keys()}

                first_item = method(
                    id_is_different=item.id,
                    attributes=['id'],
                    active=None,
                    order_by=self.primary_order_by,
                    **primary_kwargs)

                if first_item:
                    (cls.api.session
                        .query(self.orm_table.id)
                        .filter(self.orm_table.id == first_item.id)
                        .update({self.orm_table.is_primary: True}, synchronize_session=False))

        (cls.api.session
            .query(self.table.columns['id'])
            .filter(self.table.columns['id'] == item.id)
            .delete(synchronize_session=False))


class SetPrimary(SetSQLBase):
    activity_action = 'set_primary'
    missing_message = 'O item que pretende definir como primário não existe'

    def after_wrapped(self, cls, item):
        if item.is_primary:
            return True

    def sql_action(self, cls, item):
        # Set all of same relation as non primary
        clear_primary_relations(cls.api.session, self.orm_table, item)

        # Set as primary
        (cls.api.session
            .query(self.orm_table.id)
            .filter(self.orm_table.id == item.id)
            .update({self.orm_table.is_primary: True}, synchronize_session=False))


class SetAdd(SetSQLBase):
    activity_action = 'add'

    def __init__(self, wrapped, orm_table, columns, activity_tables=None, **kwargs):
        super(SetAdd, self).__init__(wrapped, orm_table, activity_tables)
        self.primary_attribute = kwargs.get('primary_attribute')
        self.primary_existing_message = kwargs.get('primary_existing_message', 'O valor está registado noutro tipo')

        self.columns = {}
        if not isinstance(columns, dict):
            columns = {k: None for k in columns}

        # pre validations
        for key, options in columns.items():
            column = getattr(orm_table, key)
            self.columns[key] = options = options or {}

            options.update({
                'column': column,
                'convert': SQLALCHEMY_CONVERT[type(column.type)],
                'nullable': options.get('nullable', column.nullable),
            })

            if isinstance(column.type, (Unicode, UnicodeText)):
                self.set_limit('length_limit', options, 1, column.type.length)
            elif isinstance(column.type, Integer):
                self.set_limit('integer_limit', options, *SQLALCHEMY_INT_LIMITS[type(column.type)])
            elif isinstance(column.type, Enum):
                options['in'] = options.get('in', column.type.enums)

    def set_limit(self, key, options, min_limit, max_limit):
        if key not in options:
            min_defined, max_defined = min_limit, max_limit
        else:
            min_defined, max_defined = options[key]
            if min_defined is None:
                min_defined = min_limit
            elif min_defined < min_limit:
                raise ValueError('Invalid min integer for %s (max:%s)' % (key, min_limit))

            if max_defined is None:
                max_defined = max_limit
            elif max_defined > max_limit:
                raise ValueError('Invalid max integer for %s (max:%s)' % (key, max_limit))

        options[key] = (min_defined, max_defined)

    @reify
    def foreign_related_tables(self):
        response = {}
        for foreign_key in self.table.foreign_keys:
            foreign_table = foreign_key.column.table
            response[foreign_table] = get_recursively_tables(foreign_table)
        return response

    def validate_limits(self, key, type_message, value, min_limit, max_limit):
        if min_limit is not None and value < min_limit:
            raise Error(key, '%s deve ser maior ou igual a %s' % (type_message, min_limit))
        elif max_limit is not None and value > max_limit:
            raise Error(key, '%s deve ser menor ou igual a %s' % (type_message, max_limit))

    def validate_if_null(self, options, key, value, kwargs):
        if value is None:
            if kwargs.get(key) is not None:
                raise Error(key, 'Inválido')
            elif not options['nullable']:
                raise Error(key, 'Obrigatório')
            else:
                return True

    def validate(self, options, key, value, kwargs):
        # if returns False, value is None
        if self.validate_if_null(options, key, value, kwargs):
            return False

        allowed = options.get('in')
        if allowed and value not in allowed:
            raise Error(key, 'Valor "%s" inválido. Deve escolher entre "%s"' % (value, '", "'.join(allowed)))
        if 'integer_limit' in options:
            self.validate_limits(key, 'O valor', value, *options['integer_limit'])
        if 'length_limit' in options:
            self.validate_limits(key, 'O tamanho', len(value), *options['length_limit'])

        return True

    def before_wrapped(self, cls, **kwargs):
        item = self.orm_table()
        item._activity_fields = set()

        # Validate
        foreign_tables_to_validate = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
        for key, options in self.columns.items():
            column = options['column']
            value = kwargs.get(key)

            if column.foreign_keys:
                for foreign_key in column.foreign_keys:
                    foreign_table = foreign_key.column.table
                    foreign_column_key = '%s_key' % foreign_table.name
                    foreign_value = kwargs.get(foreign_column_key)
                    if foreign_value is not None:
                        if value is not None:
                            raise KeyError('Envie apenas uma chave "%s" ou "%s"' % (foreign_column_key, key))
                        self.validate_if_null(options, foreign_column_key, foreign_value, kwargs)
                        foreign_tables_to_validate[foreign_table]['key'][foreign_value].add(key)
                    else:
                        self.validate(options, key, value, kwargs)
                        foreign_tables_to_validate[foreign_table]['id'][value].add(key)

            elif self.validate(options, key, value, kwargs):
                setattr(item, key, value)

        # Validate foreign keys
        for foreign_table, foreign_columns in foreign_tables_to_validate.items():
            attributes = []
            foreign_related_tables = set(self.foreign_related_tables[foreign_table])
            foreign_related_tables.add(foreign_table)
            for foreign_related_table in foreign_related_tables:
                if foreign_related_table in self.activity_tables_as_set:
                    attributes.append(foreign_related_table)
                else:
                    attributes.append(foreign_related_table.columns['id'].label('%s_id' % foreign_related_table.name))

            method = get_api_all_method(cls.api, foreign_table)
            for foreign_column_key, foreign_keys in foreign_columns.items():
                foreign_column_keys = set(foreign_keys.keys())
                with_none_in_keys = None in foreign_column_keys
                if with_none_in_keys:
                    foreign_column_keys.remove(None)

                if foreign_column_keys:
                    foreign_items = method(
                        attributes=[foreign_table.columns['id'].label('id'), foreign_column_key] + attributes,
                        **{foreign_column_key: foreign_column_keys})
                else:
                    foreign_items = [NoneMaskObject()]

                references = {}
                for foreign_item in foreign_items:
                    references[getattr(foreign_item, foreign_column_key)] = foreign_item.id

                    for attribute in attributes:
                        if not isinstance(attribute, str):
                            setattr(item, attribute.name, getattr(foreign_item, attribute.name))
                            item._activity_fields.add(attribute.name)
                        else:
                            setattr(item, attribute, getattr(foreign_item, attribute))
                            item._activity_fields.add(attribute)

                for value, keys in foreign_keys.items():
                    if value is not None:
                        relation_item_id = references.get(value)
                        if relation_item_id is None:
                            raise Error(list(keys)[0], 'O valor não existe')
                    else:
                        relation_item_id = None

                    for inner_key in keys:
                        setattr(item, inner_key, relation_item_id)
                        item._activity_fields.add(inner_key)

        return item

    def sql_action(self, cls, item):
        cls.api.session.add(item)

    def after_wrapped(self, cls, item):
        if self.primary_attribute:
            method = get_api_all_method(cls.api, self.orm_table)
            primary_kwargs = {
                foreign_key: getattr(item, foreign_key)
                for foreign_key in get_primary_relations(self.orm_table).keys()}

            existing_primaries = set(
                getattr(c, self.primary_attribute)
                for c in method(attributes=[self.primary_attribute], active=None, **primary_kwargs))
            if getattr(item, self.primary_attribute) in existing_primaries:
                raise Error(self.primary_attribute, self.primary_existing_message)

            if not existing_primaries:
                item.is_primary = True
            elif item.is_primary:
                clear_primary_relations(cls.api.session, self.orm_table, item)


def get_primary_relations(table):
    return {foreign_key.parent.name: foreign_key.parent for foreign_key in get_schema_table(table).foreign_keys}


def clear_primary_relations(session, orm_table, item, getattribute=None):
    if getattribute is None:
        getattribute = isinstance(item, dict) and '__getitem__' or '__getattribute__'

    query = session.query(orm_table.id)
    for foreign_key, foreign_column in get_primary_relations(orm_table).items():
        query = query.filter(foreign_column == getattr(item, getattribute)(foreign_key))
    return query.update({orm_table.is_primary: False}, synchronize_session=False)


set_sql_pos_columns = set_class_decorator(SetSQLPosColumns)
set_sql_pagination = set_class_decorator(SetSQLPagination)
parse_sql_orm = set_class_decorator(ParseSQLOrm)
set_count = set_class_decorator(SetCount)
set_add = set_class_decorator(SetAdd)
set_primary = set_class_decorator(SetPrimary)
set_delete = set_class_decorator(SetDelete)


def lookup_sql_columns(table, attributes, active, ignore_label_definition=False):
    columns = []
    related_tables = set()
    pos_columns_index = {}
    table = get_schema_table(table)
    flat_positions = []

    if not attributes:
        columns.extend(get_table_columns(table))
        related_tables.add(table)

        active_column, related_active_tables = get_active_column(table, active)
        related_tables.update(related_active_tables)
        columns.append(active_column)
    else:
        missing_attributes_index = {}
        backrefs = {b.parent.table.name: b for b in get_table_backrefs(table)}

        for columns_index, attribute in enumerate(maybe_list(attributes)):
            if not isinstance(attribute, str):
                attribute_table = maybe_table_schema(attribute)
                if attribute_table is not None:
                    # We can have tables defined, lets extend table columns
                    current_position = len(columns)
                    columns.extend(get_table_columns(attribute_table))
                    flat_positions.append((attribute_table.name, current_position, len(columns)))
                    related_tables.add(attribute_table)
                else:
                    columns.append(attribute)
                    related_tables.update(get_column_table_relations(attribute))
            elif attribute == 'active':
                active_column, related_active_tables = get_active_column(table, active)
                related_tables.update(related_active_tables)
                columns.append(active_column)
            else:
                column = get_table_column(table, attribute, None)
                if column is not None:
                    columns.append(column)
                    related_tables.add(table)
                else:
                    length_parts = attribute.rsplit('_length', 1)
                    backref_foreign_key = len(length_parts) == 2 and backrefs.get(length_parts[0]) or None
                    if backref_foreign_key is not None:
                        columns.append(backref_foreign_key.column.label(attribute))
                        pos_columns_index[columns_index] = ('length', backref_foreign_key.parent)
                    else:
                        columns.append(attribute)
                        missing_attributes_index[columns_index] = attribute

        # TODO add table alias

        for foreign_column in table.foreign_keys:
            if not missing_attributes_index:
                break

            foreign_table = foreign_column.column.table
            if foreign_table == table:
                continue

            foreign_table_name_patterns = ['%s_' % foreign_table.name]

            foreign_attributes = []
            for columns_index, attribute in missing_attributes_index.items():
                for pattern in foreign_table_name_patterns:
                    if attribute.startswith(pattern):
                        foreign_attributes.append((columns_index, attribute.split(pattern, 1)[1]))

            if foreign_attributes:
                (foreign_columns,
                 foreign_related_tables,
                 foreign_pos_columns_index,
                 foreign_flat_positions) = lookup_sql_columns(
                    table=foreign_table,
                    attributes=[k[1] for k in foreign_attributes],
                    active=active,
                    ignore_label_definition=ignore_label_definition)

                pos_columns_index.update(foreign_pos_columns_index)
                flat_positions.extend(foreign_flat_positions)

                if foreign_columns:
                    related_tables.update(foreign_related_tables)

                    for i, maybe_foreign_column in enumerate(foreign_columns):
                        if not isinstance(maybe_foreign_column, str):
                            columns_index = foreign_attributes[i][0]
                            attribute = missing_attributes_index.pop(columns_index)
                            if not ignore_label_definition:
                                columns[columns_index] = maybe_foreign_column.label(attribute)
                            else:
                                columns[columns_index] = maybe_foreign_column

    return columns, related_tables, pos_columns_index, flat_positions


def lookup_sql_filters(table, filters_dict):
    filters = []
    related_tables = set()
    table = get_schema_table(table)

    for attribute, value in list(filters_dict.items()):
        as_not_filter = attribute.startswith('not_')
        column_name = not as_not_filter and attribute or attribute.split('not_', 1)[1]

        sa_filter = None
        if column_name == 'active':
            filters_dict.pop(attribute)
            if value is not None:
                active_tables, active_filters = get_recursively_active_filters(table)
                if active_filters:
                    related_tables.update(active_tables)
                    sa_filter = and_(*active_filters)
        else:
            column_name, builder = lookup_filter_builder(column_name)
            column = get_table_column(table, column_name, None)
            if column is not None:
                filters_dict.pop(attribute)
                sa_filter, filter_related_tables = builder(table, column_name, value)
                related_tables.update(filter_related_tables)

        if sa_filter is not None:
            if as_not_filter:
                filters.append(not_(sa_filter))
            else:
                filters.append(sa_filter)

    # TODO add table alias

    for foreign_column in table.foreign_keys:
        if not filters_dict:
            break

        foreign_table = foreign_column.column.table
        foreign_table_name_patterns = ['%s_' % foreign_table.name]

        foreign_filters = {}
        foreign_filters_references = {}
        for attribute, value in filters_dict.items():
            for pattern in foreign_table_name_patterns:
                if attribute.startswith(pattern):
                    foreign_attribute = attribute.split(pattern, 1)[1]
                    foreign_filters[foreign_attribute] = value
                    foreign_filters_references[foreign_attribute] = attribute

        if foreign_filters:
            foreign_filters, foreign_related_tables = lookup_sql_filters(
                table=foreign_table,
                filters_dict=foreign_filters)

            if foreign_filters:
                filters.extend(foreign_filters)
                related_tables.update(foreign_related_tables)

                for foreign_attribute, attribute in foreign_filters_references.items():
                    if foreign_attribute not in foreign_filters:
                        filters_dict.pop(attribute)

    if filters_dict:
        raise AttributeError('Missing filters keys "%s"' % '", "'.join(filters_dict.keys()))
    else:
        return filters, related_tables


def lookup_sql_order_by(table, attributes, active):
    columns = []
    related_tables = set()
    table = get_schema_table(table)
    missing_attributes_index = {}

    for columns_index, attribute in enumerate(maybe_list(attributes)):
        if not isinstance(attribute, str):
            columns.append(attribute)
            related_tables.update(get_column_table_relations(attribute))
        else:
            if isinstance(attribute, OrderBy):
                as_descendant = attribute.descendant
                column_name = attribute.column_name
                attribute = as_descendant and ('%s desc' % column_name) or column_name
            elif attribute.lower().endswith(' desc'):
                as_descendant = True
                column_name = attribute[:-5]
            elif attribute.lower().endswith(' asc'):
                as_descendant = False
                column_name = attribute[:-4]
            else:
                as_descendant = False
                column_name = attribute

            if column_name == 'active':
                active_column, related_active_tables = get_active_column(table, active)
                related_tables.update(related_active_tables)
                if as_descendant:
                    columns.append(active_column.desc())
                else:
                    columns.append(active_column)
            else:
                column = get_table_column(table, column_name, None)
                if column is not None:
                    related_tables.add(table)
                    if table_is_postgresql(table):
                        column = postgresql_non_ascii_and_lower(column, as_text=False)
                    if as_descendant:
                        columns.append(column.desc())
                    else:
                        columns.append(column)
                else:
                    columns.append(attribute)
                    missing_attributes_index[columns_index] = attribute

    # TODO add table alias

    for foreign_column in table.foreign_keys:
        if not missing_attributes_index:
            break

        foreign_table = foreign_column.column.table
        if foreign_table == table:
            continue

        foreign_table_name_patterns = ['%s_' % foreign_table.name]

        foreign_attributes = []
        for columns_index, attribute in missing_attributes_index.items():
            for pattern in foreign_table_name_patterns:
                if attribute.startswith(pattern):
                    foreign_attributes.append((columns_index, attribute.split(pattern, 1)[1]))

        if foreign_attributes:
            foreign_columns, foreign_related_tables = lookup_sql_order_by(
                table=foreign_table,
                attributes=[k[1] for k in foreign_attributes],
                active=active)

            if foreign_columns:
                related_tables.update(foreign_related_tables)

                for i, maybe_foreign_column in enumerate(foreign_columns):
                    if not isinstance(maybe_foreign_column, str):
                        columns_index = foreign_attributes[i][0]
                        missing_attributes_index.pop(columns_index)
                        columns[columns_index] = maybe_foreign_column

    return columns, related_tables
















def fill_response_with_indexs(replace_indexes, references, response):
    named_tuple = lightweight_named_tuple('result', list(response[0]._real_fields))

    for index, sqlobj in enumerate(response):
        new_sqlobj = list(sqlobj)
        for attribute, indexes in replace_indexes.items():
            for i in indexes:
                value = getattr(sqlobj, attribute)
                new_sqlobj[i] = references[attribute][value]

        response[index] = named_tuple(new_sqlobj)


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
        debug=False,
        json_strict_decoder=True,
    ):

    sql_path = '%s?charset=%s' % (sql_path, encoding)
    SQL_DBS[application_name]['sql_path'] = sql_path
    is_mysql = sql_path.lower().startswith('mysql')

    if is_mysql:
        if 'bases' in SQL_DBS[application_name]:
            for base in SQL_DBS[application_name]['bases']:
                append_arguments(base, 'mysql_charset', encoding)

    metadata = SQL_DBS[application_name].get('metadata')

    if metadata:
        connection_type = None

        # Set defaults for MySQL tables
        if is_mysql:
            connection_type = 'mysql'
            for table in metadata.sorted_tables:
                append_arguments(table, 'mysql_engine', mysql_engine)
                append_arguments(table, 'mysql_charset', encoding)

        # Add some postgresql functions
        if sql_path.lower().startswith('postgresql'):
            connection_type = 'postgresql'
            sqlalchemy_listen(metadata, 'before_create', DDL(POSTGRESQL_LOWER_AND_CLEAR))

        for table in metadata.sorted_tables:
            table.__connection_type__ = connection_type

    engine_kwargs = {}
    engine_pattern = '%s-%s' % (sql_path, encoding)
    if not json_strict_decoder:
        engine_kwargs['json_deserializer'] = lambda value: loads(value, strict=False)
        engine_pattern += '-json-decoder'

    if engine_pattern in SQL_ENGINES:
        engine = SQL_ENGINES[engine_pattern]
    else:
        SQL_ENGINES[engine_pattern] = engine = create_engine(
            sql_path,
            echo=debug,
            poolclass=NullPool,
            encoding=encoding,
            **engine_kwargs)
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
                    except InternalError as error:
                        if error.orig.errno == 1061:
                            # Duplicated key
                            continue
                        raise

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


def date_in_period_filter(table, start_date, end_date):
    return or_(
        and_(table.start_date.is_(None), table.end_date.is_(None)),
        not_(or_(table.end_date < start_date, table.start_date > end_date)))


def new_lightweight_named_tuple(response, *new_fields):
    return lightweight_named_tuple('result', response._real_fields + tuple(new_fields))

def resolve_database_value(value):
    if isinstance(value, SQLALCHEMY_NOW_TYPE):
        return NOW()
    else:
        return value
