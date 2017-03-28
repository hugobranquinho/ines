# -*- coding: utf-8 -*-

from colander import drop
from pyramid.compat import is_nonstr_iter
from sqlalchemy import and_
from sqlalchemy import cast
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import Numeric
from sqlalchemy import or_
from sqlalchemy import String

from ines.api.database.postgresql import postgresql_non_ascii_and_lower
from ines.api.database.postgresql import table_is_postgresql
from ines.api.database.utils import get_column_table_relations
from ines.api.database.utils import get_table_column
from ines.cleaner import clean_string
from ines.convert import maybe_set
from ines.exceptions import Error
from ines.views.fields import FilterBy


FILTER_BUILDER = []


def filter_query_with_queries(queries, query=None, join_with='or'):
    """Filter 'query' with none/single/multiple OR'ed queries"""
    queries = [q for q in queries if q is not None]
    if len(queries) == 1:
        query_filter = queries[0]
    elif not queries:
        return query
    elif join_with == 'and':
        query_filter = and_(*queries)
    else:
        query_filter = or_(*queries)

    if query is None:
        return query_filter
    elif query_filter is not None:
        return query.filter(query_filter)
    else:
        return query


def prepare_column_for_filter(table, column_name, values, filter_name, query=None, accept_none=False, **kwargs):
    sa_filter = None
    related_tables = set()
    column = get_table_column(table, column_name)
    values = maybe_set(values)

    # Clear values
    with_none = None in values
    if with_none:
        values.remove(None)

    if values:
        if not hasattr(column, 'type'):
            clauses = getattr(column, 'clauses', None)
            if clauses is not None:
                related_tables.update(get_column_table_relations(clause) for clause in clauses)
                column = func.concat(*clauses)
            else:
                related_tables.update(get_column_table_relations(column))
                column = cast(column, String)
    
        elif isinstance(column.type, (Numeric, Integer, Date, DateTime)):
            related_tables.update(get_column_table_relations(column))
            column = cast(column, String)
    
        else:
            related_tables.update(get_column_table_relations(column))
    
        # Prepare for postgresql
        if table_is_postgresql(table):
            values = {clean_string(i).lower() for i in values}
            column = postgresql_non_ascii_and_lower(column)
    
        if filter_name == 'like':
            queries = [column.like('%{like}%'.format(like='%'.join(i.split()))) for i in values]
            sa_filter = filter_query_with_queries(queries, **kwargs)
        elif filter_name == 'ilike':
            queries = [column.ilike('%{like}%'.format(like='%'.join(i.split()))) for i in values]
            sa_filter = filter_query_with_queries(queries, **kwargs)
        elif filter_name == 'rlike':
            queries = [column.rlike('(%s)' % '|'.join(i.split())) for i in values]
            sa_filter = filter_query_with_queries(queries, **kwargs)
        elif filter_name == 'in':
            sa_filter = column.in_(values)
        else:
            queries = [column.op(filter_name)(i) for i in values]
            sa_filter = filter_query_with_queries(queries, **kwargs)

    if accept_none and with_none:
        if sa_filter is None:
            sa_filter = column.is_(None)
        else:
            sa_filter = filter_query_with_queries([sa_filter, column.is_(None)], **kwargs)

    if query is None:
        return sa_filter, related_tables
    elif sa_filter is not None:
        return query.filter(sa_filter)
    else:
        return query


def create_like_filter(table, column_name, values, **kwargs):
    return prepare_column_for_filter(table, column_name, values, filter_name='like', **kwargs)


def create_ilike_filter(table, column_name, values, **kwargs):
    return prepare_column_for_filter(table, column_name, values, filter_name='ilike', **kwargs)


def create_rlike_filter(table, column_name, values, **kwargs):
    return prepare_column_for_filter(table, column_name, values, filter_name='rlike', **kwargs)


def maybe_with_none(table, column_name, values, **kwargs):
    return prepare_column_for_filter(table, column_name, values, filter_name='in', accept_none=True, **kwargs)


def like_maybe_with_none(table, column_name, values, **kwargs):
    return prepare_column_for_filter(table, column_name, values, filter_name='like', accept_none=True, **kwargs)


def ilike_maybe_with_none(table, column_name, values, **kwargs):
    return prepare_column_for_filter(table, column_name, values, filter_name='ilike', accept_none=True, **kwargs)


def rlike_maybe_with_none(table, column_name, values, **kwargs):
    return prepare_column_for_filter(table, column_name, values, filter_name='rike', accept_none=True, **kwargs)


def register_filter(class_):
    FILTER_BUILDER.append(class_())
    return class_


class Filter(object):
    end_pattern = None
    patterns = None

    def match(self, attribute):
        return attribute in self.patterns

    def parse(self, attribute):
        if attribute.endswith(self.end_pattern):
            return attribute.rsplit(self.end_pattern, 1)[0]

    def __call__(self, table, column_name, value):
        raise NotImplemented


@register_filter
class IsFilter(Filter):
    end_pattern = '_is'
    patterns = ['is']

    def __call__(self, table, column_name, value):
        column = get_table_column(table, column_name)
        return (
            column.is_(value),
            get_column_table_relations(column))


@register_filter
class IsNotFilter(Filter):
    end_pattern = '_is_not'
    patterns = ['isnot']

    def __call__(self, table, column_name, value):
        column = get_table_column(table, column_name)
        return (
            column.isnot(value),
            get_column_table_relations(column))


@register_filter
class IsLikeFilter(Filter):
    end_pattern = '_is_like'
    patterns = ['like', 'contém']

    def __call__(self, table, column_name, value):
        return like_maybe_with_none(table, column_name, value)


@register_filter
class IsILikeFilter(Filter):
    end_pattern = '_is_ilike'
    patterns = ['ilike']

    def __call__(self, table, column_name, value):
        return ilike_maybe_with_none(table, column_name, value)


@register_filter
class IsRLikeFilter(Filter):
    end_pattern = '_is_rlike'
    patterns = ['rlike']

    def __call__(self, table, column_name, value):
        return rlike_maybe_with_none(table, column_name, value)


@register_filter
class BiggerThenFilter(Filter):
    end_pattern = '_is_bigger'
    patterns = ['bigger', '>']

    def __call__(self, table, column_name, value):
        column = get_table_column(table, column_name)
        return (
            column > value,
            get_column_table_relations(column))


@register_filter
class BiggerOrEqualThenFilter(Filter):
    end_pattern = '_is_bigger_or_equal'
    patterns = ['bigger_or_equal', '>=']

    def __call__(self, table, column_name, value):
        column = get_table_column(table, column_name)
        return (
            column >= value,
            get_column_table_relations(column))


@register_filter
class LowerThenFilter(Filter):
    end_pattern = '_is_lower'
    patterns = ['lower', '<']

    def __call__(self, table, column_name, value):
        column = get_table_column(table, column_name)
        return (
            column < value,
            get_column_table_relations(column))


@register_filter
class LowerOrEqualThenFilter(Filter):
    end_pattern = '_is_lower_or_equal'
    patterns = ['lower_or_equal', '<=']

    def __call__(self, table, column_name, value):
        column = get_table_column(table, column_name)
        return (
            column <= value,
            get_column_table_relations(column))


@register_filter
class EqualFilter(Filter):
    end_pattern = '_is_equal'
    patterns = ['equal', '==', '=']

    def __call__(self, table, column_name, value):
        column = get_table_column(table, column_name)
        return (
            column == value,
            get_column_table_relations(column))


@register_filter
class DifferentFilter(Filter):
    end_pattern = '_is_different'
    patterns = ['different', '!=', '≠']

    def __call__(self, table, column_name, value):
        column = get_table_column(table, column_name)
        return (
            column != value,
            get_column_table_relations(column))


def lookup_filter_builder(attribute):
    for builder in FILTER_BUILDER:
        matched_attribute = builder.parse(attribute)
        if matched_attribute:
            return matched_attribute, builder

    return attribute, default_filter_builder


def default_filter_builder(table, column_name, values):
    related_tables = set()
    sa_filter = None

    if isinstance(values, FilterBy):
        filter_type = values.filter_type.lower()

        if filter_type == 'or':
            or_queries = []
            for value in values.value:
                deep_sa_filter, deep_related_tables = default_filter_builder(table, column_name, value)
                if deep_sa_filter is not None:
                    related_tables.update(deep_related_tables)
                    or_queries.append(deep_sa_filter)
            sa_filter = filter_query_with_queries(or_queries)

        elif filter_type == 'and':
            and_queries = []
            for value in values.value:
                deep_sa_filter, deep_related_tables = default_filter_builder(table, column_name, value)
                if deep_sa_filter is not None:
                    related_tables.update(deep_related_tables)
                    and_queries.append(deep_sa_filter)
            sa_filter = filter_query_with_queries(and_queries, join_with='and')

        else:
            for builder in FILTER_BUILDER:
                if builder.match(filter_type):
                    sa_filter, this_related_tables = builder(table, column_name, values.value)
                    if sa_filter is not None:
                        related_tables.update(this_related_tables)
                    break
            else:
                raise Error('filter_type', 'Invalid filter type %s' % values.filter_type)

    elif values is drop:
        sa_filter = None

    elif not is_nonstr_iter(values):
        column = get_table_column(table, column_name)
        related_tables.update(get_column_table_relations(column))
        sa_filter = column == values

    else:
        or_queries = []
        noniter_values = set()

        for value in values:
            if isinstance(value, FilterBy) or is_nonstr_iter(value):
                deep_sa_filter, deep_related_tables = default_filter_builder(table, column_name, value)
                if deep_sa_filter is not None:
                    related_tables.update(deep_related_tables)
                    or_queries.append(sa_filter)
            elif value is not drop:
                noniter_values.add(value)

        if noniter_values:
            deep_sa_filter, deep_related_tables = maybe_with_none(table, column_name, noniter_values)
            if deep_sa_filter is not None:
                related_tables.update(deep_related_tables)
                or_queries.append(deep_sa_filter)

        sa_filter = filter_query_with_queries(or_queries)

    return sa_filter, related_tables


# TODO
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
            values = [clean_string(v).lower() for v in values.value.split()]
            for i, r in enumerate(response):
                r_value = r.get(key)
                if not r_value:
                    continue

                r_value = clean_string(r_value).lower()
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
            raise Error('filter_type', 'Invalid filter type %s' % values.filter_type)

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
