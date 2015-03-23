# -*- coding: utf-8 -*-

from json import loads

from colander import _ as colander_i18n
from colander import _SchemaMeta
from colander import _SchemaNode
from colander import _marker
from colander import Boolean as BaseBoolean
from colander import drop
from colander import Date
from colander import DateTime as BaseDateTime
from colander import Integer
from colander import Invalid
from colander import MappingSchema
from colander import null
from colander import OneOf
from colander import Range
from colander import SchemaNode
from colander import SchemaType
from colander import Sequence
from colander import SequenceSchema
from colander import String
from colander.compat import is_nonstr_iter

from ines import _
from ines import FALSES
from ines import TRUES
from ines.convert import pluralizing_key


# See https://github.com/Pylons/colander/pull/212
def clone_sequenceschema_fix(self):
    children = [node.clone() for node in self.children]
    cloned = self.__class__(self.typ, *children)

    attributes = self.__dict__.copy()
    attributes.pop('children', None)
    cloned.__dict__.update(attributes)
    return cloned
clone_sequenceschema_fix.__name__ = 'clone'
SchemaNode.clone = clone_sequenceschema_fix


# Propose this! Update node attributes when cloning
def update_node_attributes_on_clone(self, **kw):
    cloned = clone_sequenceschema_fix(self)
    cloned.__dict__.update(kw)
    return cloned
update_node_attributes_on_clone.__name__ = 'clone'
SchemaNode.clone = update_node_attributes_on_clone


def my_deserialize(self, cstruct=null):
    # Return None, only if request and cstruct is empty
    if self.return_none_if_defined and cstruct == '':
        return None

    appstruct = super(SchemaNode, self).deserialize(cstruct)

    # Propose this!
    if hasattr(self, 'finisher'):
        # if the finisher is a function, call a single preparer
        if callable(self.finisher):
            appstruct = self.finisher(appstruct)
        # if the finisher is a list, call each separate preparer
        elif is_nonstr_iter(self.finisher):
            for preparer in self.finisher:
                appstruct = preparer(self, appstruct)

    return appstruct
my_deserialize.__name__ = 'deserialize'
SchemaNode.deserialize = my_deserialize


def my_init(self, *arg, **kw):
    self.return_none_if_defined = kw.pop('return_none_if_defined', False)
    super(SchemaNode, self).__init__(*arg, **kw)
my_init.__name__ = '__init__'
SchemaNode.__init__ = my_init


# Propose this!
def sequence_impl(self, node, value, callback, accept_scalar):
    if accept_scalar is None:
        accept_scalar = self.accept_scalar

    value = self._validate(node, value, accept_scalar)

    error = None
    result = []

    for num, subval in enumerate(value):
        try:
            result.append(callback(node.children[0], subval))
        except Invalid as e:
            if error is None:
                error = Invalid(node)
            error.add(e, num)

    if error is not None:
        raise error

    if not result:
        return null
    return result
sequence_impl.__name__ = '_impl'
Sequence._impl = sequence_impl


# See https://github.com/Pylons/colander/pull/211
def my_range_call(self, node, value):
    if self.min is not None:
        min_value = self.min
        if callable(min_value):
            min_value = min_value()

        if value < min_value:
            min_err = colander_i18n(
                self.min_err, mapping={'val':value, 'min':min_value})
            raise Invalid(node, min_err)

    if self.max is not None:
        max_value = self.max
        if callable(max_value):
            max_value = max_value()

        if value > max_value:
            max_err = colander_i18n(
                self.max_err, mapping={'val':value, 'max':max_value})
            raise Invalid(node, max_err)
my_range_call.__name__ = '__call__'
Range.__call__ = my_range_call


# Propose this!
# If a diferent name is defined, should we clone? and set a new name?
def my_schemameta(cls, name, bases, clsattrs):
    nodes = []
    for name, value in clsattrs.items():
        if isinstance(value, _SchemaNode):
            delattr(cls, name)
            # If a diferent name is defined, should we clone? and set a new name?
            if value.name:
                value = value.clone()
            value.name = name
            if value.raw_title is _marker:
                value.title = name.replace('_', ' ').title()
            nodes.append((value._order, value))
    nodes.sort()
    cls.__class_schema_nodes__ = [ n[1] for n in nodes ]
    cls.__all_schema_nodes__ = []
    for c in reversed(cls.__mro__):
        csn = getattr(c, '__class_schema_nodes__', [])
        cls.__all_schema_nodes__.extend(csn)
my_schemameta.__name__ = '__init__'
_SchemaMeta.__init__ = my_schemameta


class OneOfWithDescription(OneOf):
    def __init__(self, choices):
        if isinstance(choices, dict):
            choices = choices.items()
        self.choices_with_descripton = choices
        super(OneOfWithDescription, self).__init__(dict(choices).keys())


class DateTime(BaseDateTime):
    def __init__(self, default_tzinfo=None):
        super(DateTime, self).__init__(default_tzinfo=default_tzinfo)


class Boolean(BaseBoolean):
    def __init__(self, **kwargs):
        if 'true_choices' not in kwargs:
            kwargs['true_choices'] = TRUES
        if 'false_choices' not in kwargs:
            kwargs['false_choices'] = FALSES
        super(Boolean, self).__init__(**kwargs)

    def deserialize(self, node, cstruct):
        if cstruct == '':
            return null
        else:
            return super(Boolean, self).deserialize(node, cstruct)


class InputField(SequenceSchema):
    field = SchemaNode(String(), missing=drop)


class InputExcludeField(SequenceSchema):
    exclude_field = SchemaNode(String(), missing=drop)


class InputFields(SequenceSchema):
    fields = SchemaNode(String(), missing=drop)


class InputExcludeFields(SequenceSchema):
    exclude_fields = SchemaNode(String(), missing=drop)


class SplitValues(object):
    def __init__(self, break_with=u',', break_limit=-1):
        self.break_with = break_with
        self.break_limit = break_limit

    def __call__(self, appstruct):
        result = []
        if appstruct is not null:
            for value in appstruct:
                if isinstance(value, basestring) and not value.startswith(u'{"'):
                    result.extend(value.split(self.break_with, self.break_limit))
        return result

split_values = SplitValues()


class SearchFields(MappingSchema):
    field = InputField(missing=drop)
    exclude_field = InputExcludeField(missing=drop)
    fields = InputFields(preparer=split_values, missing=drop)
    exclude_fields = InputExcludeFields(preparer=split_values, missing=drop)


# Global attributes
PAGE = SchemaNode(Integer(), title=_(u'Page'), missing=1)
LIMIT_PER_PAGE = SchemaNode(Integer(), title=_(u'Results per page'), missing=20)
ORDER_BY = SchemaNode(String(), title=_(u'Order by'), missing=drop)
NUMBER_OF_RESULTS = SchemaNode(Integer(), title=_(u'Number of results'))
LAST_PAGE = SchemaNode(Integer(), title=_(u'Last page'))
NEXT_PAGE_HREF = SchemaNode(String(), title=_(u'Next page url'))
PREVIOUS_PAGE_HREF = SchemaNode(String(), title=_(u'Previous page url'))
FIRST_PAGE_HREF = SchemaNode(String(), title=_(u'First page url'))
LAST_PAGE_HREF = SchemaNode(String(), title=_(u'Last page url'))


class OrderBy(object):
    def __init__(self, column_name, descendant=False):
        self.column_name = column_name
        self.descendant = descendant

    def repr(self):
        if self.descendant:
            order = 'DESC'
        else:
            order = 'ASC'
        return '%s %s' % (self.column_name, order)


def OrderFinisher(node, appstruct):
    if appstruct and appstruct.get('order_by'):
        order_by = appstruct['order_by'].split(' ', 1)
        descendant = bool(len(order_by) == 2 and order_by[1].lower() in ('desc', 'd'))
        appstruct['order_by'] = OrderBy(order_by[0], descendant)
    return appstruct


class SequenceFinisher(object):
    def __init__(self, single_key, plural_key):
        self.single_key = single_key
        self.plural_key = plural_key

    def __call__(self, node, appstruct):
        if appstruct:
            if self.single_key in appstruct:
                appstruct.setdefault(self.plural_key, []).extend(appstruct.pop(self.single_key))
            if not appstruct.get(self.plural_key):
                appstruct.pop(self.plural_key, None)
        return appstruct


class PaginationInput(MappingSchema):
    page = PAGE.clone(missing=1)
    limit_per_page = LIMIT_PER_PAGE.clone(missing=20)
    order_by = ORDER_BY.clone(missing=drop)
    finisher = [OrderFinisher]


def add_sequence_node(schema, sequence_node, single_key, plural_key=None):
    single_node = SequenceSchema(Sequence(), sequence_node, missing=drop, name=single_key)
    schema.__class_schema_nodes__.append(single_node)
    schema.__all_schema_nodes__.append(single_node)

    if not plural_key:
        plural_key = pluralizing_key(single_key)

    plural_node = SequenceSchema(
        Sequence(),
        sequence_node,
        missing=drop,
        preparer=split_values,
        name=plural_key)
    schema.__class_schema_nodes__.append(plural_node)
    schema.__all_schema_nodes__.append(plural_node)

    sequence_finisher = SequenceFinisher(single_key, plural_key)
    if hasattr(schema, 'finisher'):
        if not is_nonstr_iter(schema.finisher):
            previous_finisher = schema.finisher
            def decorator(cls, appstruct):
                appstruct = sequence_finisher(cls, appstruct)
                return previous_finisher(cls, appstruct)
            schema.finisher = decorator
        else:
            schema.finisher.append(sequence_finisher)
    else:
        schema.finisher = [sequence_finisher]


def add_sequence_nodes(schema, *sequence_nodes):
    for sequence_node in sequence_nodes:
        if isinstance(sequence_node, dict):
            add_sequence_node(schema, **sequence_node)
        elif is_nonstr_iter(sequence_node):
            add_sequence_node(schema, *sequence_node)
        else:
            add_sequence_node(schema, sequence_node, sequence_node.name)


class PaginationOutput(MappingSchema):
    page = PAGE
    limit_per_page = LIMIT_PER_PAGE
    last_page = LAST_PAGE
    number_of_results = NUMBER_OF_RESULTS
    next_page_href = NEXT_PAGE_HREF
    previous_page_href = PREVIOUS_PAGE_HREF
    first_page_href = FIRST_PAGE_HREF
    last_page_href = LAST_PAGE_HREF


class DeleteOutput(MappingSchema):
    deleted = SchemaNode(Boolean(), title=_(u'Deleted'))


class FilterBy(object):
    def __init__(self, filter_type, value):
        self.filter_type = filter_type
        self.value = value


class FilterByField(SchemaType):
    def deserialize(self, node, cstruct):
        if cstruct and isinstance(cstruct, basestring):
            try:
                json_cstruct = loads(cstruct)
            except (ValueError, UnicodeEncodeError):
                pass
            else:
                return self.create_filter_by(node, json_cstruct)

        return super(FilterByField, self).deserialize(node, cstruct)

    def create_filter_by(self, node, json_value, filter_type=None):
        if isinstance(json_value, dict):
            and_values = []
            for value_filter_type, value in json_value.items():
                if isinstance(value, dict):
                    query = self.create_filter_by(node, value, value_filter_type)
                    if isinstance(query, FilterBy):
                        and_values.append(query)

                elif not is_nonstr_iter(value):
                    query = self.create_filter_by(node, value, value_filter_type)
                    if isinstance(query, FilterBy):
                        and_values.append(query)

                else:
                    for deep_value in value:
                        query = self.create_filter_by(node, deep_value, value_filter_type)
                        if isinstance(query, FilterBy):
                            and_values.append(query)

            filter_type = (filter_type or 'and').lower()
            if filter_type not in ('and', 'or'):
                message = u'Invalid filter type %s for %s' % (value_filter_type, json_value)
                raise Invalid('filter_type', message)
            else:
                return FilterBy(filter_type, and_values)

        elif is_nonstr_iter(json_value):
            filter_type = filter_type or 'or'
            return self.create_filter_by(node, {filter_type: json_value}, filter_type)

        else:
            value = super(FilterByField, self).deserialize(node, json_value)
            if value is not null:
                return FilterBy(filter_type or '==', value)
            else:
                return value


class FilterByDate(FilterByField, Date):
    pass


class FilterByDateTime(FilterByField, DateTime):
    pass


class FilterByInteger(FilterByField, Integer):
    pass


class FilterByString(FilterByField, String):
    pass
