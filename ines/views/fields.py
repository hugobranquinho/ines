# -*- coding: utf-8 -*-

from cgi import FieldStorage
from json import loads
from os.path import basename

from colander import _ as COLANDER_I18N
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
from ines.convert import force_string
from ines.convert import force_unicode
from ines.convert import uncamelcase


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
            min_err = COLANDER_I18N(
                self.min_err, mapping={'val': value, 'min': min_value})
            raise Invalid(node, min_err)

    if self.max is not None:
        max_value = self.max
        if callable(max_value):
            max_value = max_value()

        if value > max_value:
            max_err = COLANDER_I18N(
                self.max_err, mapping={'val': value, 'max': max_value})
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
    cls.__class_schema_nodes__ = [n[1] for n in nodes]
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


class File(SchemaType):
    def serialize(self, node, value):
        if value is null:
            return null
        else:
            return value

    def deserialize(self, node, value):
        if isinstance(value, file):
            return {
                'file': value,
                'filename': basename(value.name),
                'type': None}

        elif isinstance(value, FieldStorage):
            return {
                'file': value.file,
                'filename': value.filename,
                'type': 'image/png'}

        else:
            raise Invalid(node, u'Ficheiro inválido.')


class Image(File):
    def deserialize(self, node, value):
        value = super(Image, self).deserialize(node, value)
        # TODO: validate image type
        return value


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


class OrderBy(object):
    def __init__(self, argument_name, descendant=False):
        self.argument_name = argument_name
        self.column_name = uncamelcase(argument_name)
        self.descendant = descendant

    def __repr__(self):
        if self.descendant:
            order = 'DESC'
        else:
            order = 'ASC'
        return '%s %s' % (self.column_name, order)


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


def add_sequence_node(schema, sequence_node, single_key, plural_key=None, with_filter_by=False):
    if not sequence_node.name:
        sequence_node = sequence_node.clone(name=single_key)

    if with_filter_by:
        sequence_node = set_node_with_filter_by(sequence_node)

    single_node = SequenceSchema(Sequence(), sequence_node, missing=drop, name=single_key)
    schema.__class_schema_nodes__.append(single_node)
    schema.__all_schema_nodes__.append(single_node)

    if plural_key:
        if not sequence_node.name:
            sequence_node = sequence_node.clone(name=plural_key)

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


def add_sequence_nodes(schema, *sequence_nodes, **kwargs):
    for sequence_node in sequence_nodes:
        if isinstance(sequence_node, dict):
            sequence_node.update(kwargs)
            add_sequence_node(schema, **sequence_node)
        elif is_nonstr_iter(sequence_node):
            add_sequence_node(schema, *sequence_node, **kwargs)
        else:
            add_sequence_node(schema, sequence_node, sequence_node.name, **kwargs)


def set_sequence_nodes(**kwargs):
    def decorator(class_):
        with_filter_by = kwargs.pop('with_filter_by', False)
        for key, value in kwargs.items():
            add_sequence_node(class_, value, key, with_filter_by=with_filter_by)
        return class_
    return decorator


@set_sequence_nodes(
    field=SchemaNode(String(), title=_(u'Show fields')),
    exclude_field=SchemaNode(String(), title=_(u'Exclude fields')))
class SearchFields(MappingSchema):
    pass


def PaginationOrderFinisher(node, appstruct):
    if appstruct and appstruct.get('order_by'):
        order_by = appstruct['order_by']
        if not is_nonstr_iter(order_by):
            order_by = [order_by]

        appstruct['order_by'] = []
        output_node = getattr(node, '__output_node__')
        for ob in order_by:
            ob = ob.split(' ', 1)
            if output_node:
                breadcrumbs = ob[0].split('.')
                if len(breadcrumbs) == 2:
                    table_name, column_name = breadcrumbs
                    table_name = uncamelcase(table_name)
                elif len(breadcrumbs) == 1:
                    column_name = breadcrumbs[0]
                    table_name = None
                else:
                    continue

                column_name = uncamelcase(column_name)
                for schema in output_node.__class_schema_nodes__:
                    if not table_name or schema.name == table_name:
                        if isinstance(schema, SequenceSchema):
                            schema = schema.__class_schema_nodes__[0]

                        nodes = schema.__class_schema_nodes__
                        nodes.extend(schema.__all_schema_nodes__)
                        for schema_node in nodes:
                            if isinstance(schema_node, SchemaNode) and schema_node.name == column_name:
                                break
                        else:
                            # Nothing found! dont add order by
                            continue
                        # Break this FOR to add order by
                        break
                else:
                    # Nothing found! dont add order by
                    continue

            descendant = bool(len(ob) == 2 and ob[1].lower() in ('desc', 'd'))
            appstruct['order_by'].append(OrderBy(ob[0], descendant))

    return appstruct


PAGE = SchemaNode(Integer(), title=_(u'Page'), missing=1)
LIMIT_PER_PAGE = SchemaNode(Integer(), title=_(u'Results per page'), missing=20)
ORDER_BY = SchemaNode(String(), title=_(u'Order by'), name='order_by')
NUMBER_OF_RESULTS = SchemaNode(Integer(), title=_(u'Number of results'))
NUMBER_OF_PAGE_RESULTS = SchemaNode(Integer(), title=_(u'Number of page results'))
LAST_PAGE = SchemaNode(Integer(), title=_(u'Last page'))
NEXT_PAGE_HREF = SchemaNode(String(), title=_(u'Next page url'))
PREVIOUS_PAGE_HREF = SchemaNode(String(), title=_(u'Previous page url'))
FIRST_PAGE_HREF = SchemaNode(String(), title=_(u'First page url'))
LAST_PAGE_HREF = SchemaNode(String(), title=_(u'Last page url'))


class PaginationInput(MappingSchema):
    page = PAGE.clone(missing=1)
    limit_per_page = LIMIT_PER_PAGE.clone(missing=20)
    order_by = SequenceSchema(Sequence(), ORDER_BY, missing=drop, preparer=split_values)
    finisher = [PaginationOrderFinisher]


class PaginationOutput(MappingSchema):
    page = PAGE
    limit_per_page = LIMIT_PER_PAGE
    last_page = LAST_PAGE
    number_of_results = NUMBER_OF_RESULTS
    number_of_page_results = NUMBER_OF_PAGE_RESULTS
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

    def __repr__(self):
        return '%s(%s)' % (self.filter_type, repr(self.value))

    def __str__(self):
        if not is_nonstr_iter(self.value):
            return force_string(self.value)
        else:
            return self.value

    def __unicode__(self):
        if not is_nonstr_iter(self.value):
            return force_unicode(self.value)
        else:
            return self.value


class FilterByType(SchemaType):
    def deserialize(self, node, cstruct):
        if cstruct and isinstance(cstruct, basestring):
            try:
                json_cstruct = loads(cstruct)
            except (ValueError, UnicodeEncodeError):
                pass
            else:
                return self.create_filter_by(node, json_cstruct)

        return super(FilterByType, self).deserialize(node, cstruct)

    def create_filter_by(self, node, json_value, filter_type=None):
        if isinstance(json_value, dict):
            queries = []
            for value_filter_type, value in json_value.items():
                if not filter_type and value_filter_type.lower() in ('and', 'or'):
                    filter_type = value_filter_type

                if isinstance(value, dict):
                    query = self.create_filter_by(node, value, value_filter_type)
                    if isinstance(query, FilterBy):
                        queries.append(query)

                elif not is_nonstr_iter(value):
                    query = self.create_filter_by(node, value, value_filter_type)
                    if isinstance(query, FilterBy):
                        queries.append(query)

                else:
                    for deep_value in value:
                        query = self.create_filter_by(node, deep_value, value_filter_type)
                        if isinstance(query, FilterBy):
                            queries.append(query)

            filter_type = (filter_type or 'and').lower()
            if filter_type not in ('and', 'or'):
                message = u'Invalid filter type %s for %s' % (value_filter_type, json_value)
                raise Invalid('filter_type', message)
            else:
                return FilterBy(filter_type, queries)

        elif is_nonstr_iter(json_value):
            filter_type = filter_type or 'or'
            return self.create_filter_by(node, {filter_type: json_value}, filter_type)

        else:
            value = super(FilterByType, self).deserialize(node, json_value)
            if value is not null:
                return FilterBy(filter_type or '==', value)
            else:
                return value


class FilterByDate(FilterByType, Date):
    pass


class FilterByDateTime(FilterByType, DateTime):
    pass


class FilterByInteger(FilterByType, Integer):
    pass


class FilterByString(FilterByType, String):
    pass


def set_node_with_filter_by(node):
    if isinstance(node.typ, String):
        return node.clone(typ=FilterByString())
    elif isinstance(node.typ, Integer):
        return node.clone(typ=FilterByInteger())
    elif isinstance(node.typ, Date):
        return node.clone(typ=FilterByDate())
    elif isinstance(node.typ, BaseDateTime):
        return node.clone(typ=FilterByDateTime())
    else:
        return node
