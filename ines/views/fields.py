# -*- coding: utf-8 -*-

from cgi import FieldStorage
from json import dumps
from json import loads
from os.path import basename

from colander import _ as COLANDER_I18N
from colander import _SchemaMeta
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
from deform.widget import DateInputWidget
from deform.widget import DateTimeInputWidget
from deform.widget import SelectWidget
from six import string_types
from six import u

from ines.convert import to_string
from ines.convert import to_unicode
from ines.convert import uncamelcase
from ines.i18n import _
from ines.utils import is_file_type


def datetinput_serialize(self, field, cstruct, **kw):
    if cstruct in (null, None):
        cstruct = ''
    readonly = kw.get('readonly', self.readonly)
    template = readonly and self.readonly_template or self.template
    options = dict(
        kw.get('options') or self.options or self.default_options
        )
    options['submitFormat'] = 'yyyy-mm-dd'

    if callable(options.get('max')):
        options['max'] = options['max']()

    kw.setdefault('options_json', dumps(options))
    values = self.get_template_values(field, cstruct, kw)
    return field.renderer(template, **values)
datetinput_serialize.__name__ = 'serialize'
DateInputWidget.serialize = datetinput_serialize


def datetimeinput_deserialize(self, field, pstruct):
    if pstruct is null:
        return null
    else:
        # seriously pickadate?  oh.  right.  i forgot.  you're javascript.
        date = (pstruct.get('date_submit') or pstruct.get('date', '')).strip()
        time = (pstruct.get('time_submit') or pstruct.get('time', '')).strip()

        if (not time and not date):
            return null
        result = 'T'.join([date, time])
        if not date:
            raise Invalid(field.schema, _('Incomplete date'), result)
        if not time:
            raise Invalid(field.schema, _('Incomplete time'), result)
        return result
datetinput_serialize.__name__ = 'deserialize'
DateTimeInputWidget.deserialize = datetimeinput_deserialize


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
    if 'new_order' in kw:
        kw['_order'] = next(self._counter)

    cloned = clone_sequenceschema_fix(self)
    cloned.__dict__.update(kw)
    return cloned
update_node_attributes_on_clone.__name__ = 'clone'
SchemaNode.clone = update_node_attributes_on_clone


def schema_extend(self, **nodes):
    self.children.extend(nodes)
schema_extend.__name__ = 'extend'
SchemaNode.extend = schema_extend


def my_deserialize(self, cstruct=null):
    # Return None, only if request and cstruct is empty
    if hasattr(self, 'return_none_if_defined'):
        if getattr(self, 'return_none_if_defined') and cstruct == '':
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


_original_SchemaMeta__init__ = _SchemaMeta.__init__
def _order_schemaMeta_nodes(cls, name, bases, clsattrs):
    _original_SchemaMeta__init__(cls, name, bases, clsattrs)
    if cls.__all_schema_nodes__:
        cls.__all_schema_nodes__.sort(lambda n: n._order)
_order_schemaMeta_nodes.__name__ = '__init__'
_SchemaMeta.__init__ = _order_schemaMeta_nodes


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
            kwargs['true_choices'] = frozenset(('t', 'true', 'y', 'yes', 'on', '1'))
        if 'false_choices' not in kwargs:
            kwargs['false_choices'] = frozenset(('f', 'false', 'f', 'no', 'off', '0'))
        super(Boolean, self).__init__(**kwargs)

    def deserialize(self, node, cstruct):
        if cstruct == '':
            return null
        else:
            return super(Boolean, self).deserialize(node, cstruct)


class HTMLString(String):
    pass


class TextString(String):
    pass


class File(SchemaType):
    def serialize(self, node, value):
        if value is null:
            return null
        else:
            return value

    def deserialize(self, node, value):
        if is_file_type(value):
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
            raise Invalid(node, _('Invalid file.'))


class Image(File):
    def deserialize(self, node, value):
        value = super(Image, self).deserialize(node, value)
        # TODO: validate image type
        return value


class SplitValues(object):
    def __init__(self, break_with=u(','), break_limit=-1):
        self.break_with = break_with
        self.break_limit = break_limit

    def __call__(self, appstruct):
        result = []
        if appstruct is not null:
            for value in appstruct:
                if isinstance(value, string_types) and not value.startswith(u('{"')):
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
    field=SchemaNode(String(), title=_('Show fields')),
    exclude_field=SchemaNode(String(), title=_('Exclude fields')))
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


class LimitPerPageInteger(Integer):
    schema_type_name = 'integer'

    def num(self, value):
        if isinstance(value, string_types) and value.lower() == 'all':
            return 'all'
        else:
            return int(value)


PAGE = SchemaNode(Integer(), title=_('Page'), missing=1)
LIMIT_PER_PAGE = SchemaNode(LimitPerPageInteger(), title=_('Results per page'), missing=20)
ORDER_BY = SchemaNode(String(), title=_('Order by'), name='order_by')
NUMBER_OF_RESULTS = SchemaNode(Integer(), title=_('Number of results'))
NUMBER_OF_PAGE_RESULTS = SchemaNode(Integer(), title=_('Number of page results'))
LAST_PAGE = SchemaNode(Integer(), title=_('Last page'))
NEXT_PAGE_HREF = SchemaNode(String(), title=_('Next page url'))
PREVIOUS_PAGE_HREF = SchemaNode(String(), title=_('Previous page url'))
FIRST_PAGE_HREF = SchemaNode(String(), title=_('First page url'))
LAST_PAGE_HREF = SchemaNode(String(), title=_('Last page url'))


class OrderByInput(MappingSchema):
    order_by = SequenceSchema(Sequence(), ORDER_BY, missing=drop, preparer=split_values)
    finisher = [PaginationOrderFinisher]


class PaginationInput(OrderByInput):
    page = PAGE.clone(missing=1)
    limit_per_page = LIMIT_PER_PAGE


CSV_DELIMITER = {
    u(','): _('Comma (,)'),
    u(';'): _('Semicolon (;)'),
    u('\\t'): _('TAB (\\t)')}

CSV_QUOTE_CHAR = {
    u('"'): _('Double quote'),
    u('\''): _('Single quote')}

CSV_LINE_TERMINATOR = {
    u('\\r\\n'): _('Windows (CR+LF)'),
    u('\\n'): _('Linux and MacOS (LF)'),
    u('\\r'): _('Old MacOS (CR)'),
    u('\\n\\r'): _('Other (LF+CR)')}

CSV_ENCODING = {
    u('utf-8'): _('All languages'),
    u('latin-1'): _('West Europe')}


class CSVInput(MappingSchema):
    csv_delimiter = SchemaNode(
        String(),
        title=_('CSV content delimiter'),
        missing=None,
        validator=OneOf(CSV_DELIMITER.keys()),
        widget=SelectWidget(values=CSV_DELIMITER.items()))
    csv_quote_char = SchemaNode(
        String(),
        title=_('CSV content quote'),
        missing=None,
        validator=OneOf(CSV_QUOTE_CHAR.keys()),
        widget=SelectWidget(values=CSV_QUOTE_CHAR.items()))
    csv_line_terminator = SchemaNode(
        String(),
        title=_('CSV line terminator'),
        missing=None,
        validator=OneOf(CSV_LINE_TERMINATOR.keys()),
        widget=SelectWidget(values=CSV_LINE_TERMINATOR.items()))
    csv_encoding = SchemaNode(
        String(),
        title=_('CSV encoding'),
        missing=None,
        validator=OneOf(CSV_ENCODING.keys()),
        widget=SelectWidget(values=CSV_ENCODING.items()))


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
    deleted = SchemaNode(Boolean(), title=_('Deleted'))


class FilterBy(object):
    def __init__(self, filter_type, value):
        self.filter_type = filter_type or '='
        self.value = value

    def __repr__(self):
        return '%s(%s)' % (self.filter_type, self.value)


class FilterByType(SchemaType):
    def deserialize(self, node, cstruct):
        if cstruct and isinstance(cstruct, string_types):
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
                message = u('Invalid filter type %s for %s') % (filter_type, json_value)
                raise Invalid('filter_type', message)
            else:
                return FilterBy(filter_type, queries)

        elif is_nonstr_iter(json_value):
            filter_type = filter_type or 'or'
            return self.create_filter_by(node, {filter_type: json_value}, filter_type)

        else:
            value = super(FilterByType, self).deserialize(node, json_value)
            if value is not null:
                return FilterBy(filter_type, value)
            else:
                return null


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


class ActiveBoolean(Boolean):
    def serialize(self, node, appstruct):
        if appstruct is null or appstruct == '':
            return null
        elif appstruct is None:
            return 'all'
        else:
            return appstruct and self.true_val or self.false_val

    def deserialize(self, node, cstruct):
        if cstruct is null or cstruct == '':
            return null
        elif str(cstruct).lower() == 'all':
            return None
        else:
            return Boolean.deserialize(self, node, cstruct)
