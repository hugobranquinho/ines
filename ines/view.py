# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

from copy import deepcopy

from colander import All
from colander import Boolean as BaseBoolean
from colander import drop as colander_drop
from colander import DateTime as BaseDateTime
from colander import MappingSchema
from colander import null
from colander import Number
from colander import OneOf
from colander import required as colander_required
from colander import SequenceSchema
from colander import SchemaNode as BaseSchemaNode
from colander import String
from colander import TupleSchema
from pyramid.compat import is_nonstr_iter
from pyramid.config.views import DefaultViewMapper
from pyramid.settings import asbool
from pyramid.view import view_config
from pyramid.view import view_defaults
from zope.interface import implementer

from ines.convert import camelcase
from ines.convert import force_unicode
from ines.convert import maybe_integer
from ines.convert import uncamelcase
from ines.exceptions import Error
from ines.interfaces import ISchemaViewManager
from ines.utils import MissingDict


DEFAULT_SCHEMA_METHODS = ['get', 'put', 'post', 'delete']
TRUES = frozenset(('t', 'true', 'y', 'yes', 'on', '1'))
FALSES = frozenset(('f', 'false', 'f', 'no', 'off', '0'))


class api_config(view_config):
    def __call__(self, wrapped):
        settings = self.__dict__.copy()
        depth = settings.pop('_depth', 0)

        if 'renderer' not in settings:
            settings['renderer'] = 'json'

        # Input decorator
        input_schema = settings.pop('input', None)
        use_fields = settings.pop('use_fields', False)
        if input_schema or use_fields:
            decorator = settings.pop('decorator', None)
            if decorator is None:
                decorator = []
            elif not is_nonstr_iter(decorator):
                decorator = [decorator]
            decorator = list(decorator)

            input_view_decorator = InputViewValidator(input_schema, use_fields)
            decorator.append(input_view_decorator)
            settings['decorator'] = tuple(decorator)

        # Output mapper
        output_schema = settings.pop('output', None)
        if output_schema:
            output_view = OutputView(output_schema)
            previous_mapper = settings.get('mapper', DefaultViewMapper)
            class OutputViewMapper(previous_mapper):
                def __call__(self, view):
                    view = previous_mapper.__call__(self, view)
                    return output_view(view)
            settings['mapper'] = OutputViewMapper

        def callback(context, name, ob):
            route_name = settings.get('route_name')
            if not route_name:
                route_name = getattr(ob, '__view_defaults__', {}).get('route_name')

            # Register input and output schema
            if route_name and (input_schema or use_fields or output_schema):
                schema_manager = context.config.registry.queryUtility(ISchemaViewManager, name=route_name)
                if not schema_manager:
                    schema_manager = SchemaViewManager(route_name)
                    context.config.registry.registerUtility(schema_manager, name=route_name)

                if input_schema or use_fields:
                    input_view_decorator.route_name = route_name
                    input_view_decorator.request_method = settings.get('request_method')
                    schema_manager.add_schema(input_view_decorator)
                if output_schema:
                    output_view.route_name = route_name
                    output_view.request_method = settings.get('request_method')
                    schema_manager.add_schema(output_view)

            config = context.config.with_package(info.module)
            config.add_view(view=ob, **settings)

        info = self.venusian.attach(
            wrapped,
            callback,
            category='pyramid',
            depth=depth + 1)

        if info.scope == 'class':
            if settings.get('attr') is None:
                settings['attr'] = wrapped.__name__
            if 'request_method' not in settings:
                request_method = wrapped.__name__.upper()
                if request_method == 'ADD':
                    request_method = 'POST'
                elif request_method == 'UPDATE':
                    request_method = 'PUT'
                settings['request_method'] = request_method

        settings['_info'] = info.codeinfo
        return wrapped


class api_defaults(view_defaults):
    def __init__(self, **settings):
        view_defaults.__init__(self, **settings)

        if not hasattr(self, 'renderer'):
            self.renderer = 'json'


class InputViewValidator(object):
    schema_type = 'request'

    def __init__(self, schema=None, use_fields=False):
        self.schema = schema
        self.route_name = None
        self.request_method = None

        self.fields_schema = None
        if use_fields:
            self.fields_schema = SearchFields()

    def __call__(self, wrapped):
        def decorator(context, request):
            context.input_schema = self.schema

            context.structure = {}
            if self.schema:
                structure = get_request_structure(request, self.schema)
                context.structure = self.schema.deserialize(structure) or {}

            # Construct output fields
            context.include_fields = set()
            context.exclude_fields = set()
            if self.fields_schema:
                fields_structure = get_request_structure(request, self.fields_schema)
                fields = self.fields_schema.deserialize(fields_structure)
                if fields:
                    context.include_fields.update(
                        uncamelcase(f) for f in fields.get('field', []))
                    context.include_fields.update(
                        uncamelcase(f) for f in fields.get('fields', []))
                    context.exclude_fields.update(
                        uncamelcase(f) for f in fields.get('exclude_field', []))
                    context.exclude_fields.update(
                        uncamelcase(f) for f in fields.get('exclude_fields', []))

            return wrapped(context, request)
        return decorator


def get_request_structure(request, schema):
    method = request.method.upper()
    if method in ('PUT', 'POST'):
        request_values = request.POST.dict_of_lists()
    elif method == 'DELETE':
        request_values = request.DELETE.dict_of_lists()
    else:
        request_values = request.GET.dict_of_lists()
    return construct_input_structure(schema, request_values)


def construct_input_structure(schema, values, padding=None):
    name = camelcase(schema.name)
    if padding and name:
        name = '%s.%s' % (padding, name)

    if isinstance(schema, SequenceSchema):
        child = schema.children[0]
        find_exact_name = not node_is_iterable(child)

        result = []
        for values in construct_sequence_items(name, values):
            value = construct_input_structure(child, values, padding=name)
            if value is not None:
                result.append(value)

            if find_exact_name:
                exact_value = values.get(name)
                if exact_value:
                    result.append(exact_value)

        return result

    elif isinstance(schema, TupleSchema):
        raise NotImplementedError('TupleSchema need to be implemented')

    elif isinstance(schema, MappingSchema):
        result = {}
        for child in schema.children:
            value = construct_input_structure(child, values, padding=name)
            if value is not None:
                result[child.name] = value
        return result

    else:
        for key, value in values.items():
            if value and key == name:
                if is_nonstr_iter(value):
                    return value.pop(0)
                else:
                    return value


def node_is_iterable(node):
    return isinstance(node, (TupleSchema, MappingSchema, SequenceSchema))


def construct_sequence_items(name, values):
    maybe_deep_name = name + '.'
    result_sequence = MissingDict()
    for key, value in values.items():
        if not value:
            continue

        if key == name:
            key_first = ''
        elif key.startswith(maybe_deep_name):
            key_first = key.split(maybe_deep_name, 1)[1]
        else:
            continue

        if not is_nonstr_iter(value):
            value = [value]

        if '.' in key_first:
            maybe_number, key_second = key_first.split('.', 1)
            maybe_number = maybe_integer(maybe_number)
            if maybe_number is not None:
                key = u'.'.join([name, key_second])
                result_sequence[maybe_number][key] = value[0]
                continue

        elif key_first:
            maybe_number = maybe_integer(key_first)
            if maybe_number is not None:
                result_sequence[maybe_number][name] = value[0]
                continue

        for i, key_value in enumerate(value):
            result_sequence[i][key] = key_value

    result_sequence = result_sequence.items()
    result_sequence.sort()
    return [v for i, v in result_sequence]


class OutputView(object):
    schema_type = 'response'

    def __init__(self, schema):
        self.schema = schema
        self.allowed_fields = find_allowed_fields(self.schema)
        if not self.allowed_fields:
            raise Error('output', u'Define output schema')

        self.route_name = None
        self.request_method = None

    def __call__(self, wrapped):
        def decorator(context, request):
            context.output_schema = self.schema

            # Define allowed fields
            context.allowed_fields = self.allowed_fields

            # Look and validate requested fields
            include_fields = getattr(context, 'include_fields', None)
            if include_fields:
                context.output_fields = construct_allowed_fields(self.allowed_fields, include_fields)
            else:
                context.output_fields = deepcopy(self.allowed_fields)

            # Exclude fields
            exclude_fields = getattr(context, 'exclude_fields', None)
            if exclude_fields:
                for field in exclude_fields:
                    maybe_field_child = field + '.'
                    for requested_field in context.output_fields.keys():
                        if requested_field == field or requested_field.startswith(maybe_field_child):
                            context.output_fields.pop(requested_field)

            if not context.output_fields:
                keys = u'+'.join(allowed_fields_to_set(self.allowed_fields))
                raise Error(keys, u'Please define some fields to export')
            context.fields = deepcopy(context.output_fields)

            result = wrapped(context, request)
            return construct_output_structure(self.schema, result, context.output_fields)
        return decorator


def allowed_fields_to_set(fields, padding=None):
    result = set()
    for key, children in fields.items():
        if padding:
            key = '%s.%s' % (padding, key)
        result.add(key)
        if children:
            result.update(allowed_fields_to_set(children, key))
    return result


def construct_output_structure(schema, values, fields):
    if isinstance(schema, SequenceSchema):
        result = []
        if values is None:
            return result
        elif values is null:
            return []

        child = schema.children[0]
        for value in values:
            child_value = construct_output_structure(child, value, fields)
            if child_value is not None:
                result.append(child_value)

        return result

    elif isinstance(schema, TupleSchema):
        raise NotImplementedError('TupleSchema need to be implemented')

    elif isinstance(schema, MappingSchema):
        result = {}
        if values is None:
            return result

        if isinstance(values, dict):
            get_value = values.get
        else:
            get_value = lambda k, d: getattr(values, k, d)

        for child in schema.children:
            if child.name not in fields:
                continue

            child_values = get_value(child.name, child.default)
            value = construct_output_structure(
                child,
                child_values,
                fields[child.name])
            if value is not None or child.missing is not colander_drop:
                result[camelcase(child.name)] = value

        return result

    else:
        if values is not None:
            values = schema.serialize(values)

        if values is null:
            return None
        elif values is not None:
            if isinstance(schema.typ, Number):
                return schema.typ.num(values)
            elif isinstance(schema.typ, BaseBoolean):
                return asbool(values)
            else:
                return values
        else:
            return values


def find_allowed_fields(schema, padding=None):
    if padding:
        name = '%s.%s' % (padding, schema.name)
    else:
        name = schema.name

    allowed_fields = {}
    if isinstance(schema, SequenceSchema):
        for child in schema.children:
            allowed_fields.update(find_allowed_fields(child, name))

    elif node_is_iterable(schema):
        for child in schema.children:
            allowed_fields[child.name] = find_allowed_fields(child, name)

    return allowed_fields



class DefaultAPIView(object):
    def __init__(self, context, request):
        self.request = request
        self.context = context
        self.api = self.request.api

    def create_pagination_href(self, route_name, pagination, **params):
        queries = {}
        for key, values in self.request.GET.dict_of_lists().items():
            values = [value for value in values if value]
            if values:
                queries[key] = values

        # Next page
        next_href = None
        next_page = pagination.page + 1
        if next_page <= pagination.last_page:
            queries['page'] = [next_page]
            next_href = self.request.route_url(
                route_name,
                _query=queries,
                **params)

        # Previous page
        previous_href = None
        previous_page = pagination.page - 1
        if previous_page >= 1:
            queries['page'] = [previous_page]
            previous_href = self.request.route_url(
                route_name,
                _query=queries,
                **params)

        # First page
        queries['page'] = [1]
        first_href = self.request.route_url(
            route_name,
            _query=queries,
            **params)

        # Last page
        queries['page'] = [pagination.last_page]
        last_href = self.request.route_url(
            route_name,
            _query=queries,
            **params)

        return {
            'next_page_href': next_href,
            'previous_page_href': previous_href,
            'first_page_href': first_href,
            'last_page_href': last_href}


def construct_allowed_fields(fields_dict, requested_fields, padding=None, add_all=False):
    """
    Allowed structure:
    {
        a,
        b,
        c {
            d,
            e,
            f {
                g,
                h {
                    i
                }
            }
        }
    }

    Requested: b
    If we request B, we ignore brothers of B.
    {
        b
    }

    Requested: c
    If we request C, we ignore brothers of C.
    {
        c {
            d,
            e,
            f {
                g,
                h {
                    i
                }
            }
        }
    }

    Requested: c.f.h
    If we request C.F.H, we ignore brothers of C.F.H.
    But A, B, C.D and C.E are "fathers" of C.F.H so we add it
    {
        a,
        b,
        c {
            d,
            e,
            f {
                h {
                    i
                }
            }
        }
    }

    Requested: c.f + c.f.h + a
    If we request C.F, we ignore brothers of C.F and add C.F children
    If we request C.F.H, we ignore brothers of C.F.H, even if it has been added on C.F
    If we request A, we ignore brothers of A
    {
        a,
        c {
            f {
                h {
                    i
                }
            }
        }
    }
    """

    result = {}
    level_field_added = False
    post_level_field_add = []

    for original_name, children in fields_dict.items():
        if padding:
            name = '%s.%s' % (padding, original_name)
        else:
            name = original_name

        if name in requested_fields:
            # If not requested, no others fields will be added in this level
            level_field_added = True

            if children:
                # All children fields will be added!
                result[original_name] = construct_allowed_fields(
                    children,
                    requested_fields,
                    name,
                    add_all=True)
            else:
                result[original_name] = {}

        elif not level_field_added:
            # Add this fields, if no other field added in this level
            post_level_field_add.append((original_name, name, children, add_all))

    if not level_field_added and post_level_field_add:
        for original_name, name, children, field_add_all in post_level_field_add:
            if children:
                result[original_name] = construct_allowed_fields(
                    children,
                    requested_fields,
                    name,
                    field_add_all)
            else:
                result[original_name] = {}

    return result


@implementer(ISchemaViewManager)
class SchemaViewManager(object):
    def __init__(self, route_name):
        self.route_name = route_name
        self.schemas = []

    def add_schema(self, schema):
        self.schemas.append(schema)


class SchemaView(object):
    def __init__(self, routes_names):
        self.routes_names = routes_names

    def __call__(self, context, request):
        nodes = MissingDict()
        requested_methods = [key.lower() for key in request.GET.keys()]

        for route_name in self.routes_names:
            schema_manager = request.registry.queryUtility(ISchemaViewManager, name=route_name)
            if not schema_manager:
                continue

            for schema in schema_manager.schemas:
                if not schema.request_method:
                    request_methods = DEFAULT_SCHEMA_METHODS
                else:
                    request_methods = [schema.request_method]

                add_to_requests_methods = set()
                for request_method in request_methods:
                    if not requested_methods or request_method.lower() in requested_methods:
                        add_to_requests_methods.add(request_method.lower())
                if not add_to_requests_methods:
                    continue

                if schema.schema:
                    structure = construct_schema_structure(
                        request,
                        schema.schema,
                        schema.schema_type)
                else:
                    structure = []

                if schema.schema_type == 'request' and schema.fields_schema:
                    fields_structure = construct_schema_structure(
                        request,
                        schema.fields_schema,
                        schema.schema_type)

                    if not isinstance(structure, list):
                        structure = [structure]
                    structure.extend(fields_structure)

                for method in add_to_requests_methods:
                    nodes[method][schema.schema_type] = structure

        return nodes


def construct_schema_structure(request, schema, schema_type):
    if isinstance(schema, SequenceSchema):
        child = schema.children[0]
        children = construct_schema_structure(request, child, schema_type)
        if not is_nonstr_iter(children):
            children = [children]

        details = {
            'type': 'sequence',
            'order': schema._order,
            'children': children}

        if not schema.name:
            details.update({
                'name': camelcase(child.name),
                'title': child.title,
                'description': child.description or None})
        else:
            details.update({
                'name': camelcase(schema.name),
                'title': schema.title,
                'description': schema.description or None})

        return details

    elif isinstance(schema, TupleSchema):
        raise NotImplementedError('TupleSchema need to be implemented')

    elif isinstance(schema, MappingSchema):
        result = []
        for child in schema.children:
            result.append(construct_schema_structure(request, child, schema_type))
        return result

    else:
        details = {
            'type': get_colander_type_name(schema.typ),
            'name': camelcase(schema.name),
            'title': schema.title,
            'description': schema.description or None,
            'order': schema._order}

        request_validation = []
        if schema.validator:
            if isinstance(schema.validator, All):
                validators = schema.validator.validators
            elif not is_nonstr_iter(schema.validator):
                validators = [schema.validator]

            for validator in validators:
                if isinstance(validator, OneOfWithDescription):
                    details['options'] = []
                    for choice, description in validator.choices_with_descripton:
                        details['options'].append({
                            'value': choice,
                            'text': request.translate(description)})
                elif isinstance(validator, OneOf):
                    details['options'] = []
                    for choice in validator.choices:
                        choice_description = force_unicode(choice).replace(u'_', u' ').title()
                        details['options'].append({
                            'value': choice,
                            'text': choice_description})
                else:
                    request_validation.append(validator)

        if schema_type == 'request':
            validation = {}
            if schema.required:
                validation['required'] = True

            if request_validation:
                for validator in request_validation:
                    validation[get_colander_type_name(validator)] = True
            if validation:
                details['validation'] = validation

            default = schema.missing
        else:
            if schema.missing is colander_drop:
                details['maybeNotSent'] = True
            default = schema.default

        if (default is not colander_drop
                and default is not colander_required
                and default is not null):
            if isinstance(schema.typ, Number):
                default = schema.typ.num(default)
            elif isinstance(schema.typ, BaseBoolean):
                default = asbool(default)
            details['default'] = default

        return details


def get_colander_type_name(node):
    return camelcase(str(node.__class__.__name__).lower())


class SchemaNode(BaseSchemaNode):
    def __init__(self, *arg, **kw):
        self.return_none_if_defined = kw.pop('return_none_if_defined', False)
        super(SchemaNode, self).__init__(*arg, **kw)

    def deserialize(self, cstruct=null):
        result = BaseSchemaNode.deserialize(self, cstruct)
        # Return None, only if request and cstruct is empty
        if (self.return_none_if_defined
                and (result is null or result is colander_drop)
                and cstruct is not null and not cstruct):
            return None
        else:
            return result

    def clone(self, **kwargs):
        cloned = BaseSchemaNode.clone(self)
        cloned.__dict__.update(kwargs)
        return cloned


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


class InputField(SequenceSchema):
    field = SchemaNode(String(), missing=None)


class InputExcludeField(SequenceSchema):
    exclude_field = SchemaNode(String(), missing=None)


class InputFields(SequenceSchema):
    fields = SchemaNode(String(), missing=None)


class InputExcludeFields(SequenceSchema):
    exclude_field = SchemaNode(String(), missing=None)


def split_values(appstruct):
    result = set()
    for value in appstruct:
        result.update(value.split(u','))
    return list(result)


class SearchFields(MappingSchema):
    field = InputField()
    exclude_field = InputExcludeField()
    fields = InputFields(preparer=split_values)
    exclude_fields = InputExcludeFields(preparer=split_values)
