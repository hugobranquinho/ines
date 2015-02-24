# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

from copy import deepcopy

from colander import All
from colander import Boolean
from colander import drop as colander_drop
from colander import MappingSchema
from colander import null
from colander import Number
from colander import required as colander_required
from colander import SequenceSchema
from colander import SchemaNode
from colander import String
from colander import TupleSchema
from pyramid.compat import is_nonstr_iter
from pyramid.config.views import DefaultViewMapper
from pyramid.settings import asbool
from pyramid.view import view_config
from pyramid.view import view_defaults
from zope.interface import implementer

from ines.convert import camelcase
from ines.convert import maybe_integer
from ines.convert import uncamelcase
from ines.exceptions import Error
from ines.interfaces import ISchemaViewManager
from ines.utils import MissingDict


DEFAULT_SCHEMA_METHODS = ['get', 'put', 'post', 'delete']


class api_config(view_config):
    def __call__(self, wrapped):
        settings = self.__dict__.copy()
        depth = settings.pop('_depth', 0)

        if 'renderer' not in settings:
            settings['renderer'] = 'json'

        # Input decorator
        input_schema = settings.pop('input', None)
        if input_schema:
            decorator = settings.pop('decorator', None)
            if decorator is None:
                decorator = []
            elif not is_nonstr_iter(decorator):
                decorator = [decorator]
            decorator = list(decorator)

            input_view_decorator = InputViewValidator(input_schema)
            decorator.append(input_view_decorator)
            settings['decorator'] = tuple(decorator)

        # Output mapper
        output_schema = settings.pop('output', None)
        use_fields = settings.pop('use_fields', False)
        if output_schema:
            output_view = OutputView(output_schema, use_fields)
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
            if route_name and (input_schema or output_schema):
                schema_manager = context.config.registry.queryUtility(ISchemaViewManager, name=route_name)
                if not schema_manager:
                    schema_manager = SchemaViewManager(route_name)
                    context.config.registry.registerUtility(schema_manager, name=route_name)

                if input_schema:
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

    def __init__(self, schema):
        self.schema = schema
        self.route_name = None
        self.request_method = None

    def __call__(self, wrapped):
        def decorator(context, request):
            context.input_schema = self.schema

            context.structure = {}
            if self.schema:
                structure = get_request_structure(request, self.schema)
                context.structure = self.schema.deserialize(structure) or {}

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


def structure_fields(fields, split_key=None):
    result = set()
    if fields:
        for field in fields:
            if not field:
                continue
            elif split_key:
                fields = field.split(split_key)
                result.update(structure_fields(fields))
            else:
                result.add(uncamelcase(field))
    return result


class InputField(SequenceSchema):
    field = SchemaNode(String(), missing=None)


class InputExcludeField(SequenceSchema):
    exclude_field = SchemaNode(String(), missing=None)


class InputFields(SequenceSchema):
    fields = SchemaNode(String(), missing=None)


class InputExcludeFields(SequenceSchema):
    exclude_field = SchemaNode(String(), missing=None)


class SearchFields(MappingSchema):
    field = InputField()
    exclude_field = InputExcludeField()
    fields = InputFields()
    exclude_fields = InputExcludeFields()


class OutputView(object):
    schema_type = 'response'

    def __init__(self, schema, use_fields=False):
        self.schema = schema
        self.allowed_fields = find_allowed_fields(self.schema)
        if not self.allowed_fields:
            raise Error('output', u'Define output schema')

        self.route_name = None
        self.request_method = None

        self.fields_schema = None
        if use_fields:
            self.fields_schema = SearchFields()

    def __call__(self, wrapped):
        def decorator(context, request):
            context.output_schema = self.schema

            # Construct output fields
            context.include_fields = set()
            context.exclude_fields = set()
            if self.fields_schema:
                fields_structure = get_request_structure(request, self.fields_schema)
                fields = self.fields_schema.deserialize(fields_structure)
                if fields:
                    context.include_fields.update(structure_fields(fields.get('field')))
                    context.include_fields.update(structure_fields(fields.get('fields'), split_key=u','))
                    context.exclude_fields.update(structure_fields(fields.get('exclude_field')))
                    context.exclude_fields.update(structure_fields(fields.get('exclude_fields'), split_key=u','))

            # Define allowed fields
            context.allowed_fields = self.allowed_fields

            # Look and validate requested fields
            if context.include_fields:
                context.output_fields = construct_allowed_fields(self.allowed_fields, context.include_fields)
            else:
                context.output_fields = deepcopy(self.allowed_fields)

            # Exclude fields
            if context.exclude_fields:
                for field in context.exclude_fields:
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
    for key, childrens in fields.items():
        if padding:
            key = '%s.%s' % (padding, key)
        result.add(key)
        if childrens:
            result.update(allowed_fields_to_set(childrens, key))
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
            result[camelcase(child.name)] = construct_output_structure(
                child,
                child_values,
                fields[child.name])

        return result

    else:
        if values is not None:
            values = schema.serialize(values)

        if values is null:
            return None
        elif values is not None:
            if isinstance(schema.typ, Number):
                return schema.typ.num(values)
            elif isinstance(schema.typ, Boolean):
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
    If we request C.F, we ignore brothers of C.F and add C.F childrens
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

    for original_name, childrens in fields_dict.items():
        if padding:
            name = '%s.%s' % (padding, original_name)
        else:
            name = original_name

        if name in requested_fields:
            # If not requested, no others fields will be added in this level
            level_field_added = True

            if childrens:
                # All childrens fields will be added!
                result[original_name] = construct_allowed_fields(
                    childrens,
                    requested_fields,
                    name,
                    add_all=True)
            else:
                result[original_name] = {}

        elif not level_field_added:
            # Add this fields, if no other field added in this level
            post_level_field_add.append((original_name, name, childrens, add_all))

    if not level_field_added and post_level_field_add:
        for original_name, name, childrens, field_add_all in post_level_field_add:
            if childrens:
                result[original_name] = construct_allowed_fields(
                    childrens,
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
                structure = construct_schema_structure(request, schema.schema, schema.schema_type)
                if not schema.request_method:
                    request_methods = DEFAULT_SCHEMA_METHODS
                else:
                    request_methods = [schema.request_method]
                for request_method in request_methods:
                    if not requested_methods or request_method.lower() in requested_methods:
                        nodes[request_method.lower()][schema.schema_type] = structure

        return nodes


def construct_schema_structure(request, schema, schema_type):
    if isinstance(schema, SequenceSchema):
        child = schema.children[0]
        childrens = construct_schema_structure(request, child, schema_type)
        if not is_nonstr_iter(childrens):
            childrens = [childrens]

        details = {
            'type': 'sequence',
            'order': schema._order,
            'childrens': childrens}

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

        if schema_type == 'request':
            details['required'] = schema.missing is colander_required
            if schema.validator:
                schema_validators = schema.validator
                if isinstance(schema_validators, All):
                    schema_validators = schema_validators.validators
                elif not is_nonstr_iter(schema_validators):
                    schema_validators = [schema_validators]

                validators = details['validators'] = []
                for validator in schema_validators:
                    validators.append({
                        'type': get_colander_type_name(validator),
                        'error': validator.msg})
            default = schema.missing
        else:
            details['maybeNotSent'] = schema.missing is colander_drop
            default = schema.serialize()

        if default is not null and default is not colander_required:
            if isinstance(schema.typ, Number):
                default = schema.typ.num(default)
            elif isinstance(schema.typ, Boolean):
                default = asbool(default)
            details['default'] = default

        return details


def get_colander_type_name(node):
    return camelcase(str(node.__class__.__name__).lower())
