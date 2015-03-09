# -*- coding: utf-8 -*-

from copy import deepcopy

from colander import Boolean
from colander import drop
from colander import MappingSchema
from colander import null
from colander import Number
from colander import SequenceSchema
from colander import TupleSchema
from pyramid.settings import asbool
from zope.interface import implementer

from ines.convert import camelcase
from ines.exceptions import Error
from ines.interfaces import IOutputSchemaView
from ines.views.fields import node_is_iterable


@implementer(IOutputSchemaView)
class OutputSchemaView(object):
    schema_type = 'response'

    def __init__(self, route_name, request_method, schema):
        self.route_name = route_name
        self.request_method = request_method
        self.schema = schema
        self.allowed_fields = self.find_allowed_fields(self.schema)
        if not self.allowed_fields:
            raise Error('output', u'Define output schema')

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
                    if '.' not in field:
                        context.output_fields.pop(field, None)
                    else:
                        field_blocks = field.split('.')
                        previous = context.output_fields
                        for field_block in field_blocks[:-1]:
                            previous = previous.get(field_block)
                            if not previous:
                                break
                        if previous:
                            previous.pop(field_blocks[-1])

            if not context.output_fields:
                keys = u'+'.join(self.allowed_fields_to_set(self.allowed_fields))
                raise Error(keys, u'Please define some fields to export')
            context.fields = deepcopy(context.output_fields)

            result = wrapped(context, request)
            return self.construct_structure(self.schema, result, context.output_fields)
        return decorator

    def find_allowed_fields(self, schema, padding=None):
        if padding:
            name = '%s.%s' % (padding, schema.name)
        else:
            name = schema.name

        allowed_fields = {}
        if isinstance(schema, SequenceSchema):
            for child in schema.children:
                allowed_fields.update(self.find_allowed_fields(child, name))

        elif node_is_iterable(schema):
            for child in schema.children:
                allowed_fields[child.name] = self.find_allowed_fields(child, name)

        return allowed_fields

    def encode_key(self, key):
        return camelcase(key)

    def construct_structure(self, schema, values, fields):
        if isinstance(schema, SequenceSchema):
            result = []
            if values is None:
                return result
            elif values is null:
                return []

            child = schema.children[0]
            for value in values:
                child_value = self.construct_structure(child, value, fields)
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
                value = self.construct_structure(
                    child,
                    child_values,
                    fields[child.name])
                if value is not None or child.missing is not drop:
                    result[self.encode_key(child.name)] = value

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

    def allowed_fields_to_set(self, fields, padding=None):
        result = set()
        for key, children in fields.items():
            if padding:
                key = '%s.%s' % (padding, key)
            result.add(key)
            if children:
                result.update(self.allowed_fields_to_set(children, key))
        return result


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
