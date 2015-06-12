# -*- coding: utf-8 -*-

from functools import wraps

from colander import Mapping
from colander import Sequence
from colander import Tuple
from pyramid.compat import is_nonstr_iter
from zope.interface import implementer

from ines.convert import camelcase
from ines.convert import maybe_integer
from ines.convert import maybe_list
from ines.convert import uncamelcase
from ines.interfaces import IInputSchemaView
from ines.utils import MissingDict
from ines.views.fields import SearchFields


@implementer(IInputSchemaView)
class InputSchemaView(object):
    schema_type = 'request'

    def __init__(
            self, route_name, request_method, schema=None, use_fields=False,
            auto_camelcase=True):
        self.route_name = route_name
        self.request_method = request_method

        self.schema = schema
        self.fields_schema = None
        if use_fields:
            self.fields_schema = SearchFields()
        self.auto_camelcase = auto_camelcase

    def __call__(self, wrapped):
        @wraps(wrapped)
        def wrapper(context, request):
            context.input_schema = self.schema

            context.structure = {}
            if self.schema:
                structure = self.get_structure(request, self.schema)
                context.structure = self.schema.deserialize(structure) or {}

            # Construct output fields
            context.include_fields = set()
            context.exclude_fields = set()
            if self.fields_schema:
                fields_structure = self.get_structure(request, self.fields_schema)
                fields = self.fields_schema.deserialize(fields_structure)
                if fields:
                    context.include_fields.update(
                        self.decode_key(f) for f in fields.get('field', []))
                    context.include_fields.update(
                        self.decode_key(f) for f in fields.get('fields', []))
                    context.exclude_fields.update(
                        self.decode_key(f) for f in fields.get('exclude_field', []))
                    context.exclude_fields.update(
                        self.decode_key(f) for f in fields.get('exclude_fields', []))

            return wrapped(context, request)
        return wrapper

    def decode_key(self, key):
        return uncamelcase(key) if self.auto_camelcase else key

    def encode_key(self, key):
        return camelcase(key) if self.auto_camelcase else key

    def get_structure(self, request, schema):
        method = request.method.upper()
        if method in ('PUT', 'POST'):
            request_values = request.POST.dict_of_lists()
        elif method == 'DELETE':
            request_values = request.DELETE.dict_of_lists()
        else:
            request_values = request.GET.dict_of_lists()

        return self.construct_structure(schema, request_values)

    def construct_structure(self, schema, values, padding=None):
        name = self.encode_key(schema.name)
        if padding and name:
            name = '%s.%s' % (padding, name)

        if isinstance(schema.typ, Sequence):
            child = schema.children[0]
            find_exact_name = not isinstance(child.typ, (Sequence, Tuple, Mapping))

            result = []
            for values in construct_sequence_items(name, values):
                value = self.construct_structure(child, values, padding=name)
                if value is not None:
                    result.append(value)

                if find_exact_name:
                    exact_value = values.get(name)
                    if exact_value:
                        result.append(exact_value)

            return result

        elif isinstance(schema.typ, Tuple):
            raise NotImplementedError('Tuple type need to be implemented')

        elif isinstance(schema.typ, Mapping):
            result = {}
            for child in schema.children:
                value = self.construct_structure(child, values, padding=name)
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

        value = maybe_list(value)
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
