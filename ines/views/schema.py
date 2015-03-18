# -*- coding: utf-8 -*-

from urllib2 import unquote

from colander import All
from colander import Boolean as BaseBoolean
from colander import drop
from colander import Length
from colander import Mapping
from colander import null
from colander import Number
from colander import OneOf
from colander import required
from colander import Sequence
from colander import Tuple
from pyramid.compat import is_nonstr_iter
from pyramid.settings import asbool
from zope.interface import implementer

from ines import DEFAULT_METHODS
from ines.convert import camelcase
from ines.convert import force_unicode
from ines.convert import maybe_list
from ines.interfaces import ISchemaView
from ines.views.fields import OneOfWithDescription
from ines.utils import MissingDict


@implementer(ISchemaView)
class SchemaView(object):
    def __init__(self, route_name, routes_names, title=None, description=None, model=None):
        self.route_name = route_name
        self.routes_names = routes_names
        self.title = title
        self.description = description
        self.model = model

    def __call__(self, context, request):
        nodes = MissingDict()
        requested_methods = [key.lower() for key in request.GET.keys()]

        for route_name, request_methods in self.routes_names.items():
            route_methods = []
            for request_method in maybe_list(request_methods or DEFAULT_METHODS):
                if not requested_methods or request_method in requested_methods:
                    route_methods.append(request_method)
            if not route_methods:
                continue

            schemas = request.registry.config.lookup_input_schema(route_name, route_methods)
            schemas.extend(request.registry.config.lookup_output_schema(route_name, route_methods))
            for schema in schemas:
                if schema.schema:
                    structure = self.construct_structure(
                        request,
                        schema.schema,
                        schema.schema_type)
                else:
                    structure = []

                if schema.schema_type == 'request' and schema.fields_schema:
                    fields_structure = self.construct_structure(
                        request,
                        schema.fields_schema,
                        schema.schema_type)

                    if not isinstance(structure, list):
                        structure = [structure]
                    structure.extend(fields_structure)

                for method in route_methods:
                    nodes[method][schema.schema_type] = structure

        return nodes

    def construct_structure(self, request, schema, schema_type):
        if isinstance(schema.typ, Sequence):
            child = schema.children[0]
            children = self.construct_structure(request, child, schema_type)
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

        elif isinstance(schema.typ, Tuple):
            raise NotImplementedError('Tuple type need to be implemented')

        elif isinstance(schema.typ, Mapping):
            result = []
            for child in schema.children:
                result.append(self.construct_structure(request, child, schema_type))
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
                        if isinstance(validator, Length):
                            validation_option = {}
                            if validator.min is not None:
                                validation_option['min'] = validator.min
                            if validator.max is not None:
                                validation_option['max'] = validator.max
                        else:
                            validation_option = True

                        request_validation.append((validator, validation_option))

            if schema_type == 'request':
                validation = {}
                if schema.required:
                    validation['required'] = True

                if request_validation:
                    for validator, validation_option in request_validation:
                        validation[get_colander_type_name(validator)] = validation_option
                if validation:
                    details['validation'] = validation

                default = schema.missing
            else:
                if schema.missing is drop:
                    details['maybeNotSent'] = True
                default = schema.default

            if (default is not drop
                    and default is not required
                    and default is not null):
                if isinstance(schema.typ, Number):
                    default = schema.typ.num(default)
                elif isinstance(schema.typ, BaseBoolean):
                    default = asbool(default)
                details['default'] = default

            return details


def get_colander_type_name(node):
    return camelcase(str(node.__class__.__name__).lower())
