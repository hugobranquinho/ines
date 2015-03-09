# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

from urllib2 import unquote

from colander import All
from colander import Boolean as BaseBoolean
from colander import drop as colander_drop
from colander import MappingSchema
from colander import null
from colander import Number
from colander import OneOf
from colander import required as colander_required
from colander import SequenceSchema
from colander import TupleSchema
from pyramid.compat import is_nonstr_iter
from pyramid.settings import asbool
from zope.interface import implementer

from ines import DEFAULT_METHODS
from ines.convert import camelcase
from ines.convert import force_unicode
from ines.convert import maybe_list
from ines.interfaces import ISchemaView
from ines.route import lookup_for_route_params
from ines.views.fields import OneOfWithDescription


@implementer(ISchemaView)
class SchemaView(object):
    def __init__(self, route_name, routes_names, title=None):
        self.route_name = route_name
        self.routes_names = routes_names
        self.title = title

    def __call__(self, context, request):
        model = {}
        actions = []
        requested_methods = [key.lower() for key in request.GET.keys()]
        config = request.registry.config

        for route_name, request_methods in self.routes_names.items():
            route_methods = []
            for request_method in maybe_list(request_methods or DEFAULT_METHODS):
                if not requested_methods or request_method in requested_methods:
                    route_methods.append(request_method)
            if not route_methods:
                continue

            # Make route for url
            intr_route = request.registry.introspector.get('routes', route_name)
            if intr_route is None:
                continue
            route = intr_route['object']
            params = dict((k, '{{%s}}' % camelcase(k)) for k in lookup_for_route_params(route))
            url = '%s%s' % (request.application_url, unquote(route.generate(params)))

            # Lookup for input and output schemas
            schemas = config.lookup_input_schema(route_name, route_methods)
            schemas.extend(config.lookup_output_schema(route_name, route_methods))
            for schema in schemas:
                is_input_schema = schema.schema_type == 'request'

                fields = []
                if schema.schema:
                    fields.extend(self.construct_structure(
                        request,
                        model,
                        schema.schema,
                        is_input_schema))

                if is_input_schema and schema.fields_schema:
                    fields.extend(self.construct_structure(
                        request,
                        model,
                        schema.fields_schema,
                        is_input_schema))

                if is_input_schema:
                    for req_method in maybe_list(schema.request_method or route_methods):
                        actions.append({
                            'method': req_method.upper(),
                            'href': url,
                            'fields': fields})

        return {
            'model': model.values(),
            'actions': actions}

    def construct_structure(self, request, model, schema, is_input_schema):
        if isinstance(schema, SequenceSchema):
            child = schema.children[0]
            if not schema.name:
                info = child
            else:
                info = schema

            if not model:
                model.update({
                    'type': 'sequence',
                    'name': camelcase(info.name),
                    'title': info.title,
                    'description': info.description or None,
                    'order': info._order})

            # Update or add children
            children_model = model.get('children')
            if not children_model:
                children_model = model['children'] = {}

            return self.construct_structure(request, children_model, child, is_input_schema)

        elif isinstance(schema, TupleSchema):
            raise NotImplementedError('TupleSchema need to be implemented')

        elif isinstance(schema, MappingSchema):
            result = []
            for child in schema.children:
                result.append(self.construct_structure(request, model, child, is_input_schema))
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

            if is_input_schema:
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


"""

    def construct_structure(self, request, model, schema, is_input_schema):
        if isinstance(schema, SequenceSchema):
            child = schema.children[0]
            if not schema.name:
                info = child
            else:
                info = schema

            if not model:
                model.update({
                    'type': 'sequence',
                    'name': camelcase(info.name),
                    'title': info.title,
                    'description': info.description or None,
                    'order': info._order})

            # Update or add children
            children_model = model.get('children')
            if not children_model:
                children_model = model['children'] = {}

            return self.construct_structure(request, children_model, child, is_input_schema)

        elif isinstance(schema, TupleSchema):
            raise NotImplementedError('TupleSchema need to be implemented')

        elif isinstance(schema, MappingSchema):
            result = []
            for child in schema.children:
                result.append(self.construct_structure(request, model, child, is_input_schema))
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

            if is_input_schema:
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
"""