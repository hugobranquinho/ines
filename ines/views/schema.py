# -*- coding: utf-8 -*-

from collections import defaultdict

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
from six import print_
from six import moves
from six import u
from translationstring import TranslationString
from zope.interface import implementer

from ines import DEFAULT_METHODS
from ines import MARKER
from ines.authorization import Everyone
from ines.authorization import NotAuthenticated
from ines.convert import camelcase
from ines.convert import to_unicode
from ines.convert import maybe_list
from ines.interfaces import ISchemaView
from ines.route import lookup_for_route_params
from ines.route import lookup_for_route_permissions
from ines.views.fields import FilterByType
from ines.views.fields import OneOfWithDescription
from ines.utils import different_values


unquote = moves.urllib.parse.unquote
EMPTY_STRING = u('')
UNDERSCORE = u('_')
SPACE = u(' ')


@implementer(ISchemaView)
class SchemaView(object):
    def __init__(
            self,
            schema_route_name,
            route_name=None,
            list_route_name=None,
            title=None,
            description=None,
            csv_route_name=None,
            request_methods=None,
            postman_folder_name=None):

        self.schema_route_name = schema_route_name
        self.route_name = route_name
        self.list_route_name = list_route_name
        self.csv_route_name = csv_route_name
        self.title = title
        self.description = description
        self.request_methods = request_methods or DEFAULT_METHODS
        self.postman_folder_name = postman_folder_name

    def __call__(self, context, request):
        return self.get_schema_nodes(request)

    def get_route_names(self):
        route_names = []
        if self.route_name:
            route_names.append(self.route_name)
        if self.list_route_name:
            route_names.append(self.list_route_name)
        if self.csv_route_name:
            route_names.append(self.csv_route_name)
        return route_names

    def validate_permission(self, request, permissions):
        permissions = maybe_list(permissions)
        if request.authenticated:
            return any((p in permissions for p in request.authenticated.get_principals()))
        else:
            return bool(Everyone in permissions or NotAuthenticated in permissions)

    def get_schema_nodes(self, request):
        cache_key = 'schema build cache %s' % self.schema_route_name
        schema_expire_cache = request.settings.get('schema_expire_cache', MARKER)
        nodes = request.cache.get(cache_key, MARKER, expire=schema_expire_cache)
        if nodes is MARKER:
            nodes = defaultdict(dict)
            global_types = defaultdict(list)
            global_models = defaultdict(list)
            keep_types_keys = defaultdict(set)
            keep_models_keys = defaultdict(set)
            to_translate = defaultdict(list)

            for route_name in self.get_route_names():
                info = self.get_route_info(request, route_name)
                if not info:
                    continue

                intr_route, url, url_keys = info
                url_keys = [camelcase(k) for k in url_keys]
                schemas = request.registry.config.lookup_input_schema(route_name, self.request_methods)
                schemas.extend(request.registry.config.lookup_output_schema(route_name, self.request_methods))

                for schema in schemas:
                    fields = []
                    types = defaultdict(list)
                    models = defaultdict(list)

                    if schema.schema:
                        details = self.construct_structure(
                            request,
                            schema.schema,
                            schema.schema_type,
                            types,
                            models,
                            to_translate)

                        if isinstance(details, dict):
                            fields.append(details)
                        else:
                            fields.extend(details)

                    if schema.schema_type == 'request' and schema.fields_schema:
                        details = self.construct_structure(
                            request,
                            schema.fields_schema,
                            schema.schema_type,
                            types,
                            models,
                            to_translate)

                        if isinstance(details, dict):
                            fields.append(details)
                        else:
                            fields.extend(details)

                    if schema.route_name != self.csv_route_name:
                        key = schema.request_method.lower()
                        if key == 'get' and schema.route_name == self.list_route_name:
                            key = 'list'
                    else:
                        key = 'csv'

                    nodes[key][schema.schema_type] = fields
                    nodes[key]['routeName'] = route_name
                    nodes[key]['method'] = schema.request_method.upper()
                    nodes[key]['url'] = url
                    nodes[key]['urlKeys'] = url_keys
                    nodes[key]['renderer'] = schema.renderer.lower()

                    if types:
                        keep_types_keys[key].update(types.keys())
                        for k, values in types.items():
                            global_types[k].extend(values)
                    if models:
                        keep_models_keys[key].update(models.keys())
                        for k, values in models.items():
                            global_models[k].extend(values)

            if global_types:
                if to_translate:
                    to_translate['fieldTypes'] = defaultdict(set)
                nodes['fieldTypes'] = lookup_common_fields(
                    global_types,
                    to_translate,
                    ignore_key='fieldType',
                    is_field_type=True)
                nodes['keep_types_keys'] = keep_types_keys

            if global_models:
                if to_translate:
                    to_translate['models'] = defaultdict(set)
                nodes['models'] = lookup_common_fields(global_models, to_translate, ignore_key='model')
                nodes['keep_models_keys'] = keep_models_keys

            if to_translate:
                nodes['to_translate'] = to_translate

            request.cache.put(cache_key, nodes, expire=schema_expire_cache)

        permissions_cache = {}
        types_keys = set()
        types = nodes.pop('fieldTypes', None)
        keep_types_keys = nodes.pop('keep_types_keys', None)
        models_keys = set()
        models = nodes.pop('models', None)
        keep_models_keys = nodes.pop('keep_models_keys', None)

        to_translate = nodes.pop('to_translate', None)
        fields_translation = {}
        models_translation = {}
        if to_translate:
            translator = request.translator
            fields_translation = to_translate.pop('fieldTypes', fields_translation)
            models_translation = to_translate.pop('models', fields_translation)

        for key, details in nodes.items():
            route_name = details['routeName']
            if route_name not in permissions_cache:
                info = self.get_route_info(request, route_name)
                permissions_cache[route_name] = lookup_for_route_permissions(request.registry, info[0])

            method_permissions = maybe_list(permissions_cache[route_name].get(details['method']))
            if not self.validate_permission(request, method_permissions):
                nodes.pop(key)
                continue

            if keep_types_keys:
                types_keys.update(keep_types_keys[key])
            if keep_models_keys:
                models_keys.update(keep_models_keys[key])

        if types_keys:
            nodes['fieldTypes'] = {}
            for k in types_keys:
                nodes['fieldTypes'][k] = details = types[k]
                field_fields = fields_translation.get(k)
                if field_fields:
                    for field in field_fields:
                        if field == 'options':
                            for option in details[field]:
                                if option['text']:
                                    option['text'] = translator(option['text'])
                        else:
                            details[field] = translator(details[field])

        if models_keys:
            nodes['models'] = {}
            for k in models_keys:
                nodes['models'][k] = details = models[k]
                model_fields = models_translation.get(k)
                if model_fields:
                    for field in model_fields:
                        if field == 'options':
                            for option in details[field]:
                                if option['text']:
                                    option['text'] = translator(option['text'])
                        else:
                            details[field] = translator(details[field])

        if to_translate:
            for key, values in to_translate.items():
                for value in values:
                    text_value = value.get(key)
                    if text_value:
                        value[key] = translator(text_value)
                        print_(text_value, value[key])

        return nodes

    def get_route_info(self, request, route_name):
        intr_route = request.registry.introspector.get('routes', route_name)
        if intr_route is not None:
            route = intr_route['object']
            params = dict((k, '{{%s}}' % camelcase(k)) for k in lookup_for_route_params(route))
            url = '%s%s' % (request.application_url, unquote(route.generate(params)))
            return intr_route, url, params.keys()

    def construct_structure(self, request, schema, schema_type, types, models, to_translate, parent_name=None):
        if isinstance(schema.typ, Sequence):
            child = schema.children[0]
            if not schema.name:
                schema = child

            name = camelcase(schema.name)
            details = {
                'model': name,
                'type': 'sequence',
                'title': schema.title,
                'description': schema.description or EMPTY_STRING}
            models[name].append(details)

            if isinstance(schema.title, TranslationString):
                to_translate['title'].append(details)
            if isinstance(schema.description, TranslationString):
                to_translate['description'].append(details)

            # Find and add child
            child_details = self.construct_structure(
                request,
                child,
                schema_type,
                types,
                models,
                to_translate,
                parent_name=schema.name)

            if isinstance(details, dict):
                if isinstance(child.typ, Mapping):
                    details['type'] = 'model'
                    details.update(child_details)
                else:
                    details['fields'] = [child_details]
            else:
                details['fields'] = child_details

            return details

        elif isinstance(schema.typ, Tuple):
            raise NotImplementedError('Tuple type need to be implemented')

        elif isinstance(schema.typ, Mapping):
            fields = []
            for child in schema.children:
                fields.append(self.construct_structure(
                    request,
                    child,
                    schema_type,
                    types,
                    models,
                    to_translate,
                    parent_name=schema.name))

            name = schema.name or parent_name
            if not name:
                return fields

            name = camelcase(name)
            details = {
                'type': 'model',
                'title': schema.title,
                'description': schema.description or EMPTY_STRING,
                'fields': fields,
                'model': name}
            models[name].append(details)

            if isinstance(schema.title, TranslationString):
                to_translate['title'].append(details)
            if isinstance(schema.description, TranslationString):
                to_translate['description'].append(details)

            return details

        else:
            name = camelcase(schema.name)
            details = {
                'fieldType': name,
                'title': schema.title,
                'description': schema.description or EMPTY_STRING}

            if isinstance(schema.title, TranslationString):
                to_translate['title'].append(details)
            if isinstance(schema.description, TranslationString):
                to_translate['description'].append(details)

            if hasattr(schema, 'model_reference'):
                model = schema.model_reference['model']
                model_key = schema.model_reference.get('key') or 'key'
                model_query = schema.model_reference.get('query') or 'name'
                model_application = schema.model_reference.get('application_name') or request.application_name
                details['modelReference'] = {
                    'applicationName': model_application,
                    'schemaName': schema.model_reference['schema'],
                    'key': camelcase(model[model_key].name),
                    'model': camelcase(model.name),
                    'queryField': camelcase(model[model_query].name)}

            types[name].append(details)

            if isinstance(schema.typ, FilterByType):
                for cls in schema.typ.__class__.__mro__[1:]:
                    if cls is not FilterByType:
                        details['type'] = str(cls.__name__).lower()
                        break
                details['filter'] = True
            elif hasattr(schema, 'schema_type_name'):
                details['type'] = camelcase(schema.schema_type_name)
            else:
                details['type'] = get_colander_type_name(schema.typ)

            request_validation = []
            if schema.validator:
                if isinstance(schema.validator, All):
                    validators = schema.validator.validators
                elif not is_nonstr_iter(schema.validator):
                    validators = [schema.validator]
                else:
                    validators = schema.validator

                for validator in validators:
                    if isinstance(validator, OneOfWithDescription):
                        details['options'] = []
                        add_option = details['options'].append
                        save_to_translate = False
                        for choice, description in validator.choices_with_descripton:
                            add_option({'value': choice, 'text': description})
                            save_to_translate = save_to_translate or isinstance(description, TranslationString)
                        if save_to_translate:
                            to_translate['options'].append(details)

                    elif isinstance(validator, OneOf):
                        details['options'] = []
                        add_option = details['options'].append
                        for choice in validator.choices:
                            add_option({
                                'value': choice,
                                'text': to_unicode(choice).replace(UNDERSCORE, SPACE).title()})

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

            if hasattr(schema, 'use_when'):
                details['useWhen'] = dict((camelcase(k), v) for k, v in schema.use_when.items())

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
    if hasattr(node, 'schema_type_name'):
        return node.schema_type_name
    else:
        return camelcase(str(node.__class__.__name__).lower())


def lookup_common_fields(values, to_translate, ignore_key=None, is_field_type=False):
    result = defaultdict(dict)

    dict_key = 'models'
    if is_field_type:
        dict_key = 'fieldTypes'

    for name, name_list in values.items():
        if not name_list:
            continue

        all_keys = set()
        for k in name_list:
            all_keys.update(k.keys())
        if ignore_key in all_keys:
            all_keys.remove(ignore_key)

        for key in all_keys:
            check_translation = None
            if key in to_translate:
                check_translation = to_translate[key]

            value = MARKER
            value_idx = None
            for i, name_options in enumerate(name_list):
                other_value = name_options.get(key, MARKER)
                if other_value is MARKER:
                    value_idx = None
                    break
                elif value is MARKER:
                    value = other_value
                    value_idx = i
                elif not different_values(value, other_value):
                    if check_translation:
                        idx = lookup_id(check_translation, name_options)
                        if idx is not None:
                            check_translation.pop(idx)
                    name_options.pop(key)

            if value_idx is not None:
                details = name_list[value_idx]
                if check_translation:
                    idx = lookup_id(check_translation, details)
                    if idx is not None:
                        check_translation.pop(idx)
                        to_translate[dict_key][name].add(key)

                result[name][key] = details.pop(key)

    return result


def lookup_id(values, value):
    for i, v in enumerate(values):
        if id(v) == id(value):
            return i
