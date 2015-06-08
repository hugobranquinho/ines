# -*- coding: utf-8 -*-

import datetime
from time import time as NOW_TIME
from urllib2 import quote
from urllib2 import unquote
from uuid import uuid4

from colander import Boolean
from colander import Date
from colander import DateTime
from colander import drop
from colander import Mapping
from colander import null
from colander import OneOf
from colander import required
from colander import Sequence
from colander import String
from colander import Tuple
from pyramid.authorization import Everyone
from pyramid.compat import is_nonstr_iter

from ines import DEFAULT_METHODS
from ines.authorization import NotAuthenticated
from ines.convert import camelcase
from ines.convert import maybe_list
from ines.interfaces import ISchemaView
from ines.route import lookup_for_route_params
from ines.route import lookup_for_route_permissions
from ines.views.fields import File
from ines.utils import MissingList


TODAY_DATE = datetime.date.today
NOW_DATE = datetime.datetime.now


class PostmanCollection(object):
    def __init__(self, title, description=None):
        self.title = title
        self.description = description
        self.collection_id = self.new_unique_id()
        self.collection_time = int(NOW_TIME())

    def new_unique_id(self):
        return str(uuid4())

    def __call__(self, context, request):
        requests = []
        folders = MissingList()
        folders_descriptions = {}
        config = request.registry.config

        for schema_view in request.registry.getAllUtilitiesRegisteredFor(ISchemaView):
            # Make route for url
            intr_route = request.registry.introspector.get('routes', schema_view.route_name)
            if intr_route is not None:
                # Schema permission
                headers = set()
                permissions = lookup_for_route_permissions(request.registry, intr_route)
                method_permissions = maybe_list(permissions.get('GET'))
                for method_permission in method_permissions:
                    if method_permission not in (Everyone, NotAuthenticated):
                        headers.add('Authorization: Token {{token}}')
                        break

                request_id = self.new_unique_id()
                requests.append({
                    'id': request_id,
                    'headers': '\n'.join(headers),
                    'url': request.route_url(schema_view.route_name),
                    'preRequestScript': '',
                    'pathVariables': {},
                    'method': u'GET',
                    'data': [],
                    'dataMode': 'params',
                    'version': 2,
                    'tests': '',
                    'currentHelper': 'normal',
                    'helperAttributes': {},
                    'time': self.collection_time,
                    'name': u'Schema: %s' % schema_view.title,
                    'description': schema_view.description or '',
                    'collectionId': self.collection_id,
                    'responses': [],
                    'owner': 0,
                    'synced': False})
                folders[schema_view.title].append(request_id)
                folders_descriptions[schema_view.title] = schema_view.description

            for route_name, request_methods in schema_view.routes_names.items():
                # Make route for url
                intr_route = request.registry.introspector.get('routes', route_name)
                if intr_route is None:
                    continue
                route = intr_route['object']
                permissions = lookup_for_route_permissions(request.registry, intr_route)
                params = dict((k, '{{%s}}' % camelcase(k)) for k in lookup_for_route_params(route))
                url = '%s%s' % (request.application_url, unquote(route.generate(params)))

                schemas_by_methods = MissingList()
                for schema in config.lookup_input_schema(route_name, request_methods):
                    for request_method in maybe_list(schema.request_method) or DEFAULT_METHODS:
                        schemas_by_methods[request_method].append(schema)
                for schema in config.lookup_output_schema(route_name, request_methods):
                    for request_method in maybe_list(schema.request_method) or DEFAULT_METHODS:
                        schemas_by_methods[request_method].append(schema)

                for request_method, schemas in schemas_by_methods.items():
                    title = None
                    description = None
                    schema_data = []
                    tests = []

                    for schema in schemas:
                        if schema.schema_type == 'request':
                            if schema.schema:
                                schema_data.extend(
                                    self.construct_data(request_method, schema.schema))
                            if schema.fields_schema:
                                schema_data.extend(
                                    self.construct_data(request_method, schema.fields_schema))

                        if schema.schema and not title:
                            title = schema.schema.title
                        if schema.schema and not description:
                            description = schema.schema.description

                        variables = getattr(schema.schema, 'postman_environment_variables', None)
                        if variables:
                            for environment_key, response_key in variables.items():
                                environment_key = camelcase(environment_key)
                                response_key = camelcase(response_key)
                                tests.append((
                                    'if(answer.%s){postman.setEnvironmentVariable("%s", answer.%s);}'
                                    % (response_key, environment_key, response_key)))

                        first_key_for = getattr(schema.schema, 'postman_environment_set_first_key', None)
                        if first_key_for:
                            tests.append((
                                'if(answer){for(var k in answer){postman.setEnvironmentVariable("%s", k);break;}}'
                                % camelcase(first_key_for)))

                    if tests:
                        tests.insert(0, 'var answer = JSON.parse(responseBody);')

                    # Input params
                    method_url = url
                    request_schema_data = []
                    request_method = request_method.upper()
                    if request_method == 'GET':
                        queries = []
                        for url_param in schema_data:
                            if url_param['value']:
                                queries.append(
                                    '%s=%s'
                                    % (quote(url_param['key']), quote(url_param['value'])))
                            else:
                                queries.append(quote(url_param['key']))
                        if queries:
                            method_url = '%s?%s' % (method_url, '&'.join(queries))
                    else:
                        request_schema_data = schema_data

                    # Method permission
                    headers = set()
                    method_permissions = maybe_list(permissions.get(request_method))
                    for method_permission in method_permissions:
                        if method_permission not in (Everyone, NotAuthenticated):
                            headers.add('Authorization: Token {{token}}')
                            break

                    if not title:
                        title = route_name.replace('_', ' ').title()

                    request_id = self.new_unique_id()
                    requests.append({
                        'id': request_id,
                        'headers': '\n'.join(headers),
                        'url': method_url,
                        'preRequestScript': '',
                        'pathVariables': {},
                        'method': request_method,
                        'data': request_schema_data,
                        'dataMode': 'params',
                        'version': 2,
                        'tests': '\n'.join(tests),
                        'currentHelper': 'normal',
                        'helperAttributes': {},
                        'time': self.collection_time,
                        'name': title,
                        'description': description or '',
                        'collectionId': self.collection_id,
                        'responses': [],
                        'owner': 0,
                        'synced': False})
                    folders[schema_view.title].append(request_id)

        response_folders = []
        for key, requests_ids in folders.items():
            response_folders.append({
                'id': self.new_unique_id(),
                'name': key,
                'description': folders_descriptions.get(key) or '',
                'order': requests_ids,
                'collection_name': self.title,
                'collection_id': self.collection_id,
                'collection_owner': '',
                'write': True})

        return {
            'id': self.collection_id,
            'name': self.title,
            'description': self.description or '',
            'timestamp': self.collection_time,
            'synced': False,
            'owner': '',
            'subscribed': False,
            'remoteLink': '',
            'public': False,
            'write': True,
            'order': [],
            'folders': response_folders,
            'requests': requests}

    def construct_data(self, request_method, schema, keep_parent_name=None):
        response = []
        if isinstance(schema.typ, Sequence):
            child = schema.children[0]
            response.extend(self.construct_data(
                request_method,
                child,
                keep_parent_name=schema.name))
            return response

        elif isinstance(schema.typ, Tuple):
            for child in schema.children:
                response.extend(
                    self.construct_data(
                        request_method,
                        child,
                        keep_parent_name=schema.name))
            return response

        elif isinstance(schema.typ, Mapping):
            for child in schema.children:
                response.extend(
                    self.construct_data(
                        request_method,
                        child))
            return response

        else:
            if hasattr(schema.typ, 'typ'):
                return self.construct_data(
                    request_method,
                    schema.typ,
                    keep_parent_name=keep_parent_name)

            default = schema.serialize()
            if default is null:
                default = ''
                if schema.missing not in (drop, required, null):
                    default = schema.missing

            if hasattr(schema, 'postman_default'):
                default = schema.postman_default

            if default == '' and schema.missing is required and request_method.upper() == 'POST':
                if not schema.validator:
                    if isinstance(schema.typ, String):
                        default = '%s {{$randomInt}}' % schema.name.replace('_', ' ').title()
                    elif isinstance(schema.typ, Boolean):
                        default = 'true'
                    elif isinstance(schema.typ, Date):
                        default = TODAY_DATE()
                    elif isinstance(schema.typ, DateTime):
                        default = NOW_DATE()
                else:
                    validators = schema.validator
                    if not is_nonstr_iter(validators):
                        validators = [validators]

                    for validator in validators:
                        if isinstance(validator, OneOf):
                            default = validator.choices[0]
                            break

            if default is None:
                default = ''

            if isinstance(schema.typ, File):
                item_type = 'file'
            else:
                item_type = 'text'

            response.append({
                'key': camelcase(keep_parent_name or schema.name),
                'value': str(default),
                'type': item_type,
                'enabled': schema.required})
            return response
