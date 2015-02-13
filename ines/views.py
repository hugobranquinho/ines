# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

import datetime

from pyramid.compat import is_nonstr_iter
from pyramid.decorator import reify
from pyramid.httpexceptions import HTTPException
from pyramid.settings import asbool

from ines import MISSING
from ines.convert import camelcase
from ines.convert import force_unicode
from ines.convert import maybe_date
from ines.convert import maybe_datetime
from ines.convert import maybe_integer
from ines.convert import uncamelcase
from ines.exceptions import Error
from ines.utils import format_json_response_values
from ines.utils import maybe_email
from ines.utils import MissingDict
from ines.utils import MissingList
from ines.utils import MissingSet


TODAY_DATE = datetime.date.today


def errors_json_view(context, request):
    if isinstance(context, Error):
        status = 400
        key = context.key
        message = context.message
    elif isinstance(context, HTTPException):
        if str(context.code).startswith('3'):
            # Redirect Code
            return context

        status = context.code
        key = context.title.lower().replace(' ', '_')
        message = context.explanation
    else:
        raise

    values = format_json_response_values(status, key, message)
    return request.render_to_response(
        'json',
        values=values,
        status=status)


VALIDATORS = {
    'integer': {
        'method': maybe_integer,
        'error': u'Invalid integer'},
    'email': {
        'method': maybe_email,
        'error': u'Invalid email'},
    'datetime': {
        'method': maybe_datetime,
        'error': u'Invalid datetime. Use format YYYY-MM-DD HH:MM:SS or YYYY-MM-DD'},
    'date': {
        'method': maybe_date,
        'error': u'Invalid date. Use format YYYY-MM-DD'},
    'boolean': {
        'method': asbool,
        'error': u'Invalid boolean'}}


class DefaultAPIView(object):
    fields_structure = None

    def __init__(self, context, request):
        self.request = request
        self.context = context
        self.api = self.request.api

    @reify
    def GET_dict_of_lists(self):
        return self.request.GET.dict_of_lists()

    @reify
    def POST_dict_of_lists(self):
        return self.request.POST.dict_of_lists()

    @reify
    def DELETE_dict_of_lists(self):
        return self.request.DELETE.dict_of_lists()

    def create_pagination_href(self, route_name, pagination, **params):
        queries = {}
        for key, values in self.GET_dict_of_lists.items():
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
            'nextPageHref': next_href,
            'previousPageHref': previous_href,
            'firstPageHref': first_href,
            'lastPageHref': last_href}

    @reify
    def fields(self):
        if self.external_fields:
            result = set()
            for field in self.external_fields:
                result.add(uncamelcase(field))
            return result or None

    @reify
    def external_fields(self):
        attribute = '%s_dict_of_lists' % self.request.method.upper()
        if not hasattr(self, attribute):
            attribute = 'POST_dict_of_lists'

        kwargs = self.validate_multiples(
            getattr(self, attribute),
            attributes={'fields': ('field', 'fields')},
            ignore_missing=True)

        fields = kwargs.pop('fields', None)
        if fields:
            return fields

    @reify
    def external_no_fields(self):
        attribute = '%s_dict_of_lists' % self.request.method.upper()
        if not hasattr(self, attribute):
            attribute = 'POST_dict_of_lists'

        kwargs = self.validate_multiples(
            getattr(self, attribute),
            attributes={'no_fields': ('-field', '-fields')},
            ignore_missing=True)

        no_fields = kwargs.pop('no_fields', None)
        if no_fields:
            return no_fields

    def extend_attributes(self, structure):
        return []

    def get_structure_attributes(
            self,
            structure,
            padding=None,
            fields=MISSING,
            no_fields=MISSING):

        if fields is MISSING:
            fields = self.external_fields
        if no_fields is MISSING:
            no_fields = self.external_no_fields

        attributes = set()
        for public_key, field in structure.items():
            if padding:
                padding_public_key = padding + u' ' + public_key
            else:
                padding_public_key = public_key

            if no_fields and padding_public_key in no_fields:
                continue
            elif isinstance(field, dict):
                child_fields = fields
                if not fields or padding_public_key in fields:
                    child_fields = None

                attributes.update(
                    self.get_structure_attributes(
                        field,
                        fields=child_fields,
                        no_fields=no_fields,
                        padding=public_key))
            elif not fields or padding_public_key in fields:
                attributes.update(field.attributes)

        attributes.update(self.extend_attributes(structure))
        return attributes

    @reify
    def fields_attributes(self):
        return self.get_structure_attributes(self.fields_structure)

    def _construct_details(
            self, 
            value,
            padding=None,
            fields=MISSING,
            no_fields=MISSING,
            structure=MISSING):

        details = {}
        if not value:
            return details

        if fields is MISSING:
            fields = self.external_fields
        if no_fields is MISSING:
            no_fields = self.external_no_fields
        if structure is MISSING:
            structure = self.fields_structure

        for public_key, field in structure.items():
            if padding:
                padding_public_key = padding + u' ' + public_key
            else:
                padding_public_key = public_key

            if no_fields and padding_public_key in no_fields:
                continue
            elif isinstance(field, dict):
                child_fields = fields
                if not fields or padding_public_key in fields:
                    child_fields = None

                child_details = self._construct_details(
                    value,
                    padding=padding_public_key,
                    structure=field,
                    fields=child_fields,
                    no_fields=no_fields)
                if child_details:
                    details[public_key] = child_details
            elif not fields or padding_public_key in fields:
                details[public_key] = field(self.request, value)

        return details

    def construct_details(self, value):
        return self._construct_details(value)

    def construct_multiple_details(self, values):
        return [self.construct_details(v) for v in values]

    def validate_attributes(
            self,
            request_params,
            attributes,
            ignore_missing=None,
            multiple_values=False,
            value_splitter=None,
            return_empty=None,
            convert_null=False,
            validate_options=None,
            **validators):

        attributes = dict((camelcase(k), k) for k in attributes)
        if ignore_missing is True:
            ignore_missing = attributes.values()

        if multiple_values:
            kwargs = MissingSet()
            def add_value(key, value):
                kwargs[key].add(value)
        else:
            kwargs = {}
            def add_value(key, value):
                kwargs[key] = value

        check_invalid = MissingDict()
        for key, values in validators.items():
            key = key.split('validate_', 1)[1]
            validate_method = VALIDATORS[key]['method']
            for value in set(values):
                check_invalid[value][key] = validate_method

        missing_attributes = []
        invalid = MissingList()
        for params_key, key in attributes.items():
            values = request_params.get(params_key)
            if not is_nonstr_iter(values):
                values = [values]

            add_if_empty = bool(
                return_empty
                and key in return_empty
                and params_key in request_params)

            if value_splitter:
                new_values = set()
                for value in values:
                    if value is not None:
                        new_values.update(value.split(value_splitter))
                    else:
                        new_values.add(None)
                values = new_values

            for value in set(values):
                if value:
                    if convert_null and value == u'NULL':
                        add_value(key, None)
                        continue

                    to_validate = check_invalid[key]
                    if to_validate:
                        for validate_key, method in to_validate.items():
                            value = method(value)
                            if value is None:
                                if add_if_empty:
                                    add_value(key, None)
                                invalid[validate_key].append(params_key)
                            else:
                                add_value(key, value)
                    else:
                        add_value(key, force_unicode(value))

                elif not ignore_missing or key not in ignore_missing:
                    missing_attributes.append(params_key)

                elif add_if_empty:
                    add_value(key, None)

        # Verify if all fields have values
        if missing_attributes:
            keys = u'+'.join(missing_attributes)
            message = u'Required'
            raise Error(keys, message)

        elif invalid:
            key, values = invalid.popitem()
            keys = u'+'.join(values)
            raise Error(keys, VALIDATORS[key]['error'])

        elif validate_options:
            for key, options in validate_options.items():
                if key in kwargs:
                    values = kwargs[key]
                    if not multiple_values:
                        values = [values]

                    for value in values:
                        if value not in options:
                            options = set(options)
                            if None in options:
                                options.remove(None)
                                options.add(u'NULL')
                            message = u'Use %s' % u', '.join(options)
                            raise Error(camelcase(key), message)

        return kwargs

    def validate_multiples(
            self,
            request_params,
            attributes,
            value_splitter=u',',
            **settings):

        ignore_missing = settings.get('ignore_missing')
        if ignore_missing is True:
            ignore_missing = attributes.keys()

        # Extend options
        for validate_key, options in settings.items():
            if options:
                if validate_key == 'validate_options':
                    for key, option_values in options.items():
                        info = attributes.get(key)
                        if info:
                            options[info[0]] = option_values
                            options[info[1]] = option_values

                elif validate_key.startswith('validate_'):
                    for option in list(options):
                        info = attributes.get(option)
                        if info:
                            options.extend(info)

                elif validate_key == 'ignore_missing' and options is not True:
                    for option in list(options):
                        info = attributes.get(option)
                        if info:
                            options.extend(info)

        kwargs = MissingSet()
        for key, (normal_key, splitter_key) in attributes.items():
            for values in self.validate_attributes(
                    request_params,
                    [normal_key],
                    multiple_values=True,
                    **settings).values():
                kwargs[key].update(values)

            for values in self.validate_attributes(
                    request_params,
                    [splitter_key],
                    multiple_values=True,
                    value_splitter=value_splitter,
                    **settings).values():
                kwargs[key].update(values)

            if (not kwargs.get(key)
                and (not ignore_missing or key not in ignore_missing)):
                keys = '|'.join([normal_key, splitter_key])
                raise Error(keys, u'Required')

        return kwargs

    def validate(
            self,
            request_params,
            singles=None,
            multiples=None,
            **settings):

        kwargs = MissingSet()
        if singles:
            for key, value in self.validate_attributes(
                    request_params,
                    singles,
                    multiple_values=False,
                    **settings).items():
                kwargs[key] = value

        if multiples:
            for key, values in self.validate_multiples(
                    request_params,
                    multiples,
                    **settings).items():
                kwargs[key].update(values)

        return kwargs


class FormatResponse(object):
    def __init__(self, structure, fields=None, padding=''):
        self.structure = structure
        self.fields = fields
        self.padding = padding
        self.methods = {}
        self.attributes = set()

        for public_key, field in self.structure.items():
            if self.padding:
                padding_public_key = self.padding + ' ' + public_key
            else:
                padding_public_key = public_key

            if isinstance(field, dict):
                child_fields = self.fields
                if not self.fields or padding_public_key in self.fields:
                    child_fields = None

                formater = FormatResponse(
                    field,
                    fields=child_fields,
                    padding=padding_public_key)

                if formater.methods:
                    self.attributes.update(formater.attributes)
                    self.methods[public_key] = formater
            else:
                if not self.fields or padding_public_key in self.fields:
                    self.attributes.update(field.attributes)
                    self.methods[public_key] = field

        if not self.methods and u'key' in self.structure:
            field = self.structure[u'key']
            self.attributes.update(field.attributes)
            self.methods[u'key'] = field

    def __call__(self, request, value):
        result = dict(
            (public_key, method(request, value))
            for public_key, method in self.methods.items())

        if self.padding:
            for value in result.values():
                if value is not None:
                    break
            else:
                result = None

        return result


class Field(object):
    def __init__(self, name, attributes=None):
        self.name = name

        if attributes and not is_nonstr_iter(attributes):
            attributes = [attributes]
        self.attributes = set(attributes or [self.name])

    def __call__(self, request, value):
        return getattr(value, self.name)


class DateTimeField(Field):
    def __call__(self, request, value):
        value = getattr(value, self.name)
        if value:
            return value.isoformat()


class AgeField(Field):
    def __init__(self, birthday_attribute, name='age'):
        self.name = name
        self.birthday_attribute = birthday_attribute
        self.attributes = set([birthday_attribute])

    def __call__(self, request, value):
        value = getattr(value, self.birthday_attribute)
        if value:
            today = TODAY_DATE()
            age = today.year - value.year
            if (today.month < value.month
                or (today.month == value.month and today.day < value.day)):
                age -= 1
            return age


class HrefField(Field):
    def __init__(self, route_name, attribute, value_attribute):
        self.route_name = route_name
        self.attribute = attribute
        super(HrefField, self).__init__(value_attribute, value_attribute)

    def __call__(self, request, value):
        key = getattr(value, self.name)
        if key:
            kwargs = {self.attribute: key}
            return request.route_url(self.route_name, **kwargs)
