# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
# 
# @author Hugo Branquinho <hugobranq@gmail.com>
# 
# $Id$

from colander import Boolean
from colander import Date
from colander import Float
from colander import Integer
from colander import Invalid
from colander import null
from colander import SchemaNode
from colander import String
from deform.widget import CheckboxChoiceWidget
from deform.widget import CheckboxWidget
from deform.widget import DateInputWidget
from deform.widget import FileUploadWidget
from deform.widget import PasswordWidget
from deform.widget import SelectWidget
from deform.widget import TextAreaWidget
from deform.widget import TextInputWidget
from deform.widget import Widget

from ines.exceptions import APIError
from ines.utils import cache_property
from ines.utils import MissingList


class MemoryTmpStore(dict):
    def preview_url(self, uid):
        return None

TEMPORARY_STORAGE = MemoryTmpStore()


class FormAPIError(APIError):
    def asnode(self, node):
        errors = self.aslist()
        if not errors:
            raise Invalid(node, self.msg)
        else:
            errors_dict = MissingList()
            for key, message, error_class in errors:
                errors_dict[key].append(message)

            error = Invalid(node, self.msg)
            error_names = [c.name for c in node.children]
            for key, messages in errors_dict.items():
                if key in error_names:
                    error[key] = messages

            raise error


class FormStructure(object):
    def __init__(self, form_session, request):
        self.form_session = form_session
        self.settings = self.form_session.settings
        self.request = request

    @cache_property
    def error_class(self):
        return self.settings.get('form_error_class') or 'error'

    def required_string_schema(self, *args, **kwargs):
        return RequiredStringSchemaNode(*args, **kwargs)

    def required_integer_schema(self, *args, **kwargs):
        return RequiredIntegerSchemaNode(*args, **kwargs)

    def required_float_schema(self, *args, **kwargs):
        return RequiredFloatSchemaNode(*args, **kwargs)

    def required_date_schema(self, *args, **kwargs):
        return RequiredDateSchemaNode(*args, **kwargs)

    def required_boolean_schema(self, *args, **kwargs):
        return RequiredBooleanSchemaNode(*args, **kwargs)

    @cache_property
    def base_text_input(self):
        def constructer(*args, **kwargs):
            kwargs['error_class'] = self.error_class
            return TextInputWidget(*args, **kwargs)
        return constructer

    def text_input(self, *args, **kwargs):
        return self.base_text_input(*args, **kwargs)

    @cache_property
    def base_password_input(self):
        def constructer(*args, **kwargs):
            kwargs['error_class'] = self.error_class
            return PasswordWidget(*args, **kwargs)
        return constructer

    def password_input(self, *args, **kwargs):
        return self.base_password_input(*args, **kwargs)

    @cache_property
    def base_text_area(self):
        def constructer(*args, **kwargs):
            kwargs['error_class'] = self.error_class
            return TextAreaWidget(*args, **kwargs)
        return constructer

    def text_area(self, *args, **kwargs):
        return self.base_text_area(*args, **kwargs)

    @cache_property
    def base_select(self):
        def constructer(*args, **kwargs):
            kwargs['error_class'] = self.error_class
            return SelectWidget(*args, **kwargs)
        return constructer

    def select(self, *args, **kwargs):
        return self.base_select(*args, **kwargs)

    @cache_property
    def base_date_input(self):
        def constructer(*args, **kwargs):
            kwargs['error_class'] = self.error_class
            return DateInputWidget(*args, **kwargs)
        return constructer

    def date_input(self, *args, **kwargs):
        return self.base_date_input(*args, **kwargs)

    @cache_property
    def base_checkchoice(self):
        def constructer(*args, **kwargs):
            kwargs['error_class'] = self.error_class
            return CheckboxChoiceWidget(*args, **kwargs)
        return constructer

    def checkchoice(self, *args, **kwargs):
        return self.base_checkchoice(*args, **kwargs)

    @cache_property
    def base_checkbox(self):
        def constructer(*args, **kwargs):
            kwargs['error_class'] = self.error_class
            return CheckboxWidget(*args, **kwargs)
        return constructer

    def checkbox(self, *args, **kwargs):
        return self.base_checkbox(*args, **kwargs)

    @cache_property
    def base_file_upload(self):
        def constructer(*args, **kwargs):
            kwargs['error_class'] = self.error_class
            return FileUploadWidget(TEMPORARY_STORAGE, *args, **kwargs)
        return constructer

    def file_upload(self, **kwargs):
        return self.base_file_upload(**kwargs)

    @cache_property
    def base_recaptcha(self):
        RecaptchaClass = self.settings.get('recaptcha')
        if not RecaptchaClass:
            message = 'reCaptcha is not implemented'
            raise NotImplementedError(message)

        def constructer(**kwargs):
            kwargs['error_class'] = self.error_class
            theme_name = self.settings.get('recaptcha_theme')
            kwargs['html_value'] = RecaptchaClass.html(self.request, theme_name)
            return EmptyWidget(**kwargs)
        return constructer

    def recaptcha(self, **kwargs):
        return self.base_recaptcha(**kwargs)


class EmptyWidget(Widget):
    template = 'empty'
    readonly_template = 'empty'
    html_value = ''
    return_value = '<--empty-->'

    def serialize(self, field, cstruct, readonly=False):
        template = readonly and self.readonly_template or self.template
        return field.renderer(template,
                              field=field,
                              cstruct=cstruct,
                              html_value=self.html_value)

    def deserialize(self, field, pstruct):
        return self.return_value


class RequiredSchemaNode(SchemaNode):
    def __init__(self, *args, **kwargs):
        if not kwargs.has_key('missing'):
            kwargs['missing'] = null
            kwargs['required_icon'] = True

        return SchemaNode.__init__(self, *args, **kwargs)


class RequiredStringSchemaNode(RequiredSchemaNode):
    @staticmethod
    def schema_type():
        return String()

    def __init__(self, *args, **kwargs):
        return RequiredSchemaNode.__init__(self, *args, **kwargs)


class RequiredIntegerSchemaNode(RequiredStringSchemaNode):
    @staticmethod
    def schema_type():
        return Integer()


class RequiredFloatSchemaNode(RequiredStringSchemaNode):
    @staticmethod
    def schema_type():
        return Float()


class RequiredDateSchemaNode(RequiredStringSchemaNode):
    @staticmethod
    def schema_type():
        return Date()


class RequiredBooleanSchemaNode(RequiredStringSchemaNode):
    @staticmethod
    def schema_type():
        return Boolean()
