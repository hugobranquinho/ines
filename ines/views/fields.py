# -*- coding: utf-8 -*-

from colander import Boolean as BaseBoolean
from colander import drop as colander_drop
from colander import DateTime as BaseDateTime
from colander import Integer
from colander import Mapping
from colander import null
from colander import OneOf
from colander import SchemaNode as BaseSchemaNode
from colander import Sequence
from colander import SequenceSchema
from colander import String
from colander import Tuple
from colander.compat import is_nonstr_iter

from ines import _
from ines import FALSES
from ines import TRUES


class SchemaNode(BaseSchemaNode):
    def __init__(self, *arg, **kw):
        self.return_none_if_defined = kw.pop('return_none_if_defined', False)
        super(SchemaNode, self).__init__(*arg, **kw)

    def deserialize(self, cstruct=null):
        appstruct = BaseSchemaNode.deserialize(self, cstruct)

        # Return None, only if request and cstruct is empty
        if (self.return_none_if_defined
                and (appstruct is null or appstruct is colander_drop)
                and cstruct is not null and not cstruct):
            return None

        if hasattr(self, 'after_deserialize'):
            if is_nonstr_iter(self.after_deserialize):
                for func in self.after_deserialize:
                    appstruct = func(appstruct)
            else:
                appstruct = self.after_deserialize(appstruct)

        return appstruct

    def clone(self, **kwargs):
        cloned = BaseSchemaNode.clone(self)
        cloned.__dict__.update(kwargs)
        cloned._order = next(cloned._counter)
        return cloned


class Schema(SchemaNode):
    schema_type = Mapping

MappingSchema = Schema


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

    def deserialize(self, node, cstruct):
        if cstruct == '':
            return null
        else:
            return super(Boolean, self).deserialize(node, cstruct)


class InputField(SequenceSchema):
    field = SchemaNode(String(), missing=None)


class InputExcludeField(SequenceSchema):
    exclude_field = SchemaNode(String(), missing=None)


class InputFields(SequenceSchema):
    fields = SchemaNode(String(), missing=None)


class InputExcludeFields(SequenceSchema):
    exclude_fields = SchemaNode(String(), missing=None)


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


def node_is_iterable(node):
    if hasattr(node, 'schema_type'):
        return node.schema_type in (Tuple, Mapping, Sequence)
    else:
        return False


# Global attributes
PAGE = SchemaNode(Integer(), title=_(u'Page'), missing=1)
LIMIT_PER_PAGE = SchemaNode(Integer(), title=_(u'Results per page'), missing=20)
NUMBER_OF_RESULTS = SchemaNode(Integer(), title=_(u'Number of results'))
LAST_PAGE = SchemaNode(Integer(), title=_(u'Last page'))
NEXT_PAGE_HREF = SchemaNode(String(), title=_(u'Next page url'))
PREVIOUS_PAGE_HREF = SchemaNode(String(), title=_(u'Previous page url'))
FIRST_PAGE_HREF = SchemaNode(String(), title=_(u'First page url'))
LAST_PAGE_HREF = SchemaNode(String(), title=_(u'Last page url'))


class PaginationInput(MappingSchema):
    page = PAGE.clone(missing=1)
    limit_per_page = LIMIT_PER_PAGE.clone(missing=20)


class PaginationOutput(MappingSchema):
    page = PAGE
    limit_per_page = LIMIT_PER_PAGE
    last_page = LAST_PAGE
    number_of_results = NUMBER_OF_RESULTS
    next_page_href = NEXT_PAGE_HREF
    previous_page_href = PREVIOUS_PAGE_HREF
    first_page_href = FIRST_PAGE_HREF
    last_page_href = LAST_PAGE_HREF


class DeleteOutput(MappingSchema):
    deleted = SchemaNode(Boolean(), title=_(u'Deleted'))
