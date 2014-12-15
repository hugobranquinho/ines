# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

from translationstring import TranslationString

from ines.utils import MissingList


class Error(Exception):
    def __init__(self, key, message, exception=None):
        Exception.__init__(self, message)

        self.key = key
        self.message = message
        self.childrens = []
        self.exception = exception

    def __iter__(self):
        yield (self.key, self.message)

        if self.childrens:
            for children in self.childrens:
                for children_key, children_message in children:
                    yield (children_key, children_message)

    def __setitem__(self, key, message):
        error = Error(key, message)
        self.add(error)

    def add(self, error):
        self.childrens.append(error)
        return self

    def aslist(self, request=None):
        if not request or not hasattr(request, 'translator'):
            return list(self)

        errors = []
        translator = request.translator
        for key, value in self:
            if isinstance(value, TranslationString):
                value = translator(value)
            result.append((key, value))

        return errors

    def asdict(self, request=None):
        errors = MissingList()
        for key, value in self.aslist(request):
            errors[key].append(value)
        return errors
