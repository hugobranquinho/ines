# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
# 
# @author Hugo Branquinho <hugobranq@gmail.com>
# 
# $Id$

from translationstring import TranslationString

from ines.utils import MissingList


class Error(Exception):
    def __init__(self, key, message, error_class=None, exception_error=None):
        Exception.__init__(self, message)

        self.key = key
        self.msg = message
        self.error_class = error_class
        self.childrens = []
        self.exception_error = exception_error

    @property
    def have_childrens(self):
        return bool(self.childrens)

    def add(self, key, message, error_class=None, exception_error=None):
        error = Error(key, message, error_class)
        self.childrens.append(error)
        return self

    def aslist(self):
        errors = [(self.key, self.msg, self.error_class)]
        if self.childrens:
            for children in self.childrens:
                errors.extend(children.aslist())
        return errors

    def asdict(self, request=None):
        result = MissingList()
        errors = self.aslist()
        if errors:
            if request:
                translator = request.translator
                for key, message, error_class in errors:
                    if isinstance(message, TranslationString):
                        message = translator(message)
                    result[key].append(message)
            else:
                for key, message, error_class in errors:
                    result[key].append(message)

        return result

    def change_class(self, new_class):
        new = new_class(self.key,
                        self.message,
                        self.error_class,
                        exception_error=self.exception_error)

        new.childrens = self.childrens
        return new


class APIError(Error):
    pass


class InvalidURL(APIError):
    pass


class RESTError(Error):
    status = 400

    def __init__(self, *args, **kwargs):
        status = kwargs.pop('status', None)
        if status is not None:
            self.status = int(status)

        Error.__init__(self, *args, **kwargs)


class JSONError(RESTError):
    """ This is the ``Exception`` used for JSON errors. This
    exception is raised when something goes wrong on export JSON classes
    and methods.
    See :class:`ines.exceptions.RESTError` for more information.
    """
