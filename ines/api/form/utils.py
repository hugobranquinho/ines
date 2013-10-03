# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
# 
# @author Hugo Branquinho <hugobranq@gmail.com>
# 
# $Id$

from colander import null


class FormResponse(object):
    def __init__(self,
                 html,
                 captured,
                 have_errors,
                 response,
                 return_none_values=True):

        self.html = html
        self.captured = captured
        self.have_errors = bool(have_errors)
        self.response = response
        self.return_none_values = bool(return_none_values)

    @property
    def form_passed(self):
        return bool(self.captured)

    def get_form_values(self):
        return clean_captured_dict(self.captured, self.return_none_values)


def button_in_keys(buttons, keys):
    for button in buttons:
        if button.name in keys:
            return True
    else:
        return False


def clean_appstruct(appstruct):
    if appstruct is not null and \
       appstruct and \
       isinstance(appstruct, dict):

        items = appstruct.items()
        appstruct = dict((key, value) for key, value in items
                         if value is not None)
        if appstruct:
            return appstruct

    return null


def clean_captured_dict(captured, return_none_values=True):
    if not isinstance(captured, dict):
        if not isinstance(captured, (list, tuple)):
            if captured is null and return_none_values:
                return None
            else:
                return captured
        else:
            return [clean_captured_dict(value, return_none_values)
                    for value in captured]

    else:
        new = {}
        for key, value in captured.items():
            if value is not null and (value or value == 0 or value is False):
                new[key] = clean_captured_dict(value, return_none_values)
            elif return_none_values:
                new[key] = None

        return new
