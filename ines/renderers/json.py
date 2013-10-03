# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
# 
# @author Hugo Branquinho <hugobranq@gmail.com>
# 
# $Id$

from simplejson import dumps

from ines.convert import force_unicode
from ines.renderers.binary import render_binary_response


def check_json_content_type(request):
    if request.has_value('is_iframe'):
        return 'text/html'
    else:
        return 'application/json'


def json_renderer(request, values, status=None, no_cache=False):
    # If in development mode, can return jsonp response
    if not request.settings['production_environment']:
        return jsonp_renderer(request, values, status, no_cache)
    else:
        binary = dumps(values)
        return render_binary_response(
                   binary,
                   status=status,
                   no_cache=no_cache,
                   content_type=check_json_content_type(request),
                   default_headers=request.response.headers)


def jsonp_renderer(request, values, status=None, no_cache=False):
    binary = dumps(values)

    callback = request.GET.get('callback')
    if callback:
        binary = force_unicode(binary)
        callback = force_unicode(callback)
        binary = u'%s(%s)' % (callback, binary)

    return render_binary_response(
               binary,
               status=status,
               no_cache=no_cache,
               content_type=check_json_content_type(request),
               default_headers=request.response.headers)
