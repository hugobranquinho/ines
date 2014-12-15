# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>


class RootFactory(dict):
    def __init__(self, request):
        super(dict, self).__init__()
        self.update(request.matchdict or {})
        self.request = request
