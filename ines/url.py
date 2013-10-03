# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
# 
# @author Hugo Branquinho <hugobranq@gmail.com>
# 
# $Id$


def compare_urls(first, second):
    if first.endswith('/'):
        return compare_urls(first[:-1], second)
    elif second.endswith('/'):
        return compare_urls(first, second[:-1])
    else:
        return first == second
