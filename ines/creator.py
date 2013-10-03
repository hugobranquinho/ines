# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
# 
# @author Hugo Branquinho <hugobranq@gmail.com>
# 
# $Id$

from hashlib import sha1
from os import urandom

from ines.convert import force_unicode


def create_random_hash(length=40):
    if length < 1:
        message = u'Length must be bigger then 0.'
        raise ValueError(message)

    result = ''
    salt = sha1()
    for i in xrange((length / 40) + 1):
        salt.update(urandom(60))
        result += salt.hexdigest()

    return force_unicode(result[:length])
