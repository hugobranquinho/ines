# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
# 
# @author Hugo Branquinho <hugobranq@gmail.com>
# 
# $Id$


def pop_renderer_argument(renderer, key, arguments):
    """ Pop argument from configurations (``argument``) or return default
    attribute from renderer class (``renderer``).


    Arguments

        ``renderer``

            Renderer class.

        ``key``

            Renderer class (``renderer``) and configuration (``argument``) key
            to get.

        ``arguments``

            All configurations sent.
    """
    return arguments.pop(key, None) or getattr(renderer, key)
