# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
# 
# @author Hugo Branquinho <hugobranq@gmail.com>
# 
# $Id$


def check_if_route_exists(registry, route_name):
    for action in registry.action_state.actions:
        if action.get('discriminator') == ('route', route_name):
            return True
    else:
        return False
