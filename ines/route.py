# -*- coding: utf-8 -*-

from ines.convert import maybe_list


class RootFactory(dict):
    def __init__(self, request):
        super(dict, self).__init__()
        self.update(request.matchdict or {})
        self.request = request


def lookup_for_route_params(route):
    params = {}
    while True:
        try:
            route.generate(params)
        except KeyError as error:
            key = error.args[0]
            params[key] = ''
            continue
        break
    return params.keys()


def lookup_for_route_permissions(registry, introspector_route):
    permissions = {}
    for maybe_view in registry.introspector.related(introspector_route):
        if maybe_view.type_name == 'view':
            for request_method in maybe_list(maybe_view['request_methods']):
                for maybe_permission in registry.introspector.related(maybe_view):
                    if maybe_permission.type_name == 'permission':
                        permissions[request_method] = maybe_permission['value']
                        break
    return permissions
