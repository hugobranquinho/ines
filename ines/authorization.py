# -*- coding: utf-8 -*-

from pyramid.compat import is_nonstr_iter
from pyramid.interfaces import IAuthorizationPolicy
from pyramid.security import ACLAllowed
from pyramid.security import ACLDenied
from pyramid.security import ALL_PERMISSIONS
from pyramid.security import Authenticated
from pyramid.security import Everyone
from zope.interface import implementer


class deny_authorization(str):
    __deny__ = True

    def __repr__(self):
        return 'not %s' % self


INES_POLICY = 'ines.policy'
APIKey = 'apikey'
User = 'user'
NotAuthenticated = 'system.NotAuthenticated'
NotUser = deny_authorization(User)
NotAPIKey = deny_authorization(APIKey)


@implementer(IAuthorizationPolicy)
class TokenAuthorizationPolicy(object):
    def __init__(self, application_name):
        self.application_name = application_name

    def permits(self, context, principals, permission):
        is_allowed = False
        if permission is not INES_POLICY:
            if not is_nonstr_iter(permission):
                permission = [permission]
            elif ALL_PERMISSIONS is permission:
                permission = [permission]

            for permission_value in permission:
                in_principals = (
                    permission_value == ALL_PERMISSIONS
                    or permission_value in principals)

                if getattr(permission_value, '__deny__', False):
                    if in_principals:
                        is_allowed = False
                        break
                    else:
                        is_allowed = True
                elif in_principals:
                    is_allowed = True

        else:
            request = getattr(context, 'request', None)
            if request:
                application = getattr(request.applications, self.application_name)
                is_allowed = bool(
                    application.permits(
                        context.request.application_name,
                        context.request.matched_route.name,
                        context.request.method,
                        principals))

        if is_allowed:
            return ACLAllowed(
                '<allowed>',
                '<ACL found>',
                permission,
                principals,
                context)
        else:
            return ACLDenied(
                '<deny>',
                '<No permission>',
                permission,
                principals,
                context)
