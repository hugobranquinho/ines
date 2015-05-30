# -*- coding: utf-8 -*-

from pyramid.decorator import reify
from pyramid.interfaces import IAuthenticationPolicy
from zope.interface import implementer

from ines.authorization import APIKey
from ines.authorization import Authenticated
from ines.authorization import Everyone
from ines.authorization import NotAuthenticated
from ines.authorization import User


class AuthenticatedSession(object):
    def __init__(self, session_type, session_id, **kwargs):
        self.session_type = session_type
        self.session_id = session_id

        self.principals = [
            Everyone,
            Authenticated,
            self.session_type,
            '%s.%s' % (self.session_type, self.session_id)]

        if kwargs:
            self.__dict__.update(kwargs)

    def __repr__(self):
        return '%s.%s' % (self.session_type, self.session_id)

    def get_principals(self):
        return self.principals

    @reify
    def is_authenticated(self):
        return bool(self.session_id)

    @reify
    def is_user(self):
        return self.session_type == User

    @reify
    def is_apikey(self):
        return self.session_type == APIKey


@implementer(IAuthenticationPolicy)
class ApplicationHeaderAuthenticationPolicy(object):
    def __init__(
            self,
            application_name,
            header_key='Authorization'):
        self.application_name = application_name
        self.header_key = header_key

    def get_authenticated_session(self, request):
        have_cache = hasattr(request, 'session_cache')
        if have_cache and 'authenticated' in request.session_cache:
            return request.session_cache['authenticated']

        authorization_type = None
        authorization = self.unauthenticated_userid(request)
        if authorization:
            authorization_info = authorization.split(None, 1)
            if len(authorization_info) == 2:
                authorization_type, authorization = authorization_info
            if authorization_type:
                authorization_type = authorization_type.lower()

        authenticated = None
        if authorization_type in ('apikey', 'token'):
            application = getattr(request.applications, self.application_name)
            authenticated = application.get_authorization(authorization_type, authorization)
            if authenticated and not isinstance(authenticated, AuthenticatedSession):
                session_type = authorization_type
                if authorization_type == 'token':
                    session_type = 'user'
                authenticated = AuthenticatedSession(session_type, authenticated)

        if have_cache:
            request.session_cache['authenticated'] = authenticated

        return authenticated

    def authenticated_userid(self, request):
        return self.get_authenticated_session(request)

    def unauthenticated_userid(self, request):
        return request.headers.get(self.header_key)

    def effective_principals(self, request):
        authenticated = self.get_authenticated_session(request)
        if not authenticated:
            return [Everyone, NotAuthenticated]
        else:
            return authenticated.get_principals()

    def remember(self, request, userid, **kw):
        return []

    def forget(self, request):
        return []
