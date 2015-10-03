# -*- coding: utf-8 -*-

from pyramid.decorator import reify
from pyramid.interfaces import IAuthenticationPolicy
from webob.cookies import CookieProfile
from zope.interface import implementer

from ines.authorization import APIKey
from ines.authorization import Authenticated
from ines.authorization import Everyone
from ines.authorization import NotAuthenticated
from ines.authorization import User
from ines.convert import to_bytes
from ines.convert import to_unicode


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
            header_key=None,
            cookie_key=None):

        self.application_name = application_name
        self.header_key = header_key

        self.cookie_key = cookie_key
        if self.cookie_key:
            self.cookie_profile = CookieProfile(
                cookie_name=self.cookie_key,
                path='/',
                serializer=SimpleSerializer())

        if not self.header_key and not self.cookie_key:
            raise ValueError('Please define a key for authentication validation. `header_key` or `cookie_key`')

    def get_authenticated_session(self, request):
        have_cache = hasattr(request, 'session_cache')
        if have_cache and 'authenticated' in request.session_cache:
            return request.session_cache['authenticated']

        authenticated = None
        authorization_string = self.unauthenticated_userid(request)
        if authorization_string:
            authorization_type = None
            lower_authorization_string = authorization_string.lower()
            if lower_authorization_string.startswith('token'):
                authorization_type, authorization = 'token', authorization_string[5:].strip()
            elif lower_authorization_string.startswith('apikey'):
                authorization_type, authorization = 'apikey', authorization_string[6:].strip()

            if authorization_type:
                authenticated = (
                    getattr(request.applications, self.application_name)
                    .get_authorization(authorization_type, authorization))

                if authenticated and not isinstance(authenticated, AuthenticatedSession):
                    session_type = authorization_type == 'token' and 'user' or authorization_type
                    authenticated = AuthenticatedSession(session_type, authenticated)

        if have_cache:
            request.session_cache['authenticated'] = authenticated
        return authenticated

    def authenticated_userid(self, request):
        return self.get_authenticated_session(request)

    def unauthenticated_userid(self, request):
        if self.header_key:
            userid = request.headers.get(self.header_key)
            if userid:
                return userid

        if self.cookie_key:
            userid = request.cookies.get(self.cookie_key)
            if userid:
                return userid

    def effective_principals(self, request):
        authenticated = self.get_authenticated_session(request)
        if not authenticated:
            return [Everyone, NotAuthenticated]
        else:
            return authenticated.get_principals()

    def remember(self, request, token):
        if self.cookie_key:
            return self.cookie_profile.get_headers('Token %s' % token)
        else:
            return []

    def forget(self, request):
        if self.cookie_key:
            return self.cookie_profile.get_headers(None)
        else:
            return []


class SimpleSerializer(object):
    def loads(self, bstruct):
        return to_unicode(bstruct)

    def dumps(self, appstruct):
        return to_bytes(appstruct)
