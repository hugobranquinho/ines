# -*- coding: utf-8 -*-

from json import dumps
from json import loads
from os.path import isfile
from os.path import join as join_paths
from os.path import normpath
from time import time as NOW_TIME

from pyramid.decorator import reify

from ines.api import BaseSessionManager
from ines.api import BaseSession
from ines.authentication import AuthenticatedSession
from ines.convert import date_to_timestamp
from ines.convert import force_unicode
from ines.convert import make_sha256
from ines.convert import maybe_integer
from ines.exceptions import HTTPTokenExpired
from ines.exceptions import HTTPUnauthorized
from ines.path import get_object_on_path
from ines.utils import make_unique_hash
from ines.utils import get_file_binary
from ines.utils import last_read_file_time
from ines.utils import move_file
from ines.utils import put_binary_on_file
from ines.utils import remove_file_quietly


class BasePolicySessionManager(BaseSessionManager):
    __api_name__ = 'policy'

    def __init__(self, *args, **kwargs):
        super(BasePolicySessionManager, self).__init__(*args, **kwargs)

        if 'token.session_reference_path' not in self.settings:
            self.settings['token.session_reference_path'] = join_paths(
                self.settings['token.path'],
                'reference')

        # Jobs settings
        authorization = self.config.settings.get('policy.authorization')
        if authorization:
            self.authorization_session = get_object_on_path(authorization)
        else:
            self.authorization_session = AuthenticatedSession


class BaseTokenPolicySession(BaseSession):
    __api_name__ = 'policy'

    def get_authorization(self, session_type, authorization):
        if session_type == 'apikey':
            session_id = self.get_apikey_authorization(authorization)
            if session_id:
                return self.api_session_manager.authorization_session(
                    u'apikey',
                    session_id)

        elif session_type == 'token':
            session_id = self.get_token_authorization(authorization)
            if session_id:
                return self.api_session_manager.authorization_session(
                    u'user',
                    session_id,
                    token=authorization)

    def get_token_file_path(self, token_256):
        return normpath(
            join_paths(
                self.settings['token.path'],
                token_256[0],
                token_256))

    def get_token_folder_path(self, token_256):
        return normpath(
            join_paths(
                self.settings['token.path'],
                token_256[0]))

    def get_reference_file_path(self, session_key_256):
        return normpath(
            join_paths(
                self.settings['token.session_reference_path'],
                session_key_256))

    def get_token_info(self, token_256):
        file_path = self.get_token_file_path(token_256)
        binary = get_file_binary(file_path)
        if binary:
            return loads(binary)
        else:
            return {}

    def delete_session_key_tokens(self, session_key_256):
        file_path = self.get_reference_file_path(session_key_256)
        if isfile(file_path):
            temporary_file_path = self.get_reference_file_path(session_key_256 + '.tmp')
            move_file(file_path, temporary_file_path)

            binary = get_file_binary(temporary_file_path)
            if binary:
                tokens_to_delete = binary.splitlines()
                if tokens_to_delete:
                    for token_256 in set(tokens_to_delete):
                        if token_256:
                            token_file_path = self.get_token_file_path(token_256)
                            remove_file_quietly(token_file_path)

            # Remove references file
            remove_file_quietly(temporary_file_path)

        return True

    @reify
    def token_expire_seconds(self):
        if not self.request.is_production_environ and 'tokenLifetime' in self.request.GET:
            maybe_seconds = maybe_integer(self.request.GET['tokenLifetime'])
            if maybe_seconds and maybe_seconds >= 1:
                return maybe_seconds
        return int(self.settings['token.expire_seconds'])

    def get_apikey_authorization(self, apikey):
        pass

    def get_token_authorization(self, token):
        token_256 = make_sha256(token)
        file_path = self.get_token_file_path(token_256)
        last_read_time = last_read_file_time(file_path)
        if not last_read_time:
            raise HTTPUnauthorized()

        info = self.get_token_info(token_256)
        if not info:
            raise HTTPUnauthorized()

        now = NOW_TIME()
        expire = last_read_time + self.token_expire_seconds
        end_date = info.get('end_date')
        if expire < now or (end_date and end_date < now):
            error = HTTPTokenExpired()
        else:
            lock_key = make_token_lock(self.request, token, info['session_id'])
            if info['lock_key'] == lock_key:
                return info['session_id']
            error = HTTPUnauthorized()

        # Compromised or ended! Force revalidation
        self.delete_session_key_tokens(info['session_key'])
        raise error

    def create_new_session_key_token(self, session_id, session_key_256):
        return self.create_token(session_id, session_key_256)

    def create_token(self, session_id, session_key_256, end_date=None):
        # Delete all active tokens
        self.delete_session_key_tokens(session_key_256)

        token = make_unique_hash(length=70)
        token_256 = make_sha256(token)

        if end_date:
            end_date = date_to_timestamp(end_date)

        info = dumps({
            'lock_key': make_token_lock(self.request, token, session_id),
            'session_id': session_id,
            'session_key': session_key_256,
            'end_date': end_date})

        # Save token
        file_path = self.get_token_file_path(token_256)
        put_binary_on_file(file_path, info)

        # Save reference
        reference_path = self.get_reference_file_path(session_key_256)
        put_binary_on_file(reference_path, token_256 + '\n', mode='ab')

        return token

    def expire_session_key_with_token(self, token):
        token_256 = make_sha256(token)
        token_info = self.get_token_info(token_256)
        if token_info:
            self.delete_session_key_tokens(token_info['session_key'])
            return token_info

    def create_authorization(self, session_id):
        session_key = make_unique_hash(length=70)
        session_key_256 = make_sha256(session_key)
        return session_key, self.policy.create_token(session_id, session_key_256)

    def session_is_alive(self, session_key_256):
        reference_path = self.get_reference_file_path(session_key_256)
        binary = get_file_binary(reference_path)
        if binary:
            tokens = binary.splitlines()
            if tokens:
                now = NOW_TIME()
                for token_256 in set(tokens):
                    if token_256:
                        token_path = self.get_token_file_path(token_256)
                        last_read_time = last_read_file_time(token_path)
                        if last_read_time:
                            if now <= (last_read_time + self.token_expire_seconds):
                                return True
                            else:
                                # Remove token garbage
                                remove_file_quietly(token_path)
            else:
                # Remove session reference
                remove_file_quietly(reference_path)

        return False


def make_token_lock(request, token, session_id):
    return make_sha256(
        u'-'.join([
            force_unicode(request.user_agent or ''),
            force_unicode(request.ip_address),
            force_unicode(token),
            force_unicode(session_id)]))
