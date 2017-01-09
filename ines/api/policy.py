# -*- coding: utf-8 -*-

from json import loads
from os import linesep
from os.path import isfile
from os.path import normpath

from pyramid.decorator import reify

from ines import NOW_TIME
from ines.api import BaseSessionManager
from ines.api import BaseSession
from ines.authentication import AuthenticatedSession
from ines.convert import bytes_join
from ines.convert import compact_dump
from ines.convert import date_to_timestamp
from ines.convert import to_unicode
from ines.convert import make_sha256
from ines.convert import maybe_integer
from ines.convert import to_string
from ines.exceptions import HTTPTokenExpired
from ines.exceptions import HTTPUnauthorized
from ines.path import get_object_on_path
from ines.path import join_paths
from ines.utils import compare_digest
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
        authorization = self.settings.get('authorization_session')
        if authorization:
            self.authorization_session = get_object_on_path(authorization)
        else:
            self.authorization_session = AuthenticatedSession


class BaseTokenPolicySession(BaseSession):
    __api_name__ = 'policy'

    def get_authorization(self, session_type, authorization, **kwargs):
        if session_type == 'apikey':
            session_id = self.get_apikey_authorization(authorization)
            if session_id:
                return self.api_session_manager.authorization_session(
                    'apikey',
                    session_id,
                    **kwargs)

        elif session_type == 'token':
            session_id = self.get_token_authorization(authorization)
            if session_id:
                return self.api_session_manager.authorization_session(
                    'user',
                    session_id,
                    token=authorization,
                    **kwargs)

    def get_token_file_path(self, token_256):
        token_256 = to_string(token_256)
        return normpath(
            join_paths(
                self.settings['token.path'],
                token_256[0],
                token_256))

    def get_token_folder_path(self, token_256):
        token_256 = to_string(token_256)
        return normpath(
            join_paths(
                self.settings['token.path'],
                token_256[0]))

    def get_reference_file_path(self, session_key_256):
        session_key_256 = to_string(session_key_256)
        return normpath(
            join_paths(
                self.settings['token.session_reference_path'],
                session_key_256))

    def get_token_info(self, token_256):
        file_path = self.get_token_file_path(token_256)
        binary = get_file_binary(file_path, mode='r')
        if binary:
            return loads(binary)
        else:
            return {}

    def delete_session_key_tokens(self, session_key_256):
        file_path = self.get_reference_file_path(session_key_256)
        if isfile(file_path):
            temporary_file_path = self.get_reference_file_path(session_key_256 + '.tmp')
            move_file(file_path, temporary_file_path)

            binary = get_file_binary(temporary_file_path, mode='r')
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
        if token and '-' in token:
            token_256 = make_sha256(token)
            file_path = self.get_token_file_path(token_256)
            last_read_time = last_read_file_time(file_path)
            if last_read_time:
                info = self.get_token_info(token_256)
                if info:
                    now = NOW_TIME()
                    token_expire_seconds = info.get('token_expire_seconds') or self.token_expire_seconds
                    expire = last_read_time + token_expire_seconds
                    end_date = info.get('end_date')
                    if expire > now and (not end_date or end_date > now):
                        token_lock = make_token_lock(self.request, token, info['session_id'])
                        valid_token = compare_digest(info['lock_key'], token_lock)
                        valid_session_id = compare_digest(info['session_id'], token.split('-', 1)[0])
                        if valid_token and valid_session_id:
                            return info['session_id']

                    # Compromised or ended! Force revalidation
                    self.delete_session_key_tokens(info['session_key'])

    def create_new_session_key_token(self, session_id, session_key):
        return self.create_token(session_id, session_key)

    def create_token(self, session_id, session_key, end_date=None, token_expire_seconds=None):
        session_key_256 = make_sha256(session_key)

        # Delete all active tokens
        self.delete_session_key_tokens(session_key_256)

        token = '%s-%s' % (session_id, make_unique_hash(length=70))
        token_256 = make_sha256(token)

        data = {
            'lock_key': make_token_lock(self.request, token, session_id),
            'session_id': session_id,
            'session_key': session_key_256}

        if end_date:
            data['end_date'] = date_to_timestamp(end_date)
        if token_expire_seconds:
            data['token_expire_seconds'] = int(token_expire_seconds)

        info = compact_dump(data)

        # Save token
        file_path = self.get_token_file_path(token_256)
        put_binary_on_file(file_path, info)

        # Save reference
        reference_path = self.get_reference_file_path(session_key_256)
        put_binary_on_file(reference_path, token_256 + linesep, mode='ab')

        return token

    def expire_session_key_with_token(self, token):
        token_256 = make_sha256(token)
        token_info = self.get_token_info(token_256)
        if token_info:
            self.delete_session_key_tokens(token_info['session_key'])
            return token_info

    def create_authorization(self, session_id, token_expire_seconds=None):
        session_key = make_unique_hash(length=70)
        token = self.create_token(session_id, session_key, token_expire_seconds=token_expire_seconds)
        return session_key, token

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
        bytes_join(
            '-',
            [to_unicode(request.user_agent or ''),
             to_unicode(request.ip_address),
             to_unicode(token),
             to_unicode(session_id)]))
