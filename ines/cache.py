# -*- coding: utf-8 -*-

import errno
from os import mkdir
from os import remove as remove_file
from os import rename as rename_file
from os.path import join as join_paths
from os.path import isdir
from os.path import isfile
from pickle import dumps as pickle_dumps
from pickle import loads as pickle_loads
from time import time
from uuid import uuid4

from ines import MARKER
from ines.convert import force_string
from ines.convert import make_sha256
from ines.convert import maybe_integer
from ines.convert import maybe_set
from ines.locks import LockMe
from ines.utils import file_modified_time


def make_dir(path):
    try:
        mkdir(path, 0777)
    except OSError as error:
        if error.errno is not errno.EEXIST:
            raise


class SaveMe(object):
    def __init__(
            self,
            path,
            expire=None,
            retry_errnos=None,
            **lock_settings):

        self.expire = maybe_integer(expire)
        self.path = path
        if not isdir(self.path):
            mkdir(self.path, 0777)

        # Lock settings
        settings = {}
        for key, value in lock_settings.items():
            if key.startswith('lock_'):
                settings[key.split('lock_', 1)[1]] = value

        self.lockme = LockMe(
            lock_path=join_paths(self.path, 'locks'),
            **settings)

        self.retry_errnos = maybe_set(retry_errnos)
        self.retry_errnos.add(116)  # Stale NFS file handle
        self.retry_errnos.add(errno.ESTALE)

    def lock(self, *args, **kwargs):
        return self.lockme.lock(*args, **kwargs)

    def unlock(self, *args, **kwargs):
        return self.lockme.unlock(*args, **kwargs)

    def get_modified_time(self, name):
        name_256 = make_sha256(name)
        file_path = join_paths(self.path, name_256[0], name_256)
        modified_time = file_modified_time(file_path)
        if not modified_time:
            raise KeyError('Missing cache key "%s"' % name)
        else:
            return modified_time

    def __contains__(self, name):
        name_256 = make_sha256(name)
        file_path = join_paths(self.path, name_256[0], name_256)

        if self.expire:
            modified_time = file_modified_time(file_path)
            if not modified_time:
                return False

            expire_time = modified_time + self.expire
            if expire_time < time():
                self.remove(name)
                return False

        return isfile(file_path)

    def get_binary(self, name, expire=None):
        name_256 = make_sha256(name)
        file_path = join_paths(self.path, name_256[0], name_256)

        expire = expire or self.expire
        if expire:
            modified_time = file_modified_time(file_path)
            if not modified_time:
                raise KeyError('Missing cache key "%s"' % name)

            expire_time = modified_time + expire
            if expire_time < time():
                self.remove(name)
                raise KeyError('Missing cache key "%s"' % name)

        try:
            open_file = open(file_path, 'rb')
        except IOError:
            raise KeyError('Missing cache key "%s"' % name)
        else:
            return open_file.read()

    def put_binary(self, name, binary, mode='wb', ignore_error=True):
        name_256 = make_sha256(name)
        file_path = join_paths(self.path, name_256[0], name_256)

        try:
            with open(file_path, 'wb') as f:
                f.write(binary)
            self.put_reference(name)
        except IOError as error:
            if not ignore_error:
                raise

            elif error.errno is errno.ENOENT:
                # Missing folder, create and try again
                folder_path = join_paths(self.path, name_256[0])
                make_dir(folder_path)

            elif error.errno not in self.retry_errnos:
                raise

            return self.put_binary(name, binary, mode, ignore_error=False)

    def __delitem__(self, name):
        name_256 = make_sha256(name)
        file_path = join_paths(self.path, name_256[0], name_256)

        try:
            remove_file(file_path)
        except OSError:
            pass

        self.remove_reference(name)

    def put_reference(self, name):
        pass

    def remove_reference(self, name):
        pass

    def get_values(self, name):
        try:
            binary = self.get_binary(name)
        except KeyError:
            binary = None

        if not binary:
            return []
        else:
            result = binary.split('\n')
            if not result[-1]:
                result.pop(-1)
            return result

    def extend_values(self, name, values):
        if not values:
            raise ValueError('Denife some values')

        binary = '\n'.join(force_string(v) for v in values)
        binary += '\n'
        self.put_binary(name, binary, mode='ab')

    def append_value(self, name, value):
        self.extend_values(name, [value])

    def replace_values(self, name, values):
        if not values:
            self.remove(name)
        else:
            binary = '\n'.join(force_string(v) for v in values)
            binary += '\n'
            self.put_binary(name, binary)

    def __getitem__(self, name):
        binary = self.get_binary(name)
        return pickle_loads(binary)

    def __setitem__(self, name, info):
        info = pickle_dumps(info)
        self.put_binary(name, info)

    def get(self, name, default=None, expire=None):
        try:
            binary = self.get_binary(name, expire=expire)
        except KeyError:
            return default
        else:
            return pickle_loads(binary)

    def put(self, name, info):
        self[name] = info

    def remove(self, name):
        del self[name]


class SaveMeWithReference(SaveMe):
    def __init__(self, *args, **kwargs):
        super(SaveMeWithReference, self).__init__(*args, **kwargs)

        self.reference_path = join_paths(self.path, 'references')
        if not isdir(self.reference_path):
            mkdir(self.reference_path, 0777)

    def get_references(self, name):
        first_name = name.split(' ', 1)[0]
        first_name_256 = make_sha256(first_name)
        file_path = join_paths(
            self.reference_path,
            first_name_256[0],
            first_name_256)

        references = set()
        try:
            with open(file_path, 'rb') as f:
                for saved_name in f.readlines():
                    saved_name = saved_name.replace('\n', '')
                    if saved_name.startswith(name):
                        references.add(saved_name)
        except:
            pass
        return references

    def put_reference(self, name, ignore_error=True):
        first_name = name.split(' ', 1)[0]
        first_name_256 = make_sha256(first_name)
        file_path = join_paths(
            self.reference_path,
            first_name_256[0],
            first_name_256)

        try:
            with open(file_path, 'ab') as f:
                f.write(name + '\n')
        except IOError as error:
            if not ignore_error:
                raise

            elif error.errno is errno.ENOENT:
                # Missing folder, create and try again
                folder_path = join_paths(self.reference_path, first_name_256[0])
                make_dir(folder_path)

            elif error.errno not in self.retry_errnos:
                raise

            else:
                return self.put_reference(name, ignore_error=False)

    def remove_reference(self, name):
        first_name = name.split(' ', 1)[0]
        first_name_256 = make_sha256(first_name)
        file_path = join_paths(
            self.reference_path,
            first_name_256[0],
            first_name_256)

        temporary_file_path = file_path + '.' + uuid4().hex
        try:
            rename_file(file_path, temporary_file_path)
        except OSError:
            # No file found
            return

        references = set()
        with open(temporary_file_path, 'rb') as f:
            for saved_name in f.readlines():
                saved_name = saved_name.replace('\n', '')
                if saved_name:
                    references.add(saved_name)
        if name in references:
            references.remove(name)

        if references:
            with open(file_path, 'ab') as f:
                f.write('\n'.join(references) + '\n')

        try:
            remove_file(temporary_file_path)
        except OSError:
            pass

    def get_children(self, name):
        result = {}
        for reference in self.get_references(name):
            value = self.get(reference, MARKER)
            if value is not MARKER:
                result[reference] = value
        return result

    def remove_children(self, name):
        for reference in self.get_references(name):
            self.remove(reference)

    def __contains__(self, name):
        first_name = name.split(' ', 1)[0]
        first_name_256 = make_sha256(first_name)
        file_path = join_paths(
            self.reference_path,
            first_name_256[0],
            first_name_256)

        try:
            with open(file_path, 'rb') as f:
                return name + '\n' in f.readlines()
        except IOError:
            pass

        return False


class api_cache_decorator(object):
    def __init__(self, expire_seconds=900):
        self.expire_seconds = int(expire_seconds)

    def __call__(self, wrapped):
        def replacer(cls, *args, **kwargs):
            key = ' '.join([cls.application_name, cls.__api_name__, wrapped.__name__])
            if kwargs.pop('expire', False):
                cls.config.cache.remove(key)

            if not kwargs.pop('no_cache', False):
                cached = cls.config.cache.get(key, default=MARKER, expire=self.expire_seconds)
                if cached is not MARKER:
                    return cached

            cached = wrapped(cls, *args, **kwargs)
            cls.config.cache.put(key, cached)
            return cached

        return replacer
