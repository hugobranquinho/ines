# -*- coding: utf-8 -*-

import errno
from os import mkdir
from os import remove as remove_file
from os import rename as rename_file
from os.path import join as join_paths
from os.path import isfile
from os.path import dirname
from pickle import dumps as pickle_dumps
from pickle import loads as pickle_loads
from time import time

from ines import MARKER
from ines.convert import force_string
from ines.convert import make_sha256
from ines.convert import maybe_integer
from ines.convert import maybe_set
from ines.locks import LockMe
from ines.utils import make_uuid_hash
from ines.utils import file_modified_time


def make_dir(path, mode=0777):
    path = force_string(path)
    try:
        mkdir(path, mode)
    except OSError as error:
        if error.errno is not errno.EEXIST:
            raise
    return path


class SaveMe(object):
    def __init__(
            self,
            path,
            expire=None,
            retry_errnos=None,
            retries=2,
            **lock_settings):

        self.expire = maybe_integer(expire)
        self.path = make_dir(path)

        # Lock settings
        settings = {}
        for key, value in lock_settings.items():
            if key.startswith('lock_'):
                settings[key.split('lock_', 1)[1]] = value

        self.lockme = LockMe(
            lock_path=join_paths(self.path, 'locks'),
            **settings)

        self.retries = maybe_integer(retries) or 1
        self.retry_errnos = maybe_set(retry_errnos)
        self.retry_errnos.add(116)  # Stale NFS file handle
        self.retry_errnos.add(errno.ESTALE)

    def lock(self, *args, **kwargs):
        return self.lockme.lock(*args, **kwargs)

    def unlock(self, *args, **kwargs):
        return self.lockme.unlock(*args, **kwargs)

    def get_file_path(self, name):
        name_256 = make_sha256(name)
        return join_paths(self.path, name_256[0], name_256)

    def make_folder(self, name):
        name_256 = make_sha256(name)
        folder_path = join_paths(self.path, name_256[0])
        make_dir(folder_path)

    def _contains(self, path, expire=MARKER):
        if expire is MARKER:
            expire = self.expire
        if expire:
            modified_time = file_modified_time(path)
            if not modified_time:
                return False

            if (modified_time + self.expire) < time():
                self._delete_path(path)
                return False

        return isfile(path)

    def __contains__(self, name):
        file_path = self.get_file_path(name)
        return self._contains(file_path)

    def _get_binary(self, path, expire=MARKER):
        if expire is MARKER:
            expire = self.expire
        if expire:
            modified_time = file_modified_time(path)
            if not modified_time:
                return False
            elif (modified_time + expire) < time():
                self._delete_path(path)
                return False

        retries = (maybe_integer(self.retries) or 1) + 1
        while retries:
            try:
                with open(path, 'rb') as f:
                    binary = f.read()
            except IOError as error:
                if error.errno is errno.ENOENT:
                    return False
                elif error.errno in self.retry_errnos:
                    # Try again, or not!
                    retries -= 1
                else:
                    # Your path ends here!
                    break
            else:
                return binary

        # After X retries, raise previous IOError
        raise

    def get_binary(self, name, expire=MARKER):
        file_path = self.get_file_path(name)
        binary = self._get_binary(file_path, expire)
        if binary is False:
            raise KeyError('Missing cache key "%s"' % name)
        else:
            return binary

    def _put_binary(self, path, binary, mode='wb'):
        retries = (maybe_integer(self.retries) or 1) + 1
        while retries:
            try:
                with open(path, mode) as f:
                    f.write(binary)
            except IOError as error:
                if error.errno is errno.ENOENT:
                    # Missing folder, create and try again
                    make_dir(dirname(path))
                elif error.errno in self.retry_errnos:
                    # Try again, or not!
                    retries -= 1
                else:
                    # Your path ends here!
                    break
            else:
                return binary

        # After X retries, raise previous IOError
        raise

    def put_binary(self, name, binary, mode='wb'):
        file_path = self.get_file_path(name)
        self._put_binary(file_path, binary, mode)
        self.put_reference(name)

    def _delete_path(self, path):
        retries = (maybe_integer(self.retries) or 1) + 1
        while retries:
            try:
                remove_file(path)
            except OSError as error:
                if error.errno is errno.ENOENT:
                    # Missing folder, file deleted!
                    break
                elif error.errno in self.retry_errnos:
                    # Try again, or not!
                    retries -= 1
                else:
                    # Your path ends here!
                    break
            else:
                break

    def __delitem__(self, name):
        file_path = self.get_file_path(name)
        self._delete_path(file_path)
        self.remove_reference(name)

    def put_reference(self, name):
        pass

    def remove_reference(self, name):
        pass

    def get_values(self, name, expire=MARKER):
        try:
            binary = self.get_binary(name, expire)
        except KeyError:
            return []
        else:
            result = binary.split('\n')
            if not result[-1]:
                result.pop(-1)
            return result

    def extend_values(self, name, values):
        if not values:
            raise ValueError('Define some values')

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
        value = self.get(name, default=MARKER)
        if value is MARKER:
            raise KeyError('Missing cache key "%s"' % name)
        else:
            return value

    def __setitem__(self, name, info):
        info = pickle_dumps(info)
        self.put_binary(name, info)

    def get(self, name, default=None, expire=MARKER):
        try:
            binary = self.get_binary(name, expire=expire)
        except KeyError:
            return default
        else:
            try:
                value = pickle_loads(binary)
            except EOFError:
                # Something goes wrong! Delete file to prevent more errors
                self.remove(name)
                raise
            else:
                return value

    def put(self, name, info):
        self[name] = info

    def remove(self, name):
        del self[name]


class SaveMeWithReference(SaveMe):
    def __init__(self, *args, **kwargs):
        super(SaveMeWithReference, self).__init__(*args, **kwargs)
        self.reference_path = make_dir(join_paths(self.path, 'references'))

    def get_reference_path(self, name):
        first_name = name.split(' ', 1)[0]
        first_name_256 = make_sha256(first_name)
        return join_paths(self.reference_path, first_name_256[0], first_name_256)

    def _get_references(self, path, name):
        references = set()
        binary = self._get_binary(path, expire=None)
        if binary is not False:
            for saved_name in binary.splitlines():
                if saved_name and saved_name.startswith(name):
                    references.add(saved_name)
        return references

    def get_references(self, name):
        file_path = self.get_reference_path(name)
        return self._get_references(file_path, name)

    def put_reference(self, name):
        if name not in self.get_references(name):
            file_path = self.get_reference_path(name)
            reference_line = name + '\n'
            self._put_binary(file_path, reference_line, mode='ab')

    def remove_reference(self, name, expire=MARKER):
        file_path = self.get_reference_path(name)
        temporary_file_path = file_path + '.' + make_uuid_hash()
        retries = (maybe_integer(self.retries) or 1) + 1
        while retries:
            try:
                rename_file(file_path, temporary_file_path)
            except OSError as error:
                if error.errno in self.retry_errnos:
                    # Try again, or not!
                    retries -= 1
                else:
                    # Something goes wrong
                    raise
            else:
                break

        references = self._get_references(temporary_file_path, name='')
        if name in references:
            references.remove(name)

        # Validate if references still exists
        if references:
            if expire is MARKER:
                # Dont use expire, we only need to know if file exists
                expire = None
            for name in references:
                path = self.get_file_path(name)
                if not self._contains(path, expire=expire):
                    references.remove(name)

            if references:
                reference_lines = '\n'.join(references) + '\n'
                self._put_binary(file_path, reference_lines, mode='ab')

        self._delete_path(temporary_file_path)

    def get_children(self, name, expire=MARKER):
        result = {}
        missing_reference = False
        for reference in self.get_references(name):
            value = self.get(reference, MARKER, expire=expire)
            if value is not MARKER:
                result[reference] = value
            else:
                missing_reference = True

        if missing_reference:
            self.remove_reference(name, expire)

        return result

    def remove_children(self, name):
        for reference in self.get_references(name):
            self.remove(reference)

    def __contains__(self, name):
        file_path = self.get_reference_path(name)
        binary = self._get_binary(file_path, expire=None)
        if binary is not False:
            return name in binary.splitlines()
        else:
            return False


class api_cache_decorator(object):
    def __init__(self, expire_seconds=900):
        self.expire_seconds = int(expire_seconds)

    def __call__(self, wrapped):
        def replacer(cls, *args, **kwargs):
            key = ' '.join([cls.application_name, cls.__api_name__, 'decorator', wrapped.__name__])
            if kwargs.pop('expire_cache', False):
                cls.config.cache.remove(key)
                return True

            if not kwargs.pop('no_cache', False):
                cached = cls.config.cache.get(key, default=MARKER, expire=self.expire_seconds)
                if cached is not MARKER:
                    return cached

            cached = wrapped(cls, *args, **kwargs)
            cls.config.cache.put(key, cached)
            return cached

        return replacer
