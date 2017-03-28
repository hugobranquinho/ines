# -*- coding: utf-8 -*-

from functools import lru_cache, wraps
from os.path import isfile, join as join_paths
from pickle import dumps as pickle_dumps, loads as pickle_loads

from ines import DEFAULT_RETRY_ERRNO, lazy_import_module, MARKER, NEW_LINE_AS_BYTES, NOW_TIME
from ines.cleaner import clean_string
from ines.convert import make_sha256, maybe_integer, maybe_list, maybe_set, to_bytes
from ines.locks import LockMe, LockMeMemcached
from ines.utils import (
    file_modified_time, get_file_binary, make_dir, make_uuid_hash, move_file, put_binary_on_file, remove_file_quietly)


class _SaveMe(object):
    def get_binary(self, name, expire=MARKER):
        pass

    def put_binary(self, name, binary, mode='put', expire=MARKER):
        pass

    def __delitem__(self, name):
        pass

    def __contains__(self, name):
        pass

    def get_values(self, name, expire=MARKER):
        try:
            binary = self.get_binary(name, expire)
        except KeyError:
            return []
        else:
            return binary.splitlines()

    def extend_values(self, name, values, expire=MARKER):
        if not values:
            raise ValueError('Define some values')

        binary = NEW_LINE_AS_BYTES.join(map(to_bytes, values)) + NEW_LINE_AS_BYTES
        self.put_binary(name, binary, mode='append', expire=expire)

    def append_value(self, name, value, expire=MARKER):
        self.extend_values(name, [value], expire=expire)

    def replace_values(self, name, values, expire=MARKER):
        values = maybe_list(values)
        if not values:
            self.remove(name)
        else:
            self.put_binary(
                name,
                binary=NEW_LINE_AS_BYTES.join(values) + NEW_LINE_AS_BYTES,
                expire=expire)

    def __getitem__(self, name):
        value = self.get(name, default=MARKER)
        if value is MARKER:
            raise KeyError('Missing cache key "%s"' % name)
        else:
            return value

    def __setitem__(self, name, info):
        self.put(name, info)

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

    def put(self, name, info, expire=MARKER):
        info = pickle_dumps(info)
        self.put_binary(name, info, expire=expire)

    def remove(self, name):
        del self[name]


class SaveMe(_SaveMe):
    def __init__(
            self,
            path,
            expire=None,
            retry_errno=None,
            retries=3,
            **lock_settings):

        self.expire = maybe_integer(expire)
        self.path = make_dir(path)
        self.retries = maybe_integer(retries) or 3
        self.retry_errno = maybe_set(retry_errno)
        self.retry_errno.update(DEFAULT_RETRY_ERRNO)

        # Lock settings
        settings = {}
        for key, value in list(lock_settings.items()):
            if key.startswith('lock_'):
                settings[key.split('lock_', 1)[1]] = value

        lock_path = settings.pop('path', None) or join_paths(self.path, 'locks')
        self.lockme = LockMe(lock_path, **settings)

    def lock(self, *args, **kwargs):
        return self.lockme.lock(*args, **kwargs)

    def unlock(self, *args, **kwargs):
        return self.lockme.unlock(*args, **kwargs)

    @lru_cache(1000)
    def get_file_path(self, name):
        name_256 = make_sha256(name)
        return join_paths(self.path, name_256[0], name_256)

    def _contains(self, path, expire=MARKER):
        if expire is MARKER:
            expire = self.expire
        if expire:
            modified_time = file_modified_time(path)
            if not modified_time:
                return False

            if (modified_time + self.expire) < NOW_TIME():
                self._delete_path(path)
                return False

        return isfile(path)

    def __contains__(self, name):
        return self._contains(self.get_file_path(name))

    def _get_binary(self, path, expire=MARKER):
        if expire is MARKER:
            expire = self.expire
        if expire:
            modified_time = file_modified_time(path)
            if not modified_time:
                return None
            elif (modified_time + expire) < NOW_TIME():
                self._delete_path(path)
                return None

        return get_file_binary(path, mode='rb', retries=self.retries, retry_errno=self.retry_errno)

    def get_binary(self, name, expire=MARKER):
        binary = self._get_binary(self.get_file_path(name), expire)
        if binary is None:
            raise KeyError('Missing cache key "%s"' % name)
        else:
            return binary

    def put_binary(self, name, binary, mode='put', expire=MARKER):
        mode = 'ab' if mode == 'append' else 'wb'
        put_binary_on_file(self.get_file_path(name), binary, mode, retries=self.retries, retry_errno=self.retry_errno)

    def _delete_path(self, path):
        remove_file_quietly(path, retries=self.retries, retry_errno=self.retry_errno)

    def __delitem__(self, name):
        file_path = self.get_file_path(name)
        self._delete_path(file_path)


class SaveMeWithReference(SaveMe):
    def __init__(self, *args, **kwargs):
        super(SaveMeWithReference, self).__init__(*args, **kwargs)
        self.reference_path = make_dir(join_paths(self.path, 'references'))

    def put_binary(self, name, *args, **kwargs):
        super(SaveMeWithReference, self).put_binary(name, *args, **kwargs)
        self.put_reference(name)

    def __delitem__(self, name):
        super(SaveMeWithReference, self).__delitem__(name)
        self.remove_reference(name)

    def get_reference_path(self, name):
        first_name = name.split(' ', 1)[0]
        first_name_256 = make_sha256(first_name)
        return join_paths(self.reference_path, first_name_256[0], first_name_256)

    def _get_references(self, path, name):
        references = set()
        binary = self._get_binary(path, expire=None)
        if binary is not None:
            for saved_name in binary.splitlines():
                if saved_name and saved_name.startswith(name):
                    references.add(saved_name)
        return references

    def get_references(self, name):
        file_path = self.get_reference_path(name)
        return self._get_references(file_path, name)

    def put_reference(self, name):
        if name not in self.get_references(name):
            put_binary_on_file(
                self.get_reference_path(name),
                NEW_LINE_AS_BYTES.join([to_bytes(name), b'']),
                mode='ab',
                retries=self.retries,
                retry_errno=self.retry_errno)

    def remove_reference(self, name, expire=MARKER):
        file_path = self.get_reference_path(name)
        temporary_file_path = file_path + '.' + make_uuid_hash()
        move_file(file_path, temporary_file_path, retries=self.retries, retry_errno=self.retry_errno)

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
                references = maybe_list(references)
                references.append(NEW_LINE_AS_BYTES)

                put_binary_on_file(
                    file_path,
                    binary=NEW_LINE_AS_BYTES.join(map(to_bytes, references)),
                    mode='ab',
                    retries=self.retries,
                    retry_errno=self.retry_errno)

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
        if binary is not None:
            return name in binary.splitlines()
        else:
            return False


class SaveMeMemcached(_SaveMe):
    def __init__(
            self,
            url,
            expire=None,
            **settings):

        # Lock settings
        lock_settings = {}
        for key in list(settings.keys()):
            if key.startswith('lock_'):
                lock_settings[key.split('lock_', 1)[1]] = settings.pop(key)
        lock_settings.update(settings)

        self.memcache_module = lazy_import_module('memcache')
        self.memcache = self.memcache_module.Client(url.split(';'), **settings)
        self.expire = maybe_integer(expire)
        self.lockme = LockMeMemcached(url, **lock_settings)

    def lock(self, *args, **kwargs):
        return self.lockme.lock(*args, **kwargs)

    def unlock(self, *args, **kwargs):
        return self.lockme.unlock(*args, **kwargs)

    def format_name(self, name):
        return to_bytes(make_sha256(name))

    def __contains__(self, name):
        return self.memcache.get(self.format_name(name)) is not None

    def get_binary(self, name, expire=MARKER):
        binary = self.memcache.get(self.format_name(name))
        if binary is None:
            raise KeyError('Missing cache key "%s"' % name)
        else:
            return binary

    def put_binary(self, name, binary, mode='wb', expire=MARKER):
        name_256 = self.format_name(name)
        if expire is MARKER:
            expire = self.expire

        # Append to existing file
        if mode == 'append' and name in self:
            self.memcache.append(name_256, binary, time=expire or 0)
        else:
            self.memcache.set(name_256, binary, time=expire or 0)

    def __delitem__(self, name):
        self.memcache.delete(self.format_name(name))


class api_cache_decorator(object):
    def __init__(self, expire_seconds=900):
        self.cache_name = None
        self.wrapper = None
        self.expire_seconds = expire_seconds

        self.father = None
        self.children = []

    def __call__(self, wrapped):
        @wraps(wrapped)
        def wrapper(cls, expire_cache=False, no_cache=False):
            if expire_cache:
                return self.expire(cls)

            elif not no_cache:
                cached = cls.config.cache.get(self.cache_name, default=MARKER, expire=self.expire_seconds)
                if cached is not MARKER:
                    return cached

            cached = wrapped(cls)
            cls.config.cache.put(self.cache_name, cached, expire=self.expire_seconds)
            return cached

        self.cache_name = 'ines.api_cache_decorator %s %s' % (wrapped.__module__, wrapped.__qualname__)
        self.wrapper = wrapper
        return wrapper

    def child(self, expire_seconds=MARKER):
        if expire_seconds is MARKER:
            expire_seconds = self.expire_seconds

        new = api_cache_decorator(expire_seconds=expire_seconds)
        new.father = self
        self.children.append(new)
        return new

    def expire(self, api_session, expire_children=False, ignore_father=False):
        if self.wrapper and self.cache_name:
            if expire_children and self.children:
                for child in self.children:
                    child.expire(api_session, ignore_father=True)

            clear_paths = []
            for app_session in api_session.applications.asdict().values():
                cache_path = app_session.cache.path
                if cache_path not in clear_paths:
                    clear_paths.append(cache_path)
                    app_session.cache.remove(self.cache_name)

            if not ignore_father and self.father:
                self.father.expire(api_session)

            return True

        return False


def clear_lock_key(key):
    return clean_string(key).lower().strip().replace(' ', '')


def api_lock_decorator(prefix=None, args_indexes=None, kwargs_names=None, clear_keys_method=None):
    def decorator(wrapped):
        pre_name = prefix and prefix or 'ines.api_lock_decorator %s %s' % (wrapped.__module__, wrapped.__qualname__)
        clear_method = clear_keys_method or clear_lock_key

        @wraps(wrapped)
        def wrapper(cls, *args, **kwargs):
            names = []

            if args_indexes:
                for index in args_indexes:
                    names.append(clear_method(args[index]))

            if kwargs_names:
                for name in kwargs_names:
                    names.append(clear_method(kwargs[name]))

            lock_name = names and ('%s %s' % (pre_name, ' '.join(names))) or pre_name
            print(111, lock_name, type(lock_name))

            try:
                cls.config.cache.lock(lock_name)
                return wrapped(cls, *args, **kwargs)
            finally:
                cls.config.cache.unlock(lock_name)

        return wrapper
    return decorator
