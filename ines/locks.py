# -*- coding: utf-8 -*-

import errno
from os import getpgid
from os import remove as remove_file
from os import walk as walk_on_path
from os.path import join as join_paths
from os.path import isfile
from time import sleep
from time import time as NOW_TIME

from repoze.lru import LRUCache

from ines import DEFAULT_RETRY_ERRNO
from ines import DOMAIN_NAME
from ines import MARKER
from ines import PROCESS_ID
from ines.convert import force_string
from ines.convert import force_unicode
from ines.convert import make_sha256
from ines.convert import maybe_integer
from ines.convert import maybe_set
from ines.exceptions import LockTimeout
from ines.system import start_system_thread
from ines.system import thread_is_running
from ines.utils import file_modified_time
from ines.utils import get_dir_filenames
from ines.utils import get_file_binary
from ines.utils import make_dir
from ines.utils import make_uuid_hash
from ines.utils import put_binary_on_file
from ines.utils import remove_file
from ines.utils import remove_file_quietly


class LockMe(object):
    def __init__(
            self,
            path,
            timeout=30,
            delete_lock_on_timeout=False,
            retry_errno=None,
            retries=3,
            lru_size=1000):

        self.path = make_dir(path)
        self.path_cache = LRUCache(lru_size)
        self.timeout = int(timeout)
        self.delete_lock_on_timeout = delete_lock_on_timeout
        self.retries = maybe_integer(retries) or 3
        self.retry_errno = maybe_set(retry_errno)
        self.retry_errno.update(DEFAULT_RETRY_ERRNO)

        # Clean locks!
        self.clean_junk_locks_as_daemon()

    def get_file_path(self, name):
        name = force_string(name)
        path = self.path_cache.get(name)
        if not path:
            name_256 = make_sha256(name)
            path = join_paths(self.path, name_256[0], name_256)
            self.path_cache.put(name, path)
        return path

    def get_folder_path(self, name):
        name_256 = make_sha256(name)
        return join_paths(self.path, name_256[0])

    def __contains__(self, name):
        return isfile(self.get_file_path(name))

    def lock(self, name, timeout=MARKER, delete_lock_on_timeout=MARKER):
        path = self.get_file_path(name)
        lock_code = make_uuid_hash()
        lock_name = '%s %s %s' % (DOMAIN_NAME, PROCESS_ID, lock_code)
        lock_name_to_file = lock_name + '\n'

        position = None
        while position is None:
            # Enter on wait list
            put_binary_on_file(path, lock_name_to_file, mode='ab', retries=self.retries, retry_errno=self.retry_errno)

            # Find my wait position
            binary = get_file_binary(path, retries=self.retries, retry_errno=self.retry_errno)
            if binary:
                for i, code in enumerate(binary.splitlines()):
                    if code.split()[-1] == lock_code:
                        position = i
                        break

        if position is 0:
            # Me first! Thank you!
            return None

        # Define expire time
        expire_time = None
        if timeout is MARKER:
            timeout = self.timeout
        if timeout:
            expire_time = NOW_TIME() + int(timeout)

        # Lock my wait position
        position_path = '%s.%s' % (path, position)
        put_binary_on_file(position_path, lock_name, retries=self.retries, retry_errno=self.retry_errno)

        # Wait until my locked position is cleaned
        while isfile(position_path):
            sleep(0.1)

            if expire_time and NOW_TIME() > expire_time:
                # Im done! No more wait for me!
                try:
                    remove_file(position_path)
                except OSError as error:
                    if error.errno is errno.ENOENT:
                        # Wait.. after all.. its show time!
                        return None
                    else:
                        raise

                if delete_lock_on_timeout is MARKER:
                    delete_lock_on_timeout = self.delete_lock_on_timeout
                if delete_lock_on_timeout:
                    self.unlock()
                    # Sorry, you need to do everything again
                    return self.lock(name, timeout=timeout, delete_lock_on_timeout=delete_lock_on_timeout)
                else:
                    # Clean locks!
                    self.clean_junk_locks_as_daemon()

                    message = 'Timeout (%ss) on lock "%s". Delete (%s*) to unlock' % (timeout, name, path)
                    raise LockTimeout(message, position_path)

        remove_file_quietly(position_path, retries=self.retries, retry_errno=self.retry_errno)

    def unlock(self, name):
        name_256 = make_sha256(name)
        pattern_name = name_256 + '.'
        folder_path = join_paths(self.path, name_256[0])

        # Lookup for locked positions
        files = []
        for filename in get_dir_filenames(folder_path):
            if filename.startswith(pattern_name):
                position = int(filename.split('.', 1)[1])
                files.append((position, filename))
        if files:
            files.sort()

            for position, filename in files:
                file_path = join_paths(folder_path, filename)
                if remove_file(
                        file_path,
                        retries=self.retries,
                        retry_errno=self.retry_errno):
                    return True

        # If no position found, delete base lock
        return remove_file_quietly(self.get_file_path(name), retries=self.retries, retry_errno=self.retry_errno)

    def clean_junk_locks(self):
        for path, dirnames, filenames in walk_on_path(self.path):
            filenames = filenames or []
            for dirname in dirnames:
                folder_path = join_paths(path, dirname)
                for filename in get_dir_filenames(folder_path):
                    if not filename.startswith('.'):
                        filenames.append(join_paths(dirname, filename))

            for filename in filenames:
                if filename.startswith('.'):
                    continue

                file_path = join_paths(path, filename)
                if '.' in filename:
                    # Delete inactive positions locks
                    binary = get_file_binary(file_path)
                    if binary:
                        info = binary.split()
                        if len(info) >= 2 and info[0] == DOMAIN_NAME and maybe_integer(info[1]):
                            try:
                                getpgid(int(info[1]))
                            except OSError as error:
                                if error.errno is errno.ESRCH:
                                    remove_file_quietly(
                                        file_path,
                                        retries=self.retries,
                                        retry_errno=self.retry_errno)

                else:
                    # Clean locks wait list
                    # Get last modified time, to check if file as been updated in the process
                    modified_time = file_modified_time(file_path)
                    if modified_time:
                        binary = get_file_binary(file_path)
                        if binary:
                            # Find alive locks
                            keep_codes = binary.splitlines()
                            for i, line in enumerate(keep_codes):
                                info = line.split()
                                if len(info) >= 2 and info[0] == DOMAIN_NAME and maybe_integer(info[1]):
                                    try:
                                        getpgid(int(info[1]))
                                    except OSError as error:
                                        if error.errno is errno.ESRCH:
                                            # Add empty line to keep position number
                                            keep_codes[i] = ''

                            # Check if file as been updated in the process
                            last_modified_time = file_modified_time(file_path)
                            if last_modified_time and modified_time == last_modified_time:
                                if not any(keep_codes):
                                    remove_file_quietly(file_path)
                                else:
                                    with open(file_path, 'wb') as f:
                                        f.write('\n'.join(keep_codes))

    def clean_junk_locks_as_daemon(self):
        if not thread_is_running('clean_junk_locks'):
            # Shhh.. Do it quietly!
            start_system_thread('clean_junk_locks', self.clean_junk_locks, sleep_method=False)


class LockMeMemcached(object):
    def __init__(
            self,
            url,
            timeout=30,
            delete_lock_on_timeout=False,
            **settings):

        from memcache import Client
        self.memcache = Client(url.split(';'), **settings)
        self.timeout = int(timeout)
        self.delete_lock_on_timeout = delete_lock_on_timeout

    def format_name(self, name):
        name = u'locks %s' % force_unicode(name)
        return force_string(make_sha256(name))

    def format_position_name(self, name_256, position):
        name = u'%s.%s' % (name_256, position)
        return force_string(make_sha256(name))

    def _contains(self, name_256):
        return self.memcache.get(name_256) is not None

    def __contains__(self, name):
        return self._contains(self.format_name(name))

    def lock(self, name, timeout=MARKER, delete_lock_on_timeout=MARKER):
        name_256 = self.format_name(name)
        lock_code = make_uuid_hash()
        lock_name = '%s %s %s' % (DOMAIN_NAME, PROCESS_ID, lock_code)

        position = None
        while position is None:
            self.memcache.add(name_256, '0')
            position = self.memcache.incr(name_256)

        if position is 1:
            # Me first! Thank you!
            return None

        # Define expire time
        expire_time = None
        if timeout is MARKER:
            timeout = self.timeout
        if timeout:
            expire_time = NOW_TIME() + int(timeout)

        # Lock my wait position
        position_name_256 = self.format_position_name(name_256, position)
        self.memcache.set(position_name_256, lock_name)

        # Wait until my locked position is cleaned
        while self._contains(position_name_256):
            sleep(0.1)

            if expire_time and NOW_TIME() > expire_time:
                # Im done! No more wait for me!
                if not self.memcache.delete(position_name_256):
                    # Wait.. after all.. its show time!
                    return None

                if delete_lock_on_timeout is MARKER:
                    delete_lock_on_timeout = self.delete_lock_on_timeout
                if delete_lock_on_timeout:
                    self.unlock()
                    # Sorry, you need to do everything again
                    return self.lock(name, timeout=timeout, delete_lock_on_timeout=delete_lock_on_timeout)
                else:
                    # Clean invalid locks!
                    if self.clean_junk_locks(name_256):
                        # Go again
                        return self.lock(name, timeout=timeout, delete_lock_on_timeout=delete_lock_on_timeout)

                    message = 'Timeout (%ss) on lock "%s". Delete (%s*) to unlock' % (timeout, name, name_256)
                    raise LockTimeout(message, name_256)

        self.memcache.delete(position_name_256)

    def unlock(self, name):
        name_256 = self.format_name(name)
        last_position = maybe_integer(self.memcache.get(name_256))
        if last_position:
            for position in range(1, last_position + 1):
                position_name_256 = self.format_position_name(name_256, position)
                if self.memcache.get(position_name_256) and self.memcache.delete(position_name_256):
                    return True

        # If no position found, delete base lock
        return self.memcache.delete(name_256)

    def clean_junk_locks(self, name_256):
        last_position = maybe_integer(self.memcache.get(name_256))
        if last_position:
            active_positions = 0
            for position in range(1, last_position + 1):
                position_name_256 = self.format_position_name(name_256, position)
                binary = self.memcache.get(position_name_256)
                if not binary:
                    continue

                info = binary.split()
                if len(info) >= 2 and info[0] == DOMAIN_NAME and info[1].isnumeric():
                    try:
                        getpgid(int(info[1]))
                    except OSError as error:
                        if error.errno is errno.ESRCH:
                            self.memcache.delete(position_name_256)
                            continue

                active_positions += 1

            if not active_positions:
                self.memcache.delete(name_256)
                return True
