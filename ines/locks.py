# -*- coding: utf-8 -*-

from os import getpgid
from os import listdir
from os import mkdir as make_dir
from os import remove as remove_file
from os import walk as walk_on_path
from os.path import join as join_paths
from os.path import isdir
from os.path import isfile
from threading import Thread
from time import sleep
from time import time
from uuid import uuid4

from pyramid.decorator import reify

from ines import DOMAIN_NAME
from ines import PROCESS_ID
from ines.convert import make_sha256
from ines.utils import file_modified_time


class LockMe(object):
    def __init__(
            self,
            lock_path,
            timeout=30,
            delete_lock_on_timeout=False):

        self.lock_path = lock_path
        if not isdir(self.lock_path):
            make_dir(self.lock_path, 0777)

        self.timeout = int(timeout)
        self.delete_lock_on_timeout = delete_lock_on_timeout

    def lock(self, name, timeout=None, expire=None, delete_lock_on_timeout=False):
        my_lock = _Lock(self.lock_path, name, timeout or self.timeout, expire=expire)
        try:
            my_lock.lock()
        except LockTimeout as error:
            if self.delete_lock_on_timeout or delete_lock_on_timeout:
                my_lock.unlock()
            else:
                self.clean_junk_locks(my_lock.folder_path, daemon=True)
            raise

    def unlock(self, name):
        _Lock(self.lock_path, name).unlock()

    def __contains__(self, name):
        return _Lock(self.lock_path, name, self.timeout).is_locked

    def clean_junk_locks(self, path, daemon=False):
        if daemon:
            # Shhh.. Do it quietly!
            daemon = Thread(target=self.clean_junk_locks, args=(path, ))
            daemon.setDaemon(True)
            daemon.start()
        else:
            for dirpath, dirnames, filenames in walk_on_path(path):
                for filename in filenames:
                    file_path = join_paths(dirpath, filename)
                    if not filename.startswith('.'):
                        if '.' in file_path:
                            self.clean_file_position_locks(file_path)
                        else:
                            self.clean_file_base_locks(file_path)

    def clean_file_position_locks(self, path):
        try:
            with open(path, 'rb') as f:
                binary = f.read()
        except IOError:
            pass
        else:
            info = binary.split()
            if len(info) == 3:
                domain_name, process_id, code = info
                if domain_name == DOMAIN_NAME:
                    process_id = int(process_id)
                    try:
                        getpgid(process_id)
                    except OSError:
                        remove_file_quietly(path)

    def clean_file_base_locks(self, path):
        tries = 10
        while tries:
            modified_time = file_modified_time(path)
            if not modified_time:
                # File deleted!
                break

            try:
                with open(path, 'rb') as f:
                    binary = f.read()
            except IOError:
                # File deleted!
                break

            # Find alive locks
            keep_codes = []
            for code in binary.split('\n'):
                info = code.split()
                if len(info) == 3:
                    domain_name, process_id, code_hex = info
                    if domain_name == DOMAIN_NAME:
                        process_id = int(process_id)
                        try:
                            getpgid(process_id)
                        except OSError:
                            keep_codes.append('')
                            continue
                keep_codes.append(code)

            keep_codes_str = '\n'.join(keep_codes)
            last_modified_time = file_modified_time(path)
            if not last_modified_time:
                # File deleted!
                break
            elif modified_time == last_modified_time:
                if not keep_codes:
                    remove_file_quietly(path)
                else:
                    with open(path, 'wb') as f:
                        f.write(keep_codes_str)
                # All done! End where!
                break

            tries -= 1


class _Lock(object):
    def __init__(self, lock_path, name, timeout=None, expire=None):
        self.name = name
        self.name_256 = make_sha256(name)
        self.first_name_letter = self.name_256[0]

        self.lock_path = lock_path
        self.file_path = join_paths(
            self.lock_path,
            self.first_name_letter,
            self.name_256)
        self.folder_created = False

        self.position = None

        self.expire_time = None
        self.timeout = int(timeout or 0)

        if expire:
            self.expire_time = time() + int(expire)

    @reify
    def code(self):
        return uuid4().hex

    @reify
    def folder_path(self):
        return join_paths(self.lock_path, self.first_name_letter)

    def create_folder_path(self):
        try:
            make_dir(self.folder_path, 0777)
            self.folder_created = True
        except OSError:
            if self.folder_created:
                raise

    @reify
    def position_file_path(self):
        if self.position is None:
            raise ValueError('No position found')
        else:
            return self.file_path + ('.%s' % self.position)

    def lock(self):
        # Create wait list for locks called, find my position
        save_code = '%s %s %s' % (DOMAIN_NAME, PROCESS_ID, self.code)

        try:
            with open(self.file_path, 'ab') as f:
                f.write(save_code + '\n')
        except IOError:
            self.create_folder_path()
            return self.lock()

        # Find my position
        try:
            with open(self.file_path, 'rb') as f:
                binary = f.read()
            codes = []
            for code in binary.split('\n'):
                if code:
                    codes.append(code.split()[-1])
        except IOError:
            self.create_folder_path()
            return self.lock()
        try:
            self.position = codes.index(self.code)
        except ValueError:
            return self.lock()

        if not self.position:
            # Me first! Thank you!
            return None

        try:
            with open(self.position_file_path, 'wb') as f:
                f.write(save_code)
        except IOError:
            self.create_folder_path()
            with open(self.position_file_path, 'wb') as f:
                f.write(save_code)

        while isfile(self.position_file_path):
            sleep(0.1)
            if self.expire_time and time() > self.expire_time:
                # Im done! No more wait for me!
                try:
                    remove_file(self.position_file_path)
                except OSError:
                    # Ahhhhh.. its show time!
                    break
                else:
                    message = (
                        'Timeout (%ss) on lock "%s" (%s)'
                        % (self.timeout, self.name, self.position_file_path))
                    raise LockTimeout(message, self.position_file_path)

    def locked_filenames(self):
        files = []
        try:
            filenames = listdir(self.folder_path)
        except OSError:
            pass
        else:
            pattern_name = self.name_256 + '.'
            for filename in filenames:
                if filename.startswith(pattern_name):
                    position = int(filename.split('.', 1)[1])
                    files.append((position, filename))
            if files:
                files.sort()

        return files

    def unlock(self):
        for position, filename in self.locked_filenames():
            remove_file_path = join_paths(self.folder_path, filename)
            try:
                remove_file_quietly(remove_file_path)
            except OSError:
                # Timeout was raised! Delete another!
                continue
            else:
                break
        else:
            remove_file_quietly(self.file_path)

    @property
    def is_locked(self):
        return isfile(self.file_path)


def remove_file_quietly(path):
    try:
        remove_file(path)
    except OSError:
        pass


class LockTimeout(Exception):
    def __init__(self, message, lock_path):
        super(LockTimeout, self).__init__(message)
        self.lock_path = lock_path
