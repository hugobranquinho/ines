# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

import atexit
from os import getpid
from time import sleep
from threading import Thread

from ines.utils import MissingDict


SYSTEM_RUNNING = []
ALIVE_THREADS = MissingDict()


class while_system_running_factory(object):
    def __init__(self):
        self.process_id = getpid()
        if self.process_id not in SYSTEM_RUNNING:
            SYSTEM_RUNNING.append(self.process_id)

    def __call__(self, sleep_seconds):
        if sleep_seconds < 1:
            while_time = 0.1
        else:
            while_time = 1

        slept = 0
        while self.process_id in SYSTEM_RUNNING:
            sleep(while_time)

            slept += while_time
            if slept >= sleep_seconds:
                return True

        return False


def start_system_thread(
        name, method,
        args=None, kwargs=None,
        sleep_method=True):

    if sleep_method:
        def while_method(*args, **kwargs):
            sleep_time = 1
            factory = while_system_running_factory()
            while factory(sleep_time):
                sleep_time = abs(method(*args, **kwargs) or 1)
    else:
        while_method = method

    # Daemon to validate config update
    thread = Thread(
        target=while_method,
        name=name,
        args=args or [],
        kwargs=kwargs or {})

    thread.setDaemon(True)
    thread.start()
    register_thread(name, thread)


def clean_dead_threads():
    process_id = getpid()
    for name, thread in ALIVE_THREADS[process_id].items():
        if not thread.isAlive():
            thread.join()
            ALIVE_THREADS[process_id].pop(name)

    if not ALIVE_THREADS.get(process_id):
        ALIVE_THREADS.pop(process_id)


KILLED = []
def exit_system():
    for pid in list(SYSTEM_RUNNING):
        SYSTEM_RUNNING.remove(pid)

    process_id = getpid()
    if process_id in KILLED:
        return None
    KILLED.append(process_id)

    print 'Stopping process %s...' % process_id

    count = 0
    while ALIVE_THREADS[process_id]:
        clean_dead_threads()

        if count and not count % 10:
            print 'Cant stop threads after %s tries...' % count
            for name, thread in ALIVE_THREADS[process_id].items():
                print ' ' * 4, name, thread

        sleep(0.5)
        count += 1


def register_thread(name, thread):
    clean_dead_threads()

    process_id = getpid()
    if name in ALIVE_THREADS[process_id]:
        raise KeyError('Thread "%s" already started' % name)
    else:
        ALIVE_THREADS[process_id][name] = thread


# Register on python default
atexit.register(exit_system)


# Register on uwsgi if exists
try:
    import uwsgi
except ImportError:
    pass
else:
    def after_fork():
        uwsgi.atexit = exit_system
    uwsgi.post_fork_hook = after_fork
