# -*- coding: utf-8 -*-

import atexit
from os import getpid
from time import sleep
from threading import Thread

from ines.utils import MissingDict


PROCESS_RUNNING = set()
KILLED_PROCESS = set()
ALIVE_THREADS = MissingDict()


def while_system_running_factory():
    process_id = getpid()

    def replacer(sleep_seconds):
        if sleep_seconds < 2:
            while_time = 0.1
        else:
            while_time = 1

        slept = 0
        while process_id not in KILLED_PROCESS:
            sleep(while_time)

            slept += while_time
            if slept >= sleep_seconds:
                return True

        return False
    return replacer


def start_system_thread(
        name, method,
        args=None, kwargs=None,
        sleep_method=True):

    if sleep_method:
        def while_method(*w_args, **w_kwargs):
            sleep_time = 1
            factory = while_system_running_factory()
            while factory(sleep_time):
                sleep_time = abs(method(*w_args, **w_kwargs) or 1)
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
    return thread


def clean_dead_threads():
    process_id = getpid()
    for name, thread in ALIVE_THREADS[process_id].items():
        if not thread.isAlive():
            thread.join()
            ALIVE_THREADS[process_id].pop(name)


def thread_is_running(name):
    # Clean up function
    clean_dead_threads()
    process_id = getpid()
    return name in ALIVE_THREADS[process_id]


def exit_system():
    process_id = getpid()
    if process_id in KILLED_PROCESS:
        return None
    KILLED_PROCESS.add(process_id)

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

    print 'Process %s stopped!' % process_id


def register_thread(name, thread):
    # Clean up function
    clean_dead_threads()

    process_id = getpid()
    if name in ALIVE_THREADS[process_id]:
        raise KeyError('Thread "%s" already started' % name)
    else:
        ALIVE_THREADS[process_id][name] = thread


# Register on python default
atexit.register(exit_system)


# Register uwsgi if exists
try:
    import uwsgi
except ImportError:
    pass
else:
    def after_fork():
        PROCESS_RUNNING.add(getpid())
        uwsgi.atexit = exit_system
    uwsgi.post_fork_hook = after_fork
