# -*- coding: utf-8 -*-

import datetime
import errno
from os import getpgid
from os.path import isfile
from os.path import join as join_path
from tempfile import gettempdir

from pyramid.decorator import reify
from pyramid.settings import asbool
import venusian

from ines import DOMAIN_NAME
from ines import PROCESS_ID
from ines import SYSTEM_VERSION
from ines.api import BaseSession
from ines.api import BaseSessionManager
from ines.convert import maybe_list
from ines.cron import Cron
from ines.exceptions import LockTimeout
from ines.exceptions import NoMoreDates
from ines.interfaces import IBaseSessionManager
from ines.request import make_request
from ines.system import start_system_thread


NOW = datetime.datetime.now

JOBS = []
RUNNING_JOBS = []

JOBS_REPORT_KEY = 'jobs report %s' % DOMAIN_NAME
JOBS_LOCK_KEY = lambda k: 'jobs lock %s' % k
JOBS_IMMEDIATE_KEY = 'jobs immediate run'


class BaseJobsManager(BaseSessionManager):
    __api_name__ = 'jobs'

    def __init__(self, *args, **kwargs):
        super(BaseJobsManager, self).__init__(*args, **kwargs)

        self.save_reports = asbool(self.settings.get('save_reports', True))
        self.server_domain_name = self.settings.get('server_domain_name')
        self.active = bool(
            not self.server_domain_name
            or self.server_domain_name == DOMAIN_NAME)

        self.domain_names = set(self.settings.get('domain_names', ''))
        self.domain_names.add(DOMAIN_NAME)

        try:
            import transaction
            self.transaction = transaction
        except ImportError:
            self.transaction = None

        if self.active:
            temporary_dir = gettempdir()
            domain_start_filename = 'jobs domain %s started' % DOMAIN_NAME
            domain_start_file_path = join_path(temporary_dir, domain_start_filename)

            lock_key = 'jobs monitor start check'
            self.config.cache.lock(lock_key, timeout=10)

            try:
                start_thread = not isfile(domain_start_file_path)
                if not start_thread:
                    try:
                        with open(domain_start_file_path, 'r') as f:
                            process_id = int(f.read())
                    except (IOError, ValueError):
                        start_thread = True
                    else:
                        try:
                            getpgid(process_id)
                        except OSError as error:
                            if error.errno is errno.ESRCH:
                                start_thread = True
                            else:
                                raise

                if start_thread:
                    with open(domain_start_file_path, 'w') as f:
                        f.write(str(PROCESS_ID))
            finally:
                self.config.cache.unlock(lock_key)

            # Start only one Thread for each domain
            if start_thread:
                start_system_thread('jobs_monitor', self.run_monitor)
                print 'Running jobs monitor on PID %s' % PROCESS_ID

    def system_session(self, apijob):
        environ = {
            'HTTP_HOST': DOMAIN_NAME,
            'PATH_INFO': '/%s' % apijob.name,
            'SERVER_NAME': DOMAIN_NAME,
            'REMOTE_ADDR': '127.0.0.1',
            'wsgi.url_scheme': 'job',
            'HTTP_USER_AGENT': SYSTEM_VERSION}

        request = make_request(self.config, environ)
        return self(request)

    def add_job(self, api_name, wrapped, settings):
        apijob = APIJob(self, api_name, wrapped.__name__, settings)
        JOBS.append(apijob)

        def run_job():
            return self.register_immediate_job_run(apijob)
        wrapped.run_job = run_job

        if self.active:
            self.update_job_report_info(apijob, called_date=apijob.last_called_date, as_add=True)

    def register_immediate_job_run(self, apijob):
        self.config.cache.append_value(JOBS_IMMEDIATE_KEY, apijob.name, expire=None)

    def immediate_job_run(self, name):
        return self.register_immediate_job_run(get_job(name))

    def run_monitor(self):
        try:
            self.validate_daemons()

            immediate_jobs = set(self.config.cache.get_values(JOBS_IMMEDIATE_KEY, expire=None))
            for apijob in JOBS:
                run_job = False
                if apijob.name in immediate_jobs:
                    run_job = True
                    immediate_jobs.remove(apijob.name)
                elif apijob.will_run() and apijob.next_date <= NOW():
                    run_job = True

                if run_job:
                    try:
                        daemon = start_system_thread(
                            'job_%s' % apijob.name,
                            apijob,
                            sleep_method=False)
                    except KeyError:
                        pass
                    else:
                        RUNNING_JOBS.append((apijob, daemon))

            self.config.cache.replace_values(JOBS_IMMEDIATE_KEY, immediate_jobs, expire=None)

        except Exception as error:
            self.system_session(apijob).logging.log_critical(
                'jobs_undefined_error',
                str(error))
            return 5
        else:
            return 0.5

    def update_job_report_info(self, apijob, called_date=None, as_add=False):
        if as_add or self.save_reports:
            info = self.config.cache.get(JOBS_REPORT_KEY, expire=None) or {}

            job_info = info.setdefault(apijob.name, {})
            job_info['next'] = apijob.next_date
            job_info['active'] = apijob.active
            if called_date:
                job_info.setdefault('called', []).append(called_date)

            if as_add:
                job_info['start'] = NOW()

            self.config.cache.put(JOBS_REPORT_KEY, info, expire=None)

    def get_active_jobs(self, application_names=None):
        jobs = {}
        application_names = maybe_list(application_names)
        for domain_name in self.domain_names:
            domain_info = self.config.cache.get(JOBS_REPORT_KEY, expire=None)
            if not domain_info:
                continue

            for name, info in domain_info.items():
                application_name = get_job_application_name(name)
                if not application_names or application_name in application_names:
                    if name in jobs:
                        info_next = info['next']
                        added_info_next = jobs[name]['next']
                        if info_next:
                            if not added_info_next or added_info_next > info_next:
                                jobs[name]['next'] = info_next

                        if info['start'] < jobs[name]['start']:
                            jobs[name]['start'] = info['start']
                        if info['active']:
                            jobs[name]['active'] = True
                        if info['called']:
                            jobs[name].setdefault('called', []).extend(info['called'])
                            jobs[name]['called'].sort()
                    else:
                        jobs[name] = info

                        apijob = get_job(name)
                        jobs[name]['title'] = apijob.title if apijob else None

        return jobs

    def validate_daemons(self):
        if RUNNING_JOBS:
            for apijob, daemon in RUNNING_JOBS:
                if not daemon.isAlive():
                    # Close thread
                    daemon.join()

                    try:
                        # Update report
                        self.update_job_report_info(apijob, called_date=apijob.last_called_date)
                    finally:
                        # Finally, remove daemon reference
                        RUNNING_JOBS.remove((apijob, daemon))


class BaseJobsSession(BaseSession):
    __api_name__ = 'jobs'

    def after_job_running(self):
        if hasattr(self.api, 'database') and hasattr(self.api.database, 'flush'):
            self.api.database.flush()

        if self.api_session_manager.transaction:
            self.api_session_manager.transaction.commit()

    def get_active_jobs(self, *args, **kwargs):
        return self.api_session_manager.get_active_jobs(*args, **kwargs)

    def immediate_job_run(self, name):
        return self.api_session_manager.immediate_job_run(name)


def job(**settings):
    def decorator(wrapped):
        def callback(context, name, ob):
            iob = context.config.registry.queryUtility(IBaseSessionManager, ob.__api_name__)
            if iob is not None and issubclass(iob.session, ob):
                context.jobs_manager.add_job(ob.__api_name__, wrapped, settings)

        venusian.attach(
            wrapped,
            callback,
            category='ines.jobs')

        return wrapped
    return decorator


class APIJob(object):
    def __init__(self, api_session_manager, api_name, wrapped_name, settings):
        self.api_session_manager = api_session_manager
        self.api_name = api_name
        self.wrapped_name = wrapped_name

        self.active = False
        self.next_date = None
        self.updating = False
        self.last_called_date = None

        self.domain_name = settings.pop('domain_name', None)
        self.title = settings.pop('title', None)

        self.cron = Cron(**settings)

        self.enable()

    def __repr__(self):
        return '%s (%s)' % (self.name, self.next_date)

    @reify
    def name(self):
        return '%s:%s.%s' % (
            self.application_name,
            self.api_name,
            self.wrapped_name)

    @property
    def application_name(self):
        return self.api_session_manager.config.application_name

    def disable(self):
        if self.active:
            self.active = False
            self.next_date = None

    def enable(self):
        if not self.domain_name:
            self.active = True
        elif self.domain_name == DOMAIN_NAME:
            self.active = True
        else:
            self.active = False
            self.next_date = None

        if self.active and not self.next_date:
            self.find_next()

    def find_next(self):
        if self.active:
            try:
                self.next_date = self.cron.find_next(NOW())
            except NoMoreDates:
                self.next_date = None
        else:
            self.next_date = None

    def will_run(self):
        return bool(self.active and not self.updating and self.next_date)

    def __call__(self):
        if self.will_run():
            api_session = self.api_session_manager.system_session(self)

            lock_key = JOBS_LOCK_KEY(self.name)
            if lock_key not in self.api_session_manager.config.cache.lockme:
                try:
                    self.api_session_manager.config.cache.lock(lock_key, timeout=1)
                except LockTimeout:
                    api_session.logging.log_error('job_locked', u'Job already running.')
                else:
                    self.updating = True
                    self.last_called_date = NOW()

                    session = getattr(api_session, self.api_name)
                    try:
                        getattr(session, self.wrapped_name)()
                    except (BaseException, Exception) as error:
                        api_session.logging.log_critical('jobs_error', str(error))
                    else:
                        jobs_session = getattr(api_session, self.api_session_manager.__api_name__)
                        jobs_session.after_job_running()
                finally:
                    self.updating = False
                    self.api_session_manager.config.cache.unlock(lock_key)
                    self.find_next()


def get_job_application_name(name):
    application_name, method_name = name.split(':', 1)
    return application_name


def get_job(name):
    for job in JOBS:
        if job.name == name:
            return job
