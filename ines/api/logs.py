# -*- coding: utf-8 -*-

import sys
from traceback import format_exception

from ines import NEW_LINE, NOW
from ines.api import BaseSession, BaseSessionManager
from ines.convert import to_string
from ines.middlewares.logs import LoggingMiddleware


class BaseLogSessionManager(BaseSessionManager):
    __api_name__ = 'logging'
    __middlewares__ = [LoggingMiddleware]


class BaseLogSession(BaseSession):
    __api_name__ = 'logging'

    def log(self, code, message, level='INFO', extra=None):
        level = level.upper()
        header = ('-' * 30) + ' ' + level + ' ' + ('-' * 30)
        print(header)

        arguments = [
            ('Application', self.request.application_name),
            ('Code', code),
            ('URL', self.request.url),
            ('Method', self.request.method),
            ('Date', NOW()),
            ('Language', self.request.locale_name),
            ('IP address', self.request.ip_address)]

        if extra:
            arguments.extend(extra.items())

        if self.request.authenticated:
            arguments.extend([
                ('Session type', self.request.authenticated.session_type),
                ('Session id', self.request.authenticated.session_id)])
        else:
            arguments.append(('Session', 'Without autentication'))

        bigger = max(len(k) for k, v in arguments)
        for key, value in arguments:
            print(key, ' ' * (bigger - len(key)), ':', to_string(value))

        if level == 'CRITICAL':
            sys_exc_info = sys.exc_info()  # type, value, traceback
            if sys_exc_info[2] is not None:
                print()
                print(''.join(format_exception(*sys_exc_info)))

        print()
        try:
            for line in to_string(message).split(NEW_LINE):
                print('  %s' % line)
        except UnicodeEncodeError:
            pass

        print('-' * len(header))

    def log_debug(self, code, message, **kwargs):
        if self.config.debug:
            return self.log(
                code,
                message,
                level='DEBUG',
                **kwargs)

    def log_warning(self, code, message, **kwargs):
        return self.log(
            code,
            message,
            level='WARN',
            **kwargs)

    def log_error(self, code, message, **kwargs):
        return self.log(
            code,
            message,
            level='ERROR',
            **kwargs)

    def log_critical(self, code, message, **kwargs):
        return self.log(
            code,
            message,
            level='CRITICAL',
            **kwargs)
