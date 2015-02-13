# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

import datetime

from ines.api import BaseSessionManager
from ines.api import BaseSession
from ines.convert import force_string
from ines.middlewares.logs import LoggingMiddleware
from ines.utils import format_json_response


NOW_DATE = datetime.datetime.now


class BaseLogSessionManager(BaseSessionManager):
    __api_name__ = 'logging'
    __middlewares__ = [LoggingMiddleware]


class BaseLogSession(BaseSession):
    __api_name__ = 'logging'

    def log(self, code, message, level='INFO'):
        header = ('-' * 30) + ' ' + level + ' ' + ('-' * 30)
        print header

        arguments = [
            ('Application', self.request.application_name),
            ('Code', code),
            ('URL', self.request.url),
            ('Date', NOW_DATE()),
            ('Language', self.request.locale_name),
            ('IP address', self.request.ip_address)]

        if self.request.authenticated:
            arguments.extend([
                ('Session type', self.request.authenticated.session_type),
                ('Session id', self.request.authenticated.session_id)])
        else:
            arguments.append(('Session', 'Without autentication'))

        bigger = max(len(k) for k, v in arguments)
        for key, value in arguments:
            print key, ' ' * (bigger - len(key)), ':', force_string(value)

        print
        try:
            message = force_string(message)
            for line in message.split('\n'):
                print '  %s' % line
        except UnicodeEncodeError:
            pass

        print '-' * len(header)

    def log_debug(self, code, message):
        if self.config.debug:
            return self.log(
                code,
                message,
                level='DEBUG')

    def log_info(self, code, message):
        return self.log(
            code,
            message,
            level='INFO')

    def log_warning(self, code, message):
        return self.log(
            code,
            message,
            level='WARNING')

    def log_error(self, code, message):
        return self.log(
            code,
            message,
            level='ERROR')

    def log_critical(self, code, message):
        return self.log(
            code,
            message,
            level='CRITICAL')
