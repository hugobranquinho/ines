# -*- coding: utf-8 -*-

from ines.api import BaseSession
from ines.api import BaseSessionManager


class BaseDatabaseSessionManager(BaseSessionManager):
    __api_name__ = 'database'


class BaseDatabaseSession(BaseSession):
    __api_name__ = 'database'
