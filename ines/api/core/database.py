# -*- coding: utf-8 -*-

import datetime

from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import Unicode
from sqlalchemy.ext.declarative import declared_attr

from ines.api.database.sql import sql_declarative_base
from ines.utils import make_unique_hash


NOW = datetime.datetime.now


class _Base(object):
    __table_alias__ = None


class Base(_Base):
    __key_length__ = 7
    __core_type__ = 'base'

    id = Column(Integer, primary_key=True, nullable=False)

    @declared_attr
    def key(self):
        database_key_length = self.__key_length__ + 5
        return Column(Unicode(database_key_length), unique=True, index=True, nullable=False)

    @declared_attr
    def parent_id(self):
        if hasattr(self, '__parent__'):
            return Column(Integer, ForeignKey(self.__parent__.id), nullable=False)

    updated_date = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    created_date = Column(DateTime, default=func.now(), nullable=False)

    def make_key(self):
        return make_unique_hash(self.__key_length__)


class ActiveBase(Base):
    __core_type__ = 'active'

    start_date = Column(DateTime)
    end_date = Column(DateTime)

    @property
    def active(self):
        if not hasattr(self, '_active'):
            now = NOW()
            self._active = bool(
                (not self.start_date or self.start_date <= now)
                and (not self.end_date or self.end_date > now))
        return self._active


class BranchBase(_Base):
    __core_type__ = 'branch'

    @declared_attr
    def id(self):
        parent = self.__parent__
        if not hasattr(parent, '__branches__'):
            parent.__branches__ = []
        parent.__branches__.append(self)
        return Column(Integer, ForeignKey(parent.id), primary_key=True, nullable=False)


ActiveCore = sql_declarative_base('ines.core', cls=ActiveBase)
BranchCore = sql_declarative_base('ines.core', cls=BranchBase)
Core = sql_declarative_base('ines.core', cls=Base)
