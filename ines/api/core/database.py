# -*- coding: utf-8 -*-

import datetime

from pyramid.decorator import reify
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import Unicode
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import aliased

from ines.api.database.sql import Options
from ines.api.database.sql import sql_declarative_base
from ines.exceptions import Error
from ines.utils import make_uuid_hash


class CoreTypesMissing(dict):
    def __missing__(self, key):
        self[key] = {
            'table': None,
            'parent': None,
            'childs': set(),
            'branches': set()}
        return self[key]


CORE_TYPES = CoreTypesMissing()
CORE_KEYS = set()
NOW_DATE = datetime.datetime.now
DeclarativeBase = sql_declarative_base('core')


class CoreAliased(dict):
    def __missing__(self, key):
        self[key] = aliased(Core)
        return self[key]


class Core(DeclarativeBase):
    __tablename__ = 'core'

    id = Column(Integer, primary_key=True, nullable=False)
    key = Column(Unicode(70), unique=True, index=True, nullable=False)
    # If null, means this object is a relation type
    type = Column(Unicode(50), index=True)
    parent_id = Column(Integer, ForeignKey('core.id'))

    start_date = Column(DateTime)
    end_date = Column(DateTime)
    updated_date = Column(
        DateTime,
        default=func.now(), onupdate=func.now(),
        nullable=False)
    created_date = Column(DateTime, default=func.now(), nullable=False)

    def make_key(self):
        return make_uuid_hash()


CORE_TYPES['core']['table'] = Core


class CoreColumnParent(object):
    def __init__(self, table, attribute):
        self._table = table
        self.attribute = attribute
        self.with_label = None

    def clone(self):
        return CoreColumnParent(self._table, self.attribute)

    @reify
    def table(self):
        return self._table.__table__

    @reify
    def name(self):
        return '%s.%s' % (self._table.__tablename__, self.attribute)

    def __repr__(self):
        return self.name

    def get_core_column(self):
        return getattr(Core, self.attribute)

    def get_alias_column(self, alias):
        column = getattr(alias, self.attribute)
        if self.with_label:
            return column.label(self.with_label)
        else:
            return column

    def label(self, name):
        self.with_label = name
        return self


def replace_core_attribute(wrapped):
    def decorator(self):
        name = wrapped.__name__
        value = CoreColumnParent(self, name)
        setattr(self, name, value)
        CORE_KEYS.add(name)
        return value
    return decorator


class CoreType(object):
    core_name = None

    @declared_attr
    def __tablename__(self):
        if self.core_name in CORE_TYPES:
            message = u'Core "%s" already defined' % self.core_name
            raise Error('core', message)
        else:
            CORE_TYPES[self.core_name]['table'] = self

            core_relation = getattr(self, 'core_relation', None)
            if core_relation:
                relation, relation_table = core_relation
                if relation == 'parent':
                    CORE_TYPES[self.core_name]['parent'] = relation_table
                    CORE_TYPES[relation_table.core_name]['childs'].add(self)
                elif relation == 'branch':
                    CORE_TYPES[relation_table.core_name]['branches'].add(self)
                else:
                    raise ValueError('Invalid relation type')

            tablename = 'core_%s' % self.core_name
            setattr(self, '__tablename__', tablename)
            return tablename

    @declared_attr
    @replace_core_attribute
    def key(self):
        pass

    @declared_attr
    @replace_core_attribute
    def type(self):
        pass

    @declared_attr
    @replace_core_attribute
    def start_date(self):
        pass

    @declared_attr
    @replace_core_attribute
    def end_date(self):
        pass

    @declared_attr
    @replace_core_attribute
    def updated_date(self):
        pass

    @declared_attr
    @replace_core_attribute
    def created_date(self):
        pass

    @declared_attr
    @replace_core_attribute
    def parent_id(self):
        pass

    @declared_attr
    def id_core(self):
        return Column(
            Integer, ForeignKey(Core.id),
            primary_key=True, nullable=False)


class CoreOptions(Options):
    def add_table(self, table, ignore=None, add_name=None):
        Options.add_table(self, table, ignore=ignore, add_name=add_name)

        columns = table.__dict__.keys()
        for key in columns:
            maybe_column = getattr(table, key)
            if isinstance(maybe_column, CoreColumnParent):
                if not ignore or key not in ignore:
                    if add_name:
                        key = '%s_%s' % (add_name, key)
                    self.add_column(key, maybe_column.clone())

        start_alias = table.start_date.clone()
        end_alias = table.end_date.clone()
        key = 'active'
        if add_name:
            key = '%s_%s' % (add_name, key)

        self.add_column(
            key,
            and_(or_(start_alias == None, start_alias <= func.now()),
                 or_(end_alias == None, end_alias <= func.now())).label(key))

from sqlalchemy import and_
from sqlalchemy import or_


def find_parent_tables(table):
    # Find parent tables
    tables = set()
    while True:
        core_relation = getattr(table, 'core_relation', None)
        if not core_relation:
            break
        relation, table = core_relation
        tables.add(table)
    return tables
