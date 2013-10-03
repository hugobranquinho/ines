# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
# 
# @author Hugo Branquinho <hugobranq@gmail.com>
# 
# $Id$

import transaction

from pyramid.settings import asbool
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import desc
from sqlalchemy import event
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.ext.declarative.api import _as_declarative
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from zope.sqlalchemy import ZopeTransactionExtension

from ines.api.database import BaseDatabaseClass
from ines.api.database import BaseDatabaseSession
from ines.cleaner import clean_percentage
from ines.convert import force_unicode
from ines.path import find_package_name
from ines.utils import cache_property
from ines.utils import find_settings
from ines.utils import get_method
from ines.utils import Options


DATABASES = {}


class BaseSQLClass(BaseDatabaseClass):
    def __init__(self, config, session, package_name):
        BaseDatabaseClass.__init__(self, config, session, package_name)

        sql_module_path = session.__module__
        sql_module = get_method(sql_module_path)
        sql_module  # pyflakes

        # Define required sql settings
        default_debug = self.settings['debug']
        create_tables = asbool(self.settings.get('sql_create_tables', True))
        self.settings.update({
            'sql_path': self.settings['sql_path'],
            'sql_debug': asbool(self.settings.get('sql_debug', default_debug)),
            'sql_encoding': self.settings.get('sql_encoding', 'utf8'),
            'sql_create_tables': create_tables})

        # Initialize SQLALchemy DBSession
        options = find_settings(self.settings, 'sql_')
        config = get_sql_config(package_name)
        config.configure(**options)
        self.settings['sql_session'] = config.session


class BaseSQLSession(BaseDatabaseSession):
    _base_class = BaseSQLClass

    def flush(self):
        session = getattr(self, '_session', None)
        if session is not None:
            session.flush()
            transaction.commit()
            del self._session

    @cache_property
    def session(self):
        return self.settings['sql_session']()

    def rollback(self):
        session = getattr(self, '_session', None)
        if session is not None:
            transaction.abort()
            del self._session

    def break_to_like(self, column, value):
        value = force_unicode(value)
        value = u'%'.join(value.split())
        value = clean_percentage(u'%' + value + u'%')
        value = force_unicode(value)
        return column.like(value)


class TableConfig(object):
    def __init__(self, sql_config, table_name, table, table_arguments):
        self.sql_config = sql_config
        self.table_name = table_name
        self.table = table
        self.table_arguments = table_arguments
        self.is_configurated = False

    @property
    def table_args(self):
        table_args = getattr(self.table, '__table_args__', None)
        if table_args is None:
            table_args = {}
            self.table.__table_args__ = table_args

        return table_args

    def set_argument(self, key, value):
        if isinstance(self.table_args, dict):
            if not self.table_args.has_key(key):
                self.table_args[key] = value

        elif isinstance(self.table_args, tuple):
            table_dict = None
            table_args = list(self.table_args)
            for arg in table_args:
                if isinstance(arg, dict):
                    table_dict = arg
                    if arg.has_key(key):
                        break
            else:
                if table_dict is None:
                    table_args.append({key: value})
                else:
                    table_dict[key] = value

                self.table.__table_args__ = tuple(table_args)

    def configure(self):
        if self.is_configurated:
            # Already configurated
            return

        self.table._decl_class_registry = self.sql_config.class_registry
        self.table.metadata = self.sql_config.metadata

        if self.sql_config.mysql_engine:
            self.set_argument('mysql_engine', self.sql_config.mysql_engine)

        if self.sql_config.encoding:
            self.set_argument('mysql_charset', self.sql_config.encoding)

        _as_declarative(self.table, self.table_name, self.table_arguments)

        # DDL configuration
        if hasattr(self.table, '_ddl'):
            for ddl_key, ddl_method in ctablels._ddl:
                event.listen(self.table.__table__, ddl_key, ddl_method)


class SQLConfig(object):
    def __init__(self, package_name):
        self.package_name = package_name
        self.tables = {}
        self.class_registry = {}

        self.path = None
        self.mysql_engine = None
        self.encoding = None
        self.debug = False
        self.engine = None
        self.session = None

        self.metadata = MetaData()
        self.metadata.package_name = package_name
        self.base = declarative_base(metadata=self.metadata,
                                     metaclass=inesDeclarativeMeta,
                                     class_registry=self.class_registry)

    def add_table(self, table, table_name, table_arguments):
        self.tables[table_name] = TableConfig(self,
                                              table_name,
                                              table,
                                              table_arguments)

    def configure(self,
                  path,
                  debug=False,
                  encoding=None,
                  mysql_engine=None,
                  pool_size=100,
                  pool_recycle=7200,
                  create_tables=True):

        self.path = path
        self.debug = debug
        self.mysql_engine = mysql_engine
        self.pool_size = int(pool_size)
        self.pool_recycle = int(pool_recycle)

        self.encoding = encoding
        if self.encoding:
            self.path = '%s?charset=%s' % (self.path, self.encoding)
            if not hasattr(self.base, '__table_args__'):
                self.base.__table_args__ = {}
            self.base.__table_args__['mysql_charset'] = self.encoding

        for table_config in self.tables.values():
            table_config.configure()

        self.engine = create_engine(self.path,
                                    echo=self.debug,
                                    encoding=self.encoding,
                                    poolclass=NullPool)

        session_maker = sessionmaker(extension=ZopeTransactionExtension())
        self.session = scoped_session(session_maker)
        self.session.configure(bind=self.engine)

        self.metadata.bind = self.engine
        if create_tables:
            self.metadata.create_all(self.engine)


def get_sql_config(package_name=None):
    package_name = package_name or find_package_name(level=1)
    config = DATABASES.get(package_name)
    if config is None:
        config = SQLConfig(package_name)
        DATABASES[package_name] = config

    return config


class inesDeclarativeMeta(DeclarativeMeta):
    def __init__(cls, classname, bases, dict_):
        if '_decl_class_registry' not in cls.__dict__:
            config = get_sql_config()
            config.add_table(cls, classname, dict_)

            # Lets change the classname to prevent duplications
            classname = '%s_%s' % (config.package_name, classname)

        type.__init__(cls, classname, bases, dict_)


def create_declarative_base(package_name=None, encoding='utf8'):
    config = get_sql_config(package_name)
    return config.base


class TablesSet(set):
    def have(self, table):
        return table in self

    def __contains__(self, table):
        table = getattr(table, '__table__', None)
        if table is not None:
            return set.__contains__(self, table)
        else:
            return False


class SQLOptions(Options):
    def add_sql(self, attribute, column):
        self.add_attribute_value(attribute, 'column', column)

    def get_columns(self, attributes=None, with_tables=False):
        if not attributes:
            attributes = self.keys()
            ignore_not_found = True
        else:
            ignore_not_found = False

        columns = []
        if with_tables:
            tables = TablesSet()
            def add_column(column):
                columns.append(column)
                if isinstance(column, Column):
                    table = column.table
                else:
                    table = column._element.table
                tables.add(table)
        else:
            add_column = columns.append

        for attribute in attributes:
            if attribute is not None:
                if not ignore_not_found:
                    add_column(self[attribute]['column'])
                else:
                    column = self[attribute].get('column')
                    if column is not None:
                        add_column(column)

        if with_tables:
            return columns, tables
        else:
            return columns

    def structure_order_by(self, *arguments):
        result = []
        add_order = result.append
        for argument in arguments:
            if isinstance(argument, (tuple, list)):
                column_name, reverse = argument
            else:
                column_name = argument
                reverse = False

            column = self[column_name]['column']
            if reverse:
                add_order(desc(column))
            else:
                add_order(column)

        return result
