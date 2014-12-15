# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

from pyramid.decorator import reify
from pyramid.settings import asbool
from repoze.tm import default_commit_veto
from repoze.tm import TM as RepozeTM
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from zope.sqlalchemy import ZopeTransactionExtension
import transaction

from ines.api import BaseSession
from ines.api import BaseSessionClass
from ines.utils import MissingDict


SQL_DBS = MissingDict()


class BaseDatabaseSessionClass(BaseSessionClass):
    __api_name__ = 'database'
    __middlewares__ = [(0, RepozeTM, {'commit_veto': default_commit_veto})]

    def __init__(self, *args, **kwargs):
        super(BaseDatabaseSessionClass, self).__init__(*args, **kwargs)

        settings = self.config.settings
        sql_path = settings['sql.path']

        kwargs = {
            'encoding': settings.get('sql.encoding', 'utf8')}

        if sql_path.startswith('mysql://'):
            kwargs['mysql_engine'] = settings.get('sql.mysql_engine', 'InnoDB')

        if 'sql.debug' in settings:
            kwargs['debug'] = asbool(settings['sql.debug'])
        else:
            kwargs['debug'] = self.config.debug

        self.db_session = initialize_sql(
            self.config.application_name,
            sql_path,
            **kwargs)


class BaseDatabaseSession(BaseSession):
    __api_name__ = 'database'

    def flush(self):
        self.session.flush()
        transaction.commit()

    @reify
    def session(self):
        return self.api_session_class.db_session()

    def rollback(self):
        transaction.abort()

    def direct_insert(self, obj):
        values = {}
        for column in obj.__table__.c:
            name = column.name
            value = getattr(obj, name, None)
            if value is None and column.default:
                values[name] = column.default.execute()
            else:
                values[name] = value

        return (
            obj.__table__
            .insert(values)
            .execute(autocommit=True))

    def direct_delete(self, obj, query):
        return bool(
            obj.__table__
            .delete(query)
            .execute(autocommit=True)
            .rowcount)

    def direct_update(self, obj, query, values):
        for column in obj.__table__.c:
            name = column.name
            if name not in values and column.onupdate:
                values[name] = column.onupdate.execute()

        return (
            obj.__table__
            .update(query)
            .values(values)
            .execute(autocommit=True))


def initialize_sql(
        application_name,
        sql_path,
        encoding='utf8',
        mysql_engine='InnoDB',
        debug=False):

    sql_path = '%s?charset=%s' % (sql_path, encoding)
    SQL_DBS[application_name]['sql_path'] = sql_path
    is_mysql = sql_path.lower().startswith('mysql://')

    if is_mysql:
        base = SQL_DBS[application_name].get('base')
        if base is not None:
            append_arguments(base, 'mysql_charset', encoding)

    metadata = SQL_DBS[application_name].get('metadata')

    # Set defaults for MySQL tables
    if is_mysql and metadata:
        for table in metadata.sorted_tables:
            append_arguments(table, 'mysql_engine', mysql_engine)
            append_arguments(table, 'mysql_charset', encoding)

    SQL_DBS[application_name]['engine'] = engine = create_engine(
        sql_path,
        echo=debug,
        poolclass=NullPool,
        encoding=encoding)

    session_maker = sessionmaker(extension=ZopeTransactionExtension())
    session = scoped_session(session_maker)
    session.configure(bind=engine)
    SQL_DBS[application_name]['session'] = session

    if metadata is not None:
        metadata.bind = engine
        metadata.create_all(engine)

        # Force indexes creation
        for table in metadata.sorted_tables:
            if table.indexes:
                for index in table.indexes:
                    try:
                        index.create()
                    except OperationalError:
                        pass

    return session


def sql_declarative_base(application_name, encoding='utf8'):
    metadata = MetaData()
    metadata.application_name = application_name
    SQL_DBS[application_name]['metadata'] = metadata

    base = declarative_base(metadata=metadata)
    SQL_DBS[application_name]['base'] = base
    return base


def append_arguments(obj, key, value):
    arguments = getattr(obj, '__table_args__', None)
    if arguments is None:
        arguments = obj.__table_args__ = {key: value}

    elif isinstance(arguments, dict):
        if key not in arguments:
            arguments[key] = value

    elif isinstance(arguments, tuple):
        last_arguments_dict = None
        new_arguments = list(arguments)
        for argument in new_arguments:
            if isinstance(argument, dict):
                last_arguments_dict = argument
                if key in argument:
                    break
        else:
            if last_arguments_dict is None:
                new_arguments.append({key: value})
            else:
                last_arguments_dict[key] = value

            obj.__table_args__ = tuple(new_arguments)
