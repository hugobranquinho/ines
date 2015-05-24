# -*- coding: utf-8 -*-

from pyramid.decorator import reify
from pyramid.settings import asbool
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from zope.sqlalchemy import ZopeTransactionExtension

from ines.api import BaseSession
from ines.api import BaseSessionManager
from ines.middlewares.repozetm import RepozeTMMiddleware
from ines.utils import MissingDict
from ines.utils import MissingSet


SQL_DBS = MissingDict()


class BaseSQLSessionManager(BaseSessionManager):
    __middlewares__ = [RepozeTMMiddleware]

    def __init__(self, *args, **kwargs):
        super(BaseSQLSessionManager, self).__init__(*args, **kwargs)
        import transaction
        self.transaction = transaction

        self.db_session = initialize_sql(
            getattr(self, '__database_name__', self.config.application_name),
            **get_sql_settings_from_config(self.config))


class BaseSQLSession(BaseSession):
    def flush(self):
        self.session.flush()
        self.api_session_manager.transaction.commit()

    @reify
    def session(self):
        return self.api_session_manager.db_session()

    def rollback(self):
        self.api_session_manager.transaction.abort()

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


def get_sql_settings_from_config(config):
    sql_path = config.settings['sql.path']
    kwargs = {
        'sql_path': sql_path,
        'encoding': config.settings.get('sql.encoding', 'utf8')}

    if sql_path.startswith('mysql://'):
        kwargs['mysql_engine'] = config.settings.get(
            'sql.mysql_engine',
            'InnoDB')

    if 'sql.debug' in config.settings:
        kwargs['debug'] = asbool(config.settings['sql.debug'])
    else:
        kwargs['debug'] = config.debug

    return kwargs


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

    indexed_columns = SQL_DBS[application_name]['indexed_columns'] = MissingSet()
    if metadata is not None:
        metadata.bind = engine
        metadata.create_all(engine)

        # Force indexes creation
        for table in metadata.sorted_tables:
            if table.indexes:
                for index in table.indexes:
                    for column in getattr(index.columns, '_all_columns'):
                        indexed_columns[table.name].add(column.name)

                    try:
                        index.create()
                    except OperationalError:
                        pass

    return session


def append_arguments(obj, key, value):
    arguments = getattr(obj, '__table_args__', None)
    if arguments is None:
        obj.__table_args__ = {key: value}

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
