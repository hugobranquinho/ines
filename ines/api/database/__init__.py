# -*- coding: utf-8 -*-

from pyramid.decorator import reify
import transaction

from ines.api import BaseSession


class BaseSQLSession(BaseSession):
    def flush(self):
        self.session.flush()
        transaction.commit()

    @reify
    def session(self):
        return self.api_session_manager.db_session()

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
