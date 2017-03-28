# -*- coding: utf-8 -*-

from sqlalchemy import Enum
from sqlalchemy import func
from sqlalchemy import Numeric
from sqlalchemy import String

from ines.api.database.utils import get_schema_table
from ines.api.database.utils import get_table_column
from ines.cleaner import clean_string
from ines.cleaner import LOWER_MAPPING


POSTGRESQL_LOWER_AND_CLEAR = """
    CREATE OR REPLACE FUNCTION lower_and_clear(VARCHAR)
        RETURNS VARCHAR AS $$ SELECT translate(lower($1), '%s', '%s') $$
        LANGUAGE SQL""" % LOWER_MAPPING


def table_is_postgresql(table):
    return get_schema_table(table).__connection_type__ == 'postgresql'


def compare_as_non_ascii_filter(table, column_name, value):
    column = get_table_column(table, column_name)
    if table_is_postgresql(table):
        return postgresql_non_ascii_and_lower(column) == clean_string(value).lower()
    else:
        return column == value


def postgresql_non_ascii_and_lower(column, as_text=True):
    if hasattr(column, 'property'):
        columns = column.property.columns
        if len(columns) > 1:
            column = func.concat(*columns)
        else:
            column = column.property.columns[0]

    if isinstance(column.type, Enum):
        return column
    elif isinstance(column.type, String):
        return func.lower_and_clear(column)
    elif isinstance(column.type, Numeric):
        return column
    elif as_text:
        return func.text(column)
    else:
        return column
