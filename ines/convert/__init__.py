# -*- coding: utf-8 -*-

from pyramid.compat import is_nonstr_iter

from ines.convert.codes import (inject_junk,
                                make_sha256)

from ines.convert.dates import (date_to_timestamp,
                                maybe_date,
                                maybe_datetime)

from ines.convert.strings import (camelcase,
                                  force_string,
                                  force_unicode,
                                  maybe_integer,
                                  maybe_null,
                                  maybe_unicode,
                                  pluralizing_key,
                                  pluralizing_word,
                                  uncamelcase)


def maybe_list(value):
    if value is None:
        return []
    elif not is_nonstr_iter(value):
        return [value]
    else:
        return list(value)


def maybe_set(value):
    if value is None:
        return set()
    elif not is_nonstr_iter(value):
        return set([value])
    else:
        return set(value)
