# -*- coding: utf-8 -*-

from pyramid.compat import is_nonstr_iter

from ines.convert.codes import (inject_junk,
                                make_sha256,
                                make_sha256_no_cache)

from ines.convert.dates import (convert_timezone,
                                date_to_timestamp,
                                maybe_date,
                                maybe_datetime)

from ines.convert.metrics import (format_metric,
                                  format_metric_factory,
                                  metric_to_unicode,
                                  metric_to_unicode_factory)

from ines.convert.strings import (camelcase,
                                  force_string,
                                  force_unicode,
                                  json_dumps,
                                  maybe_integer,
                                  maybe_null,
                                  maybe_string,
                                  maybe_unicode,
                                  pluralizing_key,
                                  pluralizing_word,
                                  prepare_for_json,
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
