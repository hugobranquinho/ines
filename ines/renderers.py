# -*- coding: utf-8 -*-

import datetime
from json import dumps

from ines import DEFAULT_RENDERERS
from ines.convert import camelcase
from ines.convert import force_unicode


DATE = datetime.date
DATETIME = datetime.datetime


class CSV(object):
    def __call__(self, info):
        def _render(value, system):
            request = system.get('request')
            if request is not None:
                response = request.response
                ct = response.content_type
                if ct == response.default_content_type:
                    response.content_type = 'text/csv'

                output = request.registry.config.lookup_output_schema(
                    request.matched_route.name,
                    request_method=request.method)
                if output:
                    output = output[0]
                    node = output.schema.children[0]
                    value_lines = []

                    header = []
                    value_lines.append(header)
                    for child in node.children:
                        header.append(child.title)

                    for item_value in value:
                        row = []
                        value_lines.append(row)

                        if isinstance(item_value, dict):
                            for child in node.children:
                                row.append(item_value.get(camelcase(child.name)))
                        else:
                            for child in node.children:
                                row.append(getattr(item_value, child.name, None))

                    value = value_lines

            separator = u';'
            separator_replace = u','
            line_break = u'\n'
            line_break_replace = u' '

            lines = []
            for value_items in value:
                row = []
                for item in value_items:
                    if item is None:
                        item = u''

                    elif isinstance(item, bool):
                        if item:
                            item = u'true'
                        else:
                            item = u'false'

                    elif isinstance(item, (int, basestring, long)):
                        item = force_unicode(item)

                    elif isinstance(item, (DATE, DATETIME)):
                        item = item.isoformat()

                    else:
                        item = dumps(item)
                        print item

                    if u'"' in item:
                        item = u'"%s"' % item.replace(u'"', u'\\"')

                    item = item.replace(separator, separator_replace)
                    item = item.replace(line_break, line_break_replace)
                    row.append(item)

                lines.append(separator.join(row))
            return line_break.join(lines)

        return _render


csv_renderer_factory = CSV()  # bw compat
DEFAULT_RENDERERS['csv'] = csv_renderer_factory
