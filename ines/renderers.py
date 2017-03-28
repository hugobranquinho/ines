# -*- coding: utf-8 -*-

import codecs
from collections import defaultdict
from csv import QUOTE_ALL, QUOTE_MINIMAL, QUOTE_NONNUMERIC, QUOTE_NONE, writer as csv_writer
import datetime
from io import StringIO
from os.path import basename

from colander import Mapping, Sequence
from pyramid.renderers import json_renderer_factory

from ines import DEFAULT_RENDERERS
from ines.convert import camelcase, encode_and_decode, maybe_string, to_string
from ines.exceptions import Error
from ines.i18n import _


DATE = datetime.date
DATETIME = datetime.datetime


# Compact JSON response
json_renderer_factory.kw['separators'] = (',', ':')


class CSV(object):
    sequence_count = defaultdict(int)

    def lookup_header(self, node, values):
        if isinstance(node.typ, Sequence):
            count = set(len(getattr(value, node.name)) for value in values)
            node_max = self.sequence_count[node.name] = max(count)

            row = []
            first_row = self.lookup_header(node.children[0], values)
            row.extend(first_row)

            for i in range(1, node_max):
                row.extend('%s (%s)' % (title, i) for title in first_row)

            return row

        elif isinstance(node.typ, Mapping):
            header = []
            for child in node.children:
                header.extend(self.lookup_header(child, values))
            return header

        else:
            return [node.title]

    def lookup_row(self, request, node, value):
        if isinstance(node.typ, Sequence):
            row = []
            value_length = len(value)

            for i in range(self.sequence_count[node.name]):
                if i >= value_length:
                    child_value = None
                else:
                    child_value = value[i]

                for child in node.children[0].children:
                    if child_value is None:
                        row.extend(self.lookup_row(request, child, None))
                    elif isinstance(value, dict):
                        row.extend(self.lookup_row(request, child, child_value.get(camelcase(node.name))))
                    else:
                        row.extend(self.lookup_row(request, child, getattr(child_value, child.name, None)))

            return row

        elif isinstance(node.typ, Mapping):
            row = []
            for child in node.children:
                if isinstance(value, dict):
                    row.extend(self.lookup_row(request, child, value.get(camelcase(node.name))))
                else:
                    row.extend(self.lookup_row(request, child, getattr(value, child.name, None)))
            return row

        else:
            if value is not None and hasattr(node, 'value_decoder'):
                value = node.value_decoder(request, value)

            return [value]

    def lookup_rows(self, request, node, values):
        rows = [self.lookup_header(node, values)]
        for value in values:
            rows.append(self.lookup_row(request, node, value))
        return rows

    def build_with_schema(self, request, schema, values):
        pass

    def __call__(self, info):
        def _render(value, system):
            request = system.get('request')

            output_schema = None
            if isinstance(value, dict):
                output_schema = value['schema']
                value = value['value']

            delimiter = ';'
            quote_char = '"'
            line_terminator = '\r\n'
            quoting = QUOTE_ALL
            encoder = None

            if request is not None:
                response = request.response
                ct = response.content_type
                if ct == response.default_content_type:
                    response.content_type = 'text/csv'

                output = request.registry.config.lookup_output_schema(
                    request.matched_route.name,
                    request_method=request.method)
                if output:
                    output_schema = output[0].schema

                if output_schema:
                    value = self.lookup_rows(request, output_schema.children[0], value)

                    output_filename = getattr(output_schema, 'filename', None)
                    if output_filename:
                        if callable(output_filename):
                            output_filename = output_filename()
                        response.content_disposition = 'attachment; filename="%s"' % output_filename

                _get_param = request.params.get
                get_param = lambda k: _get_param(k) or _get_param(camelcase(k))

                csv_delimiter = get_param('csv_delimiter')
                if csv_delimiter:
                    delimiter = to_string(csv_delimiter)
                    if delimiter == '\\t':
                        delimiter = '\t'
                    else:
                        delimiter = delimiter[0]

                csv_quote_char = get_param('csv_quote_char')
                if csv_quote_char:
                    quote_char = to_string(csv_quote_char)

                csv_line_terminator = get_param('csv_line_terminator')
                if csv_line_terminator:
                    if csv_line_terminator == '\\n\\r':
                        line_terminator = '\n\r'
                    elif csv_line_terminator == '\\n':
                        line_terminator = '\n'
                    elif csv_line_terminator == '\\r':
                        line_terminator = '\r'

                csv_encoding = get_param('csv_encoding')
                if csv_encoding:
                    try:
                        encoder = codecs.lookup(csv_encoding)
                    except LookupError:
                        raise Error('csv_encoding', _('Invalid CSV encoding'))
                    else:
                        if encoder.name != 'utf-8':
                            request.response.charset = encoder.name

                yes_text = request.translate(_('Yes'))
                no_text = request.translate(_('No'))

                csv_quoting = get_param('csv_quoting')
                if csv_quoting:
                    csv_quoting = to_string(csv_quoting).lower()
                    if csv_quoting == 'minimal':
                        quoting = QUOTE_MINIMAL
                    elif csv_quoting == 'non_numeric':
                        quoting = QUOTE_NONNUMERIC
                    elif csv_quoting == 'none':
                        quoting = QUOTE_NONE

            else:
                yes_text = 'Yes'
                no_text = 'No'

            if not value:
                return ''

            f = StringIO()
            csvfile = csv_writer(
                f,
                delimiter=delimiter,
                quotechar=quote_char,
                lineterminator=line_terminator,
                quoting=quoting)

            for value_items in value:
                row = []
                for item in value_items:
                    if item is None:
                        item = ''
                    elif not isinstance(item, str):
                        if isinstance(item, bool):
                            item = item and yes_text or no_text
                        elif isinstance(item, (float, int)):
                            item = str(item)
                        elif isinstance(item, (DATE, DATETIME)):
                            item = item.isoformat()
                        elif not isinstance(item, str):
                            item = to_string(item)
                    row.append(item)
                csvfile.writerow(row)

            f.seek(0)
            response = f.read()
            f.close()

            if encoder:
                response = encoder.decode(encoder.encode(response)[0])[0]
            else:
                response = to_string(response)

            return response

        return _render


csv_renderer_factory = CSV()  # bw compat
DEFAULT_RENDERERS['csv'] = csv_renderer_factory


class File(object):
    def __call__(self, info):
        def _render(value, system):
            f = value['file']
            response = system['request'].response
            response.app_iter = f
            response.content_length = int(value['content_length'])
            response.status = 200
            response.content_type = maybe_string(value.get('content_type'))

            filename = value.get('filename') or basename(f.name)
            if value.get('is_attachment'):
                content_disposition = 'attachment; filename="%s"' % filename
            else:
                content_disposition = 'inline; filename="%s"' % filename

            response.content_disposition = encode_and_decode(content_disposition, 'latin-1', 'ignore')

            # Add others headers
            add_header = response._headerlist.append
            add_header(('Status-Code', response.status))

            return None
        return _render


file_renderer_factory = File()  # bw compat
DEFAULT_RENDERERS['file'] = file_renderer_factory


def html_renderer_factory(info):
    def _render(value, system):
        value = to_string(value)
        request = system.get('request')
        if request is not None:
            response = request.response
            ct = response.content_type
            if ct == response.default_content_type:
                response.content_type = 'text/html'
        return value
    return _render


DEFAULT_RENDERERS['html'] = html_renderer_factory
