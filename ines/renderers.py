# -*- coding: utf-8 -*-

import codecs
from csv import QUOTE_MINIMAL
from csv import writer as csv_writer
from cStringIO import StringIO
import datetime
from os.path import basename

from colander import Mapping
from colander import Sequence

from ines import _
from ines import DEFAULT_RENDERERS
from ines.convert import camelcase
from ines.convert import force_string
from ines.convert import force_unicode
from ines.convert import json_dumps
from ines.convert import maybe_string
from ines.exceptions import Error


DATE = datetime.date
DATETIME = datetime.datetime


class CSV(object):
    def lookup_header(self, node):
        if isinstance(node.typ, Sequence):
            return self.lookup_header(node.children[0])

        elif isinstance(node.typ, Mapping):
            header = []
            for child in node.children:
                header.extend(self.lookup_header(child))
            return header

        else:
            return [node.title]

    def lookup_row(self, node, value):
        if isinstance(node.typ, Sequence):
            return self.lookup_row(node.children[0], value)

        elif isinstance(node.typ, Mapping):
            row = []
            for child in node.children:
                if isinstance(value, dict):
                    row.extend(self.lookup_row(child, value.get(camelcase(node.name))))
                else:
                    row.extend(self.lookup_row(child, getattr(value, child.name, None)))
            return row

        else:
            return [value]

    def lookup_rows(self, node, values):
        rows = [self.lookup_header(node)]
        for value in values:
            rows.append(self.lookup_row(node, value))
        return rows

    def __call__(self, info):
        def _render(value, system):
            request = system.get('request')

            delimiter = ';'
            quotechar = '"'
            lineterminator = '\r\n'
            quoting = QUOTE_MINIMAL
            encode_string = force_string
            decode_string = force_unicode

            if request is not None:
                response = request.response
                ct = response.content_type
                if ct == response.default_content_type:
                    response.content_type = 'text/csv'

                output = request.registry.config.lookup_output_schema(
                    request.matched_route.name,
                    request_method=request.method)
                if output:
                    value = self.lookup_rows(output[0].schema.children[0], value)

                csv_delimiter = request.params.get(camelcase('csv_delimiter'))
                if csv_delimiter:
                    delimiter = encode_string(csv_delimiter)
                    if delimiter == '\\t':
                        delimiter = '\t'
                    else:
                        delimiter = delimiter[0]

                csv_quote_char = request.params.get(camelcase('csv_quote_char'))
                if csv_quote_char:
                    quotechar = encode_string(csv_quote_char)

                csv_line_terminator = request.params.get(camelcase('csv_line_terminator'))
                if csv_line_terminator:
                    if csv_line_terminator == u'\\n\\r':
                        lineterminator = '\n\r'
                    elif csv_line_terminator == u'\\n':
                        lineterminator = '\n'
                    elif csv_line_terminator == u'\\r':
                        lineterminator = '\r'

                csv_encoding = request.params.get(camelcase('csv_encoding'))
                if csv_encoding:
                    try:
                        encoder = codecs.lookup(csv_encoding)
                    except LookupError:
                        raise Error('csv_encoding', _(u'Invalid CSV encoding'))
                    else:
                        if encoder.name != 'utf-8':
                            encode_string = lambda v: encoder.encode(v)[0]
                            decode_string = lambda v: encoder.decode(v)[0]
                            request.response.charset = encoder.name

                yes_text = encode_string(request.translate(_(u'Yes')))
                no_text = encode_string(request.translate(_(u'No')))
            else:
                yes_text = 'Yes'
                no_text = 'No'

            if not value:
                return u''

            f = StringIO()
            csvfile = csv_writer(
                f,
                delimiter=delimiter,
                quotechar=quotechar,
                lineterminator=lineterminator,
                quoting=quoting)

            for value_items in value:
                row = []
                for item in value_items:
                    if item is None:
                        item = ''
                    elif isinstance(item, bool):
                        item = item and yes_text or no_text
                    elif isinstance(item, (int, basestring, long)):
                        item = encode_string(unicode(item))
                    elif isinstance(item, (DATE, DATETIME)):
                        item = encode_string(item.isoformat())
                    else:
                        item = encode_string(json_dumps(item) or '')
                    row.append(item)
                csvfile.writerow(row)

            f.seek(0)
            response = decode_string(f.read())
            f.close()
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
                response.content_disposition = 'attachment; filename="%s"' % filename
            else:
                response.content_disposition = 'inline; filename="%s"' % filename

            # Add others headers
            add_header = response._headerlist.append
            add_header(('Status-Code', response.status))
            add_header(('Accept-Ranges', 'bytes'))

            return None
        return _render


file_renderer_factory = File()  # bw compat
DEFAULT_RENDERERS['file'] = file_renderer_factory
