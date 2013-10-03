# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
# 
# @author Hugo Branquinho <hugobranq@gmail.com>
# 
# $Id$

from os.path import isfile
from types import FunctionType
from urllib import quote

from pyramid.exceptions import NotFound
from webob import Response

from ines.cleaner import clean_filename
from ines.convert import force_string
from ines.renderers.utils import pop_renderer_argument


class BinaryRenderer(object):
    """ Class to construct :app:`Pyramid` binary renderer. This is the
    renderers base.


    Arguments

        ``encoding``

            Encode to convert ``filename`` and binary to string if they
            aren't sent as unicode. If ``encoding`` don't exists a
            ``LookupError`` error is raised.
            Default is ``utf-8``.

        ``encoding_error``

            Errors treatment type.
            If can't convert some characters to unicode and is defined:

                'strict' - Is raised a :class:`UnicodeDecodeError`;
                'replace' - Unknown characters are replaced with '?';
                'ignore' - Unknown characters are ignored.

            Default is ``strict``.

        ``filename``

            Define default filename for ``Content-Disposition`` header
            response.

        ``filename_extension``

            Define default ``filename`` extension. Used to force filename
            extension.

        ``is_attachment``

            Define if response is an attachment. Default is ``False``.

        ``content_type``

            Default content type to define in response on header
            ``Content-Type``.

        ``status``

            Default response status.

        ``no_cache``

            Define ``Cache-Control: no-cache`` in response header if ``True``.
            Default is ``False``.
    """
    encoding = 'utf-8'
    encoding_key = 'encoding'

    encoding_error = 'strict'
    encoding_error_key = 'encoding_error'

    filename = None
    filename_key = 'filename'

    filename_extension = None
    filename_extension_key = 'filename_extension'

    is_attachment = False
    is_attachment_key = 'is_attachment'

    content_type = None
    content_type_key = 'content_type'

    no_cache = False
    no_cache_key = 'no_cache'

    status = 200

    def __init__(self, **kwargs):
        # Default response headers
        self.default_headers = kwargs.pop('default_headers', None)

        # All request configurable keys
        self._configurable_keys = set()

        for key, value in kwargs.items():
            setattr(self, key, value)

    def add_configurable_keys(self, *keys):
        self._configurable_keys.update(keys)

    def construct_extension(self, filename, filename_extension=None):
        """ Add default filename extension, if defined.


        Return unicode ``filename`` with default extension if not defined.


        Arguments

            ``filename``

                Unicode filename.

            ``filename_extension``

                Unicode filename extension to had to filename if not defined.
                Sent an extension without dot (.).
        """
        extension = filename_extension or self.filename_extension
        if not extension:
            return filename

        # Check filename extension
        if filename.endswith(u'.%s' % extension):
            return filename
        else:
            return u'%s.%s' % (filename, extension)

    def construct_filename(self, filename=None, filename_extension=None):
        """ Construct filename to add in ``Content-Disposition`` header and add
        filename extension.


        Return string ``filename`` or default filename with default extension
        if defined.


        Arguments

            ``filename``

                Unicode filename. If not ``filename`` and not default filename
                ``None`` is sent.

            ``filename_extension``

                Unicode filename extension to had to filename if not defined.
                Sent an extension without dot (.).
        """
        filename = filename or self.filename
        if filename:
            if isinstance(filename, FunctionType):
                filename = unicode(filename())

            filename = self.construct_extension(filename, filename_extension)
            filename = clean_filename(filename)
            return quote(filename)

    def _render(self, binary, **kwargs):
        """ Construct binary response to use as :app:`Pyramid` response.


        Return a :class:`webob.Response` with ``binary``.


        Arguments

            ``binary``

                String to add as response body.

            ``kwargs``

                Arguments to define response configurations. See
                :class:`ines.renderers.BinaryRenderer` attributes for
                more informations.
        """
        # Response configuration
        arguments = {'status': int(kwargs.pop('status', self.status) or 200)}

        encoding = pop_renderer_argument(self, 'encoding', kwargs)
        if encoding is not None:
            encoding = force_string(encoding)
            arguments['charset'] = encoding

        encoding_error = pop_renderer_argument(self, 'encoding_error', kwargs)

        # Add content type to response if defined
        content_type = pop_renderer_argument(self, 'content_type', kwargs)
        if content_type:
            arguments['content_type'] = force_string(content_type)

        # Add content disposition to response if filename is defined
        filename = pop_renderer_argument(self, 'filename', kwargs)
        extension = pop_renderer_argument(self, 'filename_extension', kwargs)
        filename = self.construct_filename(filename, extension)
        if filename:
            if pop_renderer_argument(self, 'is_attachment', kwargs):
                content_disposition = 'attachment; filename="%s"' % filename
            else:
                content_disposition = 'inline; filename="%s"' % filename
            arguments['content_disposition'] = content_disposition

        # Create response
        binary = force_string(binary, errors=encoding_error)
        response = Response(body=binary, **arguments)

        # Add others headers
        add_header = response._headerlist.append
        add_header(('Connection', 'close'))
        add_header(('Status', response.status))

        # Define default headers
        if self.default_headers:
            for key, value in dict(self.default_headers).items():
                if not response.headers.has_key(key):
                    add_header((key, value))

        # Define response cache
        if pop_renderer_argument(self, 'no_cache', kwargs):
            add_header(('Cache-Control', 'no-cache'))

        return response

    def render(self, binary, **kwargs):
        """ Construct binary response to use as :app:`Pyramid` response.
        See :meth:`ines.renderers.BinaryRenderer._render` for more
        information.
        """
        return self._render(binary, **kwargs)

    def set_request_options(self, request):
        for key in self._configurable_keys:
            request_key = getattr(self, key)
            value = request.get_value(request_key)
            if value:
                setattr(self, key, value)


def render_binary_response(binary, **kwargs):
    """ Construct binary response to use as :app:`Pyramid` response.
    See :meth:`ines.renderers.BinaryRenderer._render` for more
    information.
    """
    renderer = BinaryRenderer(**kwargs)
    return renderer.render(binary)


def render_file_response(path, **kwargs):
    """ Construct file path response to use as :app:`Pyramid` response.


    Return a :class:`webob.Response` with open ``path``.
    If a invalid ``path`` is sent :class:`pyramid.exceptions.NotFound` is
    raised.


    Arguments

        ``path``

            Full path to file.

        ``kwargs``

            Arguments to define response configurations. See
            :class:`ines.renderers.BinaryRenderer` attributes for
            more informations.
    """
    if not isfile(path):
        raise NotFound()
    else:
        renderer = BinaryRenderer(**kwargs)

        # Open file
        open_file = open(path, 'rb')
        binary = open_file.read()
        open_file.close()

        return renderer.render(binary)
