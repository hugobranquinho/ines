# -*- coding: utf-8 -*-

from collections import defaultdict
from json import loads
import ssl
from socket import create_connection

from six import string_types
from six import moves
from six import u
from webob.multidict import MultiDict

from ines.convert import to_bytes
from ines.convert import to_string
from ines.convert import to_unicode
from ines.convert import guess_datetime
from ines.convert import maybe_integer
from ines.convert import maybe_unicode
from ines.convert import string_join
from ines.exceptions import Error


urlencode = moves.urllib.parse.urlencode
build_opener = moves.urllib.request.build_opener
HTTPCookieProcessor = moves.urllib.request.HTTPCookieProcessor
HTTPRedirectHandler = moves.urllib.request.HTTPRedirectHandler
HTTPSHandler = moves.urllib.request.HTTPSHandler
Request = moves.urllib.request.Request
HTTPError = moves.urllib.error.HTTPError
HTTPConnection = moves.http_client.HTTPConnection
HTTPSConnection = moves.http_client.HTTPSConnection
SimpleCookie = moves.http_cookies.SimpleCookie

PROTOCOLS = [
    getattr(ssl, 'PROTOCOL_%s' % k)
    for k in ['TLSv1_2', 'TLSv1_1', 'TLSv1', 'SSLv23', 'SSLv3', 'SSLv2']
    if hasattr(ssl, 'PROTOCOL_%s' % k)]


FOUND_SSL_PROTOCOLS = {}
TRY_SSL_PROTOCOLS = defaultdict(lambda: list(PROTOCOLS))


class inesHTTPSConnection(HTTPSConnection):
    """Based on the recipe http://code.activestate.com/recipes/
    577548-https-httplib-client-connection-with-certificate-v
    """
    sock = None

    def _create_socket(self):
        sock = create_connection((self.host, self.port), self.timeout)
        if hasattr(self, '_tunnel_host') and self._tunnel_host:
            self._tunnel()
        return sock

    def connect(self):
        ssl_version = FOUND_SSL_PROTOCOLS.get(self.host)
        if ssl_version is not None:
            self.sock = ssl.wrap_socket(
                self._create_socket(),
                self.key_file,
                self.cert_file,
                ssl_version=ssl_version)

        else:
            while True:
                to_try = TRY_SSL_PROTOCOLS[self.host]
                if not to_try:
                    TRY_SSL_PROTOCOLS.pop(self.host, None)
                    message = u('Could not open ssl url: %s') % self.host
                    raise Error('url', message)
                ssl_version = to_try.pop(0)

                try:
                    self.sock = ssl.wrap_socket(
                        self._create_socket(),
                        self.key_file,
                        self.cert_file,
                        ssl_version=ssl_version)
                except ssl.SSLError:
                    continue

                FOUND_SSL_PROTOCOLS[self.host] = ssl_version
                break


class inesHTTPSHandler(HTTPSHandler):
    def https_open(self, request):
        return self.do_open(inesHTTPSConnection, request)


class inesHTTPError(HTTPError):
    def __init__(self, request, fp, code, message, headers):
        error_message = '(%s) %s' % (code, message)
        HTTPError.__init__(self, fp.geturl(), code, error_message, headers, fp)
        self.request = request
        self.message = error_message

    def __repr__(self):
        return 'HTTPError: %s' % self.message


class inesHTTPRedirectHandler(HTTPRedirectHandler):
    def http_error_302(self, request, fp, code, message, headers):
        cookie = SimpleCookie()

        request_cookie = request.headers.get('Cookie')
        if request_cookie:
            cookie.load(request_cookie)

        set_cookie = headers.get('set-cookie')
        if set_cookie:
            for value in set_cookie:
                cookie.load(value)

        headers['Cookie'] = cookie.output(header='', sep='; ')

        redirect_handler = HTTPRedirectHandler.http_error_302(self, request, fp, code, message, headers)
        return inesHTTPError(request, redirect_handler, code, message, headers)

    http_error_301 = http_error_303 = http_error_307 = http_error_302


# install opener for SSL fix
# install redirect handler
URL_OPENER = build_opener(
    inesHTTPSHandler(),
    inesHTTPRedirectHandler,
    HTTPCookieProcessor())


def parse_request_type(content_type):
    if content_type is None:
        return 'text/plain'

    content_type = to_string(content_type)
    if ';' in content_type:
        content_type = content_type.split(';', 1)[0]

    return string_join('/', (c.strip().lower() for c in content_type.split('/')))


def open_url(url, data=None, timeout=None, headers=None, method='get'):
    if timeout:
        timeout = abs(float(timeout))

    url = to_string(url)
    if ' ' in url:
        url = url.replace(' ', '%20')

    if data:
        if isinstance(data, string_types):
            data = to_string(data)
        else:
            if isinstance(data, (dict, MultiDict)):
                data = data.items()
            data = dict((to_string(k), to_string(v)) for k, v in data)
            data = urlencode(data)

        if method.lower() == 'get':
            url += '?%s' % data
            data = None
        else:
            data = to_bytes(data)

    req = Request(url, data=data, headers=headers or {})
    req.get_method = lambda: method.upper()

    try:
        response = URL_OPENER.open(req, timeout=timeout)
    except Exception as error:
        message = u('Could not open the url: %s') % to_unicode(url)
        raise Error('url', message, exception=error)

    if isinstance(response, inesHTTPError):
        raise Error('url', response.message, exception=response)
    else:
        return response


def get_url_headers(*args, **kwargs):
    kwargs['method'] = 'head'
    response = open_url(*args, **kwargs)
    response.close()
    return response.headers


def get_url_info(*args, **kwargs):
    headers = get_url_headers(*args, **kwargs)
    return {
        'size': maybe_integer(headers.get('Content-Length')) or 0,
        'type': maybe_unicode(headers.get('Content-Type')),
        'updated': guess_datetime(headers.get('Last-Modified'))}


def get_url_file(*args, **kwargs):
    response = open_url(*args, **kwargs)
    result = response.read()
    response.close()
    return result


def get_url_body(*args, **kwargs):
    return to_unicode(get_url_file(*args, **kwargs))


def open_json_url(*args, **kwargs):
    encoding = kwargs.pop('encoding', None)

    body = get_url_body(*args, **kwargs)
    if not body:
        return None

    if encoding is not None:
        body = body.decode(encoding)
        if encoding != 'utf-8':
            body = body.encode('utf-8').decode('utf-8')

    try:
        json_response = loads(body)
    except Exception as error:
        raise Error('url', u('Could not decode json response.'), exception=error)
    else:
        return json_response


def ping_url(protocol, url):
    url = to_string(url)
    protocol = to_string(protocol).lower()

    url_parts = url.split('/', 1)
    host = url_parts[0]
    if len(url_parts) == 1:
        path = '/'
    else:
        path = '/%s' % url_parts[1]

    if protocol == 'https':
        connection = HTTPSConnection(host)
    elif protocol == 'http':
        connection = HTTPConnection(host)
    else:
        raise ValueError('url', u('Invalid protocol %s. Use only http or https.') % protocol)

    valid_url = False
    try:
        connection.request('HEAD', path)
        response = connection.getresponse()
    except Exception:
        pass
    else:
        if response.status != 404:
            valid_url = True
    finally:
        connection.close()
        return valid_url
