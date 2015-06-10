# -*- coding: utf-8 -*-

import mimetypes

from ines.convert import maybe_string


class GuessTypeWithData(object):
    def __init__(self):
        self.startswith_func = []
        self.special_func = []
        self.min_header_size = 0

    def add(self, required_size, func):
        required_size = int(required_size)
        for i, (s, f) in enumerate(self.special_func):
            if s < required_size:
                self.special_func.insert(i, (required_size, func))
                break
        else:
            self.special_func.append((required_size, func))

        first_size = self.special_func[0][0]
        if first_size > self.min_header_size:
            self.min_header_size = first_size

    def add_startswith(self, key, mimetype):
        size = len(key)
        for i, (s, k, m) in enumerate(self.startswith_func):
            if s < size:
                self.startswith_func.insert(i, (size, key, mimetype))
                break
        else:
            self.startswith_func.append((size, key, mimetype))

        first_size = self.startswith_func[0][0]
        if first_size > self.min_header_size:
            self.min_header_size = first_size

    def __call__(self, data):
        for size, func in self.special_func:
            mimetype = func(data[:size])
            if mimetype:
                return mimetype

        for required_size, key, mimetype in self.startswith_func:
            if data[:required_size].startswith(key):
                return mimetype


# TODO: http://www.freeformatter.com/mime-types-list.html
guess_mimetype_with_data = GuessTypeWithData()

guess_mimetype_with_data.add_startswith('<?xml', 'text/xml')
guess_mimetype_with_data.add_startswith('\xef\xbb\xbf<?xml', 'text/xml')
guess_mimetype_with_data.add_startswith('\0<\0?\0x\0m\0l', 'text/xml')
guess_mimetype_with_data.add_startswith('<\0?\0x\0m\0l\0', 'text/xml')
guess_mimetype_with_data.add_startswith('\xfe\xff\0<\0?\0x\0m\0l', 'text/xml')
guess_mimetype_with_data.add_startswith('\xff\xfe<\0?\0x\0m\0l\0', 'text/xml')
guess_mimetype_with_data.add_startswith('<html', 'text/html')
guess_mimetype_with_data.add_startswith('<HTML', 'text/html')


# Add image handlers
# See python lib imghdr
guess_mimetype_with_data.add_startswith(b'\211PNG\r\n\032\n', 'image/png')
guess_mimetype_with_data.add_startswith(b'GIF87a', 'image/gif')
guess_mimetype_with_data.add_startswith(b'GIF89a', 'image/gif')
guess_mimetype_with_data.add_startswith(b'MM', 'image/tiff')
guess_mimetype_with_data.add_startswith(b'II', 'image/tiff')
guess_mimetype_with_data.add_startswith(b'\001\332', 'image/rgb')
guess_mimetype_with_data.add_startswith(b'\x59\xA6\x6A\x95', 'image/rast')
guess_mimetype_with_data.add_startswith(b'#define ', 'image/xbm')
guess_mimetype_with_data.add_startswith(b'BM', 'image/bmp')
guess_mimetype_with_data.add_startswith(b'\x76\x2f\x31\x01', 'image/exr')


def lookup_jpeg(header):
    if header[6:] in (b'JFIF', b'Exif'):
        return 'image/jpeg'

guess_mimetype_with_data.add(10, lookup_jpeg)


def lookup_netpbm(header):
    if len(header) >= 3 and header[0] == ord(b'P') and header[2] in b' \t\n\r':
        if header[1] in b'14':
            return 'image/pbm'
        elif header[1] in b'25':
            return 'image/pgm'
        elif header[1] in b'36':
            return 'image/ppm'

guess_mimetype_with_data.add(3, lookup_netpbm)


def lookup_webp(header):
    if header.startswith(b'RIFF') and header[8:] == b'WEBP':
        return 'image/webp'

guess_mimetype_with_data.add(12, lookup_netpbm)


def find_mimetype(filename=None, header_or_file=None):
    if isinstance(header_or_file, file):
        header_or_file.seek(0)
        header = header_or_file.read(guess_mimetype_with_data.min_header_size)
    else:
        header = maybe_string(header_or_file)

    mimetype = None
    if header is not None:
        mimetype = guess_mimetype_with_data(header)

    if not mimetype:
        filename = maybe_string(filename)
        if filename:
            mimetype, encoding = mimetypes.guess_type(filename, strict=True)
            if not mimetype:
                mimetype, encoding = mimetypes.guess_type(filename, strict=False)

    return maybe_string(mimetype)
