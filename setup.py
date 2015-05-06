###############################################################################
#
# The MIT License (MIT)
#
# Copyright (c) 2014-2015 Hugo Branquinho
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
###############################################################################

__version__ = '0.1a1'

import os
from setuptools import find_packages
from setuptools import setup


here = os.path.abspath(os.path.dirname(__file__))
try:
    with open(os.path.join(here, 'README')) as f:
        README = f.read()
    with open(os.path.join(here, 'CHANGES')) as f:
        CHANGES = f.read()
except IOError:
    README = CHANGES = ''

requires = [
    'setuptools',
    'pyramid',
    'Babel',
    'translationstring',
    'zope.interface',
    'Paste',
    'WebOb',
    'PasteDeploy',
    'colander >= 1.0',
    'transaction',
    'SQLAlchemy',
    'zope.sqlalchemy',
    'repoze.tm2']

setupkw = dict(
    name='ines',
    version=__version__,
    description='Web applications manager for pyramid packages',
    long_description=README + '\n\n' + CHANGES,
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Framework :: Pyramid',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Unix',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'License :: OSI Approved :: MIT License',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Internet :: WWW/HTTP :: WSGI',
        'Topic :: Utilities'],
    keywords='web wsgi pylons pyramid utilities core',
    author='Hugo Branquinho',
    author_email='hugobranq@gmail.com',
    url='https://github.com/hugobranq/ines',
    download_url='https://github.com/hugobranq/ines/tarball/0.1',
    license='MIT license',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=requires,
    entry_points="""
        [console_scripts]
        apidocjs = ines.scripts.apidocjs:main

        [paste.app_factory]
        not_found_api_application = ines.wsgi:not_found_api_application

        [paste.composite_factory]
        onthefly_url_map = ines.wsgi:onthefly_url_map_factory""")

try:
    import babel
    babel = babel  # PyFlakes
    setupkw['message_extractors'] = {
        'ines': [('**.py', 'lingua_python', None),
                 ('**.pt', 'lingua_xml', None)]}
except ImportError:
    pass

setup(**setupkw)
