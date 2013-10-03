__version__ = '0.1'

import os
import sys
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

requires = ['setuptools',
            'pyramid >= 1.0',
            'Babel >= 0.9.6',
            'WebOb >= 1.2b3',
            'translationstring >= 0.4']

python_version = sys.version_info[:2]
if python_version <= (2, 6):
    requires.extend(['importlib'])

tests_require = ['WebTest',
                 'zope.component']

testing_extras = tests_require + ['nose',
                                  'coverage']

setupkw = dict(
    name='ines',
    version=__version__,
    description='Web applications manager for pyramid packages',
    long_description=README + '\n\n' + CHANGES,
    classifiers=['Development Status :: 1 - Planning',
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
    tests_require=tests_require,
    test_suite='ines.tests',
    extras_require={'testing': testing_extras})


try:
    import babel
    babel = babel # PyFlakes
    setupkw['message_extractors'] = {
        'ines': [('**.py', 'lingua_python', None),
                 ('**.pt', 'lingua_xml', None)]}
except ImportError:
    pass

setup(**setupkw)
