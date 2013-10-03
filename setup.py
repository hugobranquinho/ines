__version__ = '0.1'

import os
import sys
from setuptools import find_packages
from setuptools import setup


here = os.path.abspath(os.path.dirname(__file__))
try:
    README = open(os.path.join(here, 'README')).read()
    CHANGES = open(os.path.join(here, 'CHANGES')).read()
except IOError:
    README = CHANGES = ''

requires = ['setuptools',
            'pyramid >= 1.0',
            'lingua >= 1.3',
            'Babel >= 0.9.6',
            'WebOb >= 1.2b3',
            'translationstring >= 0.4']

python_version = sys.version_info[:2]
if python_version <= (2, 6):
    requires.extend([
        'importlib'])

tests_require = ['WebTest',
                 'zope.component']

testing_extras = tests_require + ['nose',
                                  'coverage']

setup(name='ines',
      version=__version__,
      description='Web applications manager for pyramid packages',
      long_description=README + '\n\n' + CHANGES,
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Intended Audience :: Developers',
          'Natural Language :: English',
          'Framework :: Pyramid',
          'Operating System :: MacOS :: MacOS X',
          'Operating System :: Unix',
          'Programming Language :: Python :: 2.6',
          "Topic :: Internet :: WWW/HTTP",
          "Topic :: Internet :: WWW/HTTP :: WSGI",
          'Topic :: Utilities'],
      keywords='web wsgi pylons pyramid utilities core',
      author='Hugo Branquinho',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      install_requires=requires,
      tests_require=tests_require,
      test_suite='ines.tests',
      extras_require={'testing': testing_extras},
      message_extractors={'ines': [('**.py', 'lingua_python', None),
                                   ('**.pt', 'lingua_xml', None)]})
