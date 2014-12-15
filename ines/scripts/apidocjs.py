# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
#
# @author Hugo Branquinho <hugobranq@gmail.com>

import optparse
import os
from pkg_resources import find_distributions
from pkg_resources import resource_filename
from subprocess import Popen
import sys


def main(argv=sys.argv):
    return APIDocJSCommand().run(argv)


class APIDocJSCommand(object):
    description = 'Compile API Doc JS documentation for our projects'
    usage = 'usage: %prog project_names [options]'
    parser = optparse.OptionParser(usage, description=description)

    parser.add_option('-o', '--output',
                      dest='output_name',
                      help=('Output dirname. Folder name for the generated '
                            'documentation.'))

    parser.add_option('-f', '--file-filters',
                      dest='filters',
                      action='append',
                      help=('RegEx-Filter to select files that should be ' 
                            'parsed (many -f can be used). Default .cs .dart '
                            '.erl .go .java .js .php .py .rb .ts.'))

    parser.add_option('-t', '--template',
                      dest='template',
                      help=('Use template for output files. You can create '
                            'and use your own template.'))

    def run(self, argv):
        options, args = self.parser.parse_args(argv[1:])

        if not args:
            print 'You must provide at least one project_name'
            return 0

        packages_names = set()
        for maybe_path in args:
            path = os.path.join(os.getcwd(), maybe_path)
            if os.path.isdir(path):
                distribution = [x for x in find_distributions(path, only=True)]
                if len(distribution) != 1:
                    raise ValueError('Package not found in %s' % maybe_path)
                distribution = distribution[0]
                packages_names.add(distribution.project_name.replace('-', '_'))

            else:
                packages_names.add(maybe_path)

        for package_name in packages_names:
            input_path = resource_filename(package_name, '')

            output_name = options.output_name or 'apidocjs'
            output_path = resource_filename(package_name, output_name)

            cmds = ['apidoc', '-i', input_path, '-o', output_path]

            if options.filters:
                for f in options.filters:
                    cmds.append('-f')
                    cmds.append('"%s"' % f)

            if options.template:
                cmds.append('-t')
                cmds.append(options.template)

            print 'Creating apidoc for', package_name, 'on', input_path
            p = Popen(cmds)
            p.wait()

        return 0


if __name__ == '__main__': # pragma: no cover
    sys.exit(main() or 0)
