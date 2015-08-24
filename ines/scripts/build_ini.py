# -*- coding: utf-8 -*-

import optparse
import os
import sys

from six import moves
from six import print_


def main(argv=sys.argv):
    return BuildINICommand().run(argv)


class BuildINICommand(object):
    description = 'Compile INI files for multiple servers'
    usage = 'usage: %prog configuration_file'
    parser = optparse.OptionParser(usage, description=description)

    def run(self, argv):
        options, args = self.parser.parse_args(argv[1:])
        if not args:
            print_('You must provide at least one config file')
            return 0

        HERE = os.getcwd()
        settings = {'directory': HERE}

        for configuration_path in args:
            if not configuration_path.startswith(os.sep):
                configuration_path = os.path.join(HERE, configuration_path)
            if not os.path.isfile(configuration_path):
                print_('Invalid configuration path `%s`' % configuration_path)
                return 0

            config = moves.configparser.RawConfigParser(allow_no_value=True)
            config.read(configuration_path)

            # Get and update DEFAULT options
            config_settings = settings.copy()
            for key, value in config._defaults.items():
                config_settings[key] = value.format(**config_settings)

            for output_path, section_options in config._sections.items():
                section_settings = config_settings.copy()
                for key, value in section_options.items():
                    section_settings[key] = value.format(**section_settings)

                # Find input INI file(s)
                template_inputs = section_settings.get('template_input')
                if not template_inputs:
                    print_((
                        '`template_input` option required in section `DEFAULT` or `%s` of `%s`')
                        % (output_path, configuration_path))
                    return 0

                # Extend input template(s)
                template = moves.configparser.RawConfigParser(allow_no_value=True)
                for input_template_path in template_inputs.split(','):
                    if not input_template_path.startswith(os.sep):
                        input_template_path = os.path.join(HERE, input_template_path)
                    if not os.path.isfile(input_template_path):
                        print_('Invalid input template path `%s`' % input_template_path)
                        return 0
                    template.read(input_template_path)

                # Format template options
                sections = [(template.default_section, template._defaults)]
                sections.extend(s for s in template._sections.items())
                for section, values in sections:
                    for key, value in values.items():
                        new_value = value.format(**section_settings)
                        if value != new_value:
                            template.set(section, key, new_value)

                # Save template(s)
                if not output_path.startswith(os.sep):
                    output_path = os.path.join(HERE, output_path)
                with open(output_path, 'w') as f:
                    template.write(f)

        return 0


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main() or 0)
