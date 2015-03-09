# -*- coding: utf-8 -*-

from os import getpid

from paste.deploy.loadwsgi import loadapp
from paste.urlmap import parse_path_expression
from paste.urlmap import URLMap
from pyramid.httpexceptions import HTTPNotFound
from pyramid.paster import get_app
from pyramid.paster import get_appsettings
from webob.response import Response

from ines.convert import maybe_integer
from ines.system import start_system_thread
from ines.utils import file_modified_time
from ines.utils import format_error_to_json


def not_found_api_application(global_settings, **settings):
    def call_not_found(environ, start_response):
        not_found = HTTPNotFound()
        response = Response(
            body=format_error_to_json(not_found),
            status=not_found.code,
            content_type='application/json')
        return response(environ, start_response)
    return call_not_found


def onthefly_url_map_factory(loader, global_settings, **settings):
    ini_path = global_settings['__file__']
    return OnTheFlyAPI(ini_path)


class OnTheFlyAPI(URLMap):
    def __init__(self, config_path):
        self.config_path = config_path
        self.applications = []
        self.validate_config_seconds = 15

        self.start_applications()

        # Start thread for config ini validation
        self.last_update_time = file_modified_time(self.config_path)
        self.validate_config_update()

    def validate_config_update(self):
        def validator():
            last_update = file_modified_time(self.config_path)
            if self.last_update_time < last_update:
                self.last_update_time = last_update
                self.start_applications(debug=True)

            # Return sleep seconds
            return abs(self.validate_config_seconds or 15)

        start_system_thread('config_ini_update_validator', validator)

    def start_applications(self, debug=False):
        settings = get_appsettings(self.config_path)
        not_found_application = settings.local_conf.pop(
            'not_found_application',
            settings.global_conf.get('not_found_application'))
        if not_found_application:
            not_found_application = loadapp(
                not_found_application,
                global_conf=settings.global_conf)
        else:
            not_found_application = not_found_api_application(
                settings.global_conf, **settings.local_conf)
        self.not_found_application = not_found_application

        self.validate_config_seconds = maybe_integer(
            settings.local_conf.pop('validate_config_seconds', None))

        for path, app_name in settings.local_conf.items():
            path = parse_path_expression(path)
            self[path] = get_app(self.config_path, app_name)

            if debug:
                print (
                    'Application %s reloaded on pid %s'
                    % (app_name, getpid()))
