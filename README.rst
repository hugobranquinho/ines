ines
====

An extensible package made with pyramid (see http://www.pylonsproject.org/)
to relate multiple applications.


Example
=======

::

    from ines.api import BaseAPISession
    # Register APP one internal API
    class OneSession(BaseAPISession):
        __app_name__ = 'app_one'  # This is required when you have multiple app in one package
        def some_method(self):
            return u'Method of app ONE called'
    # Register two APP internal API
    class TwoSession(BaseAPISession):
        __app_name__ = 'app_two'
        def some_method(self):
            return u'Method of app TWO called'

    from ines.config import APIConfigurator
    # Start APPS config
    config_one = APIConfigurator(
        application_name='app_one',
        settings={'api.extension.session_path': OneSession})
    config_two = APIConfigurator(
        application_name='app_two',
        settings={'api.extension.session_path': TwoSession})

    from ines.view import api_config
    # Add route / view for app one
    config_one.add_routes(('one_app_home', ''))
    @api_config(route_name='one_app_home')
    def one_app_home(context, request):
        # Here we call the two APP method
        return {
            'two_app': request.api.applications.app_two.some_method(),
            'one_app': request.api.applications.app_one.some_method(),
            'one_app_direct': request.api.some_method()}
    # Add route / view for app two
    config_two.add_routes(('one_app_home', ''))
    @api_config(route_name='one_app_home')
    def one_app_home(context, request):
        # Here we call the two APP method
        return {
            'one_app': request.api.applications.app_one.some_method(),
            'two_app': request.api.applications.app_two.some_method(),
            'two_app_direct': request.api.some_method()}

    # Create pyramid wsgi middleware and start server
    from wsgiref.simple_server import make_server
    from paste.urlmap import URLMap
    from ines.wsgi import not_found_api_application
    urlmap = URLMap(not_found_app=not_found_api_application({}))
    urlmap['/one'] = config_one.make_wsgi_app()
    urlmap['/two'] = config_two.make_wsgi_app()
    server = make_server('0.0.0.0', 6543, urlmap)
    server.serve_forever()

