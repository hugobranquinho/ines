# -*- coding: utf-8 -*-

from ines.middlewares import Middleware
from ines.path import get_object_on_path


class RepozeTMMiddleware(Middleware):
    name = 'repoze.tm'

    def __init__(self, config, application, **settings):
        super(RepozeTMMiddleware, self).__init__(config, application, **settings)

        commit_veto = settings.get('commit_veto')
        if commit_veto:
            commit_veto = get_object_on_path(commit_veto)

        from repoze.tm import TM
        self.repozetm = TM(self.application, commit_veto=commit_veto)

    def __call__(self, environ, start_response):
        return self.repozetm(environ, start_response)
