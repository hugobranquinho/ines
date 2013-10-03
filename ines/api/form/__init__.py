# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
# 
# @author Hugo Branquinho <hugobranq@gmail.com>
# 
# $Id$

from pkg_resources import resource_filename

from colander import null
from deform import Button
from deform import Form
from deform import ValidationFailure
from deform import ZPTRendererFactory

from ines.api import BaseClass
from ines.api import BaseSession
from ines.api.form.schema import FormStructure
from ines.api.form.utils import button_in_keys
from ines.api.form.utils import clean_appstruct
from ines.api.form.utils import FormResponse
from ines.path import maybe_resource_dir
from ines.renderers import render_binary_response
from ines.utils import cache_property


# Define ines deform templates
DEFORM_TEMPLATES = [resource_filename('ines', 'templates/deform/'),
                    resource_filename('deform', 'templates/')]


class BaseFormClass(BaseClass):
    def __init__(self, config, session, package_name):
        BaseClass.__init__(self, config, session, package_name)

        # Add required translation dirs
        self.config.add_translation_dirs(
            'deform:locale/',
            'colander:locale/')

        # Required options
        form_error_class = self.settings.get('form_error_class') or 'error'

        deform_templates = []
        templates = self.settings.get('deform_templates')
        if templates:
            if not isinstance(templates, (list, tuple)):
                templates = templates.split()

            for template in templates:
                template = maybe_resource_dir(template)
                if not template:
                    message = 'Invalid deform template dir "%s"' % template
                    raise IOError(message)
                else:
                    deform_templates.append(template)

        self.settings.update({
            'form_error_class': form_error_class,
            'deform_templates': deform_templates})


class BaseFormSession(BaseSession):
    _base_class = BaseFormClass

    @cache_property
    def structure(self):
        return FormStructure(self, self.request)

    @property
    def deform_templates(self):
        templates = list(self.settings['deform_templates'])
        templates.extend(DEFORM_TEMPLATES)
        return tuple(templates)

    def create(self,
               schema,
               appstruct=null,
               title=None,
               onclick=None,
               form_submit_name='submit',
               buttons=None,
               return_none_values=True,
               action='',
               method='POST',
               readonly=False,
               autocomplete='off',
               ajax_response=None,
               ajax_options='{}'):

        all_buttons = []
        if title:
            button = Button(form_submit_name, title)
            if onclick is not None:
                button.onclick = onclick
            all_buttons.append(button)

        if buttons:
            all_buttons.extend(buttons)

        use_ajax = bool(ajax_response)
        form = Form(schema,
                    buttons=all_buttons,
                    action=action,
                    autocomplete=autocomplete,
                    method=method,
                    use_ajax=use_ajax,
                    ajax_options=ajax_options,
                    renderer=ZPTRendererFactory(
                                 self.deform_templates,
                                 translator=self.translator))

        html = None
        response = None
        captured = None
        have_errors = False

        arguments = getattr(self.request, method.upper())
        if all_buttons and button_in_keys(all_buttons, arguments.keys()):
            controls = arguments.items()

            try:
                captured = form.validate(controls)
                if ajax_response:
                    response = ajax_response()

                if response is None:
                    html = form.render(captured)

            except ValidationFailure, e:
                html = e.render()
                have_errors = True

        else:
            appstruct = clean_appstruct(appstruct)
            html = form.render(appstruct, readonly=readonly)

        if use_ajax and self.request.is_xhr and not response:
            response = render_binary_response(html)

        return FormResponse(html,
                            captured,
                            have_errors,
                            response,
                            return_none_values)
