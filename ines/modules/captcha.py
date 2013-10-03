# -*- coding: utf-8 -*-
# Copyright (C) Hugo Branquinho. All rights reserved.
# 
# @author Hugo Branquinho <hugobranq@gmail.com>
# 
# $Id$

from urllib2 import URLError

from recaptcha.client.captcha import displayhtml
from recaptcha.client.captcha import submit


class reCaptcha(object): # pragma: no cover
    def __init__(
            self,
            public_key,
            private_key,
            disable_recaptcha=False,
            html_code=None):
        self.public_key = public_key
        self.private_key = private_key
        self.disable_recaptcha = disable_recaptcha
        self.html_code = html_code

    def html(self, request, theme_name=None):
        if self.disable_recaptcha:
            return self.html_code or u''

        html = displayhtml(
                   self.public_key,
                   use_ssl=request.protocol == 'https')

        if theme_name:
            html = u'<script type="text/javascript">var RecaptchaOptions=' \
                   u'{theme:\'%s\'};</script>%s' % (theme_name, html)

        if self.html_code:
            return self.html_code.format(html=html)
        else:
            return html

    def validate(self, request):
        if self.disable_recaptcha:
            return True

        try:
            response = submit(
                           request.get_value('recaptcha_challenge_field'),
                           request.get_value('recaptcha_response_field'),
                           self.private_key,
                           request.ip_address)
        except URLError:
            return True
        else:
            return response.is_valid
