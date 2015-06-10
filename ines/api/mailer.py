# -*- coding: utf-8 -*-

from email.mime.text import MIMEText
from email.utils import formataddr
from email.utils import formatdate
from email.utils import getaddresses
from os.path import basename
from smtplib import SMTPRecipientsRefused

from ines.api import BaseSessionManager
from ines.api import BaseSession
from ines.convert import force_string
from ines.convert import force_unicode
from ines.convert import maybe_list
from ines.exceptions import Error
from ines.mimetype import find_mimetype


def format_email(email, encoding=None):
    label = None

    if isinstance(email, dict):
        if 'label' in email:
            label = email['label']
        email = email['email']

    elif not isinstance(email, basestring):
        label, email = email

    return formataddr((
        force_string(label, encoding=encoding),
        force_string(email, encoding=encoding)))


class BaseMailerSessionManager(BaseSessionManager):
    __api_name__ = 'mailer'

    def __init__(self, *args, **kwargs):
        super(BaseMailerSessionManager, self).__init__(*args, **kwargs)

        from pyramid_mailer import mailer_factory_from_settings
        self.mailer = mailer_factory_from_settings(self.settings, prefix='')
        from pyramid_mailer.message import Message
        self.message_cls = Message
        from pyramid_mailer.message import Attachment
        self.attachment_cls = Attachment


class BaseMailerSession(BaseSession):
    __api_name__ = 'mailer'

    def create_message(
            self,
            subject,
            message,
            recipients,
            sender=None,
            cc=None,
            bcc=None,
            reply_to=None,
            as_html=True,
            content_charset='utf-8',
            attachments=None,
            with_signature=True):

        message = force_unicode(message, encoding=content_charset)
        if with_signature and self.settings.get('signature'):
            message += u'\n\n'
            message += force_unicode(self.settings['signature'], encoding=content_charset)

        options = {}
        if as_html:
            options['html'] = message.replace(u'\n', u'<br>')
        else:
            options['body'] = message

        # FROM sender
        if sender:
            options['sender'] = format_email(sender, encoding=content_charset)

        # Envelope CC
        if cc:
            if not isinstance(cc, list):
                cc = [cc]
            options['cc'] = [format_email(e, content_charset) for e in cc]

        # Envelope BCC
        if bcc:
            if not isinstance(bcc, list):
                bcc = [bcc]
            options['bcc'] = [format_email(e, content_charset) for e in bcc]

        if not isinstance(recipients, list):
            recipients = [recipients]

        extra_headers = {
            'Date': formatdate(localtime=True)}

        # Force reply to another email
        if reply_to:
            extra_headers['Reply-To'] = '<%s>' % force_string(reply_to)

        mime_attachments = []
        if attachments:
            for attachment in attachments:
                filename = force_string(
                    attachment.get('filename')
                    or basename(attachment['file'].name)).replace(' ', '')
                mimetype = force_string(
                    attachment.get('content_type')
                    or find_mimetype(filename, attachment['file']))

                attachment['file'].seek(0)
                mime_attachments.append(self.api_session_manager.attachment_cls(
                    data=attachment['file'],
                    filename=filename,
                    content_type=mimetype))

        return self.api_session_manager.message_cls(
            subject=force_string(subject, encoding=content_charset),
            recipients=[format_email(e, content_charset) for e in recipients],
            attachments=mime_attachments,
            extra_headers=extra_headers,
            **options)

    def send(self, *args, **kwargs):
        mime_message = self.create_message(*args, **kwargs)
        return self.send_immediately(mime_message, fail_silently=False)

    def send_immediately(self, mime_message, fail_silently=False):
        try:
            self.api_session_manager.mailer.send_immediately(mime_message, fail_silently=fail_silently)
        except Exception as error:
            if isinstance(error, SMTPRecipientsRefused):
                for email, error_message in error.args[0].items():
                    code = error_message[0]
                    if code == 554:
                        raise Error('email', u'Invalid email "%s"' % force_unicode(email))
                    elif code == 450:
                        raise Error('email', u'Invalid email domain "%s"' % force_unicode(email))
            raise
        else:
            return True
