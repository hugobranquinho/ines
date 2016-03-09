# -*- coding: utf-8 -*-

from email.utils import formataddr
from email.utils import formatdate
from email.utils import make_msgid
from os import linesep
from os.path import basename
from smtplib import SMTPRecipientsRefused

from six import _import_module
from six import string_types
from six import u

from ines.api import BaseSessionManager
from ines.api import BaseSession
from ines.api.jobs import job
from ines.convert import to_string
from ines.convert import to_unicode
from ines.convert import maybe_list
from ines.convert import maybe_string
from ines.exceptions import Error
from ines.i18n import _
from ines.middlewares.repozetm import RepozeTMMiddleware
from ines.mimetype import find_mimetype
from ines.path import join_paths
from ines.utils import get_dir_filenames
from ines.utils import make_dir
from ines.utils import make_unique_hash


NEW_LINE = u(linesep)
HTML_NEW_LINE = u('<br/>')


def format_email(email, encoding=None):
    label = None

    if isinstance(email, dict):
        if 'label' in email:
            label = email['label']
        email = email['email']

    elif not isinstance(email, string_types):
        label, email = email

    return to_unicode(
        formataddr((
            maybe_string(label, encoding=encoding),
            to_string(email, encoding=encoding))))


class BaseMailerSessionManager(BaseSessionManager):
    __api_name__ = 'mailer'

    def __init__(self, *args, **kwargs):
        super(BaseMailerSessionManager, self).__init__(*args, **kwargs)

        pyramid_mailer = _import_module('pyramid_mailer')
        self.mailer = pyramid_mailer.mailer_factory_from_settings(self.settings, prefix='')
        pyramid_mailer_message = _import_module('pyramid_mailer.message')
        self.message_cls = pyramid_mailer_message.Message
        self.attachment_cls = pyramid_mailer_message.Attachment

        if self.settings.get('queue_path'):
            make_dir(self.settings['queue_path'])
            make_dir(join_paths(self.settings['queue_path'], 'cur'))
            make_dir(join_paths(self.settings['queue_path'], 'tmp'))
            make_dir(join_paths(self.settings['queue_path'], 'new'))

            sendmail_queue = _import_module('repoze.sendmail.queue')
            self.queue_processor = sendmail_queue.QueueProcessor

            self.transaction = _import_module('transaction')
            self.__dict__.setdefault('__middlewares__', []).append(RepozeTMMiddleware)


class BaseMailerSession(BaseSession):
    __api_name__ = 'mailer'

    def create_message(
            self,
            subject,
            recipients,
            body=None,
            html=None,
            sender=None,
            cc=None,
            bcc=None,
            reply_to=None,
            content_charset='utf-8',
            attachments=None,
            message_id=None):

        if body:
            body = to_unicode(body, encoding=content_charset)
            if not html:
                html = body.replace(NEW_LINE, HTML_NEW_LINE)

        if html:
            html = to_unicode(html, encoding=content_charset)
            if not html.lower().startswith('<html'):
                html = '<html><body>%s</body></html>' % html

        options = {}
        # FROM sender
        if sender:
            options['sender'] = format_email(sender, encoding=content_charset)

        # Envelope CC
        if cc:
            if isinstance(cc, dict):
                cc = [cc]
            options['cc'] = [format_email(e, content_charset) for e in maybe_list(cc)]

        # Envelope BCC
        if bcc:
            if isinstance(bcc, dict):
                bcc = [bcc]
            options['bcc'] = [format_email(e, content_charset) for e in maybe_list(bcc)]

        if not isinstance(recipients, list):
            recipients = [recipients]

        if not message_id:
            message_id = make_msgid(make_unique_hash(10))

        extra_headers = {
            'Date': formatdate(localtime=True),
            'Message-ID': to_string(message_id)}

        # Force reply to another email
        if reply_to:
            extra_headers['Reply-To'] = '<%s>' % to_string(reply_to)

        mime_attachments = []
        if attachments:
            if isinstance(attachments, dict):
                attachments = [attachments]
            for attachment in maybe_list(attachments):
                filename = to_string(
                    attachment.get('filename')
                    or basename(attachment['file'].name)).replace(' ', '')
                mimetype = to_string(
                    attachment.get('content_type')
                    or find_mimetype(filename, attachment['file']))

                attachment['file'].seek(0)
                mime_attachments.append(self.api_session_manager.attachment_cls(
                    data=attachment['file'],
                    filename=filename,
                    content_type=mimetype))

        return self.api_session_manager.message_cls(
            subject=to_unicode(subject, encoding=content_charset),
            html=html,
            body=body,
            recipients=[format_email(e, content_charset) for e in recipients],
            attachments=mime_attachments,
            extra_headers=extra_headers,
            **options)

    @job(second=0, minute='*/5',
         title=_('Send queue emails'),
         unique_name='ines:mailer_queue_send')
    def mailer_queue_send(self):
        queue_path = self.settings.get('queue_path')
        if queue_path:
            subdir_new = join_paths(queue_path, 'new')
            subdir_cur = join_paths(queue_path, 'cur')

            while True:
                for f in get_dir_filenames(subdir_new):
                    if not f.startswith('.'):
                        break
                else:
                    for f in get_dir_filenames(subdir_cur):
                        if not f.startswith('.'):
                            break
                    else:
                        break  # Break while

                qp = self.api_session_manager.queue_processor(
                    self.api_session_manager.mailer.smtp_mailer,
                    self.settings['queue_path'])
                qp.send_messages()

    def send_to_queue(self, *args, **kwargs):
        if not self.settings.get('queue_path'):
            return self.send_immediately(*args, **kwargs)

        mime_message = self.create_message(*args, **kwargs)
        self.api_session_manager.mailer.send_to_queue(mime_message)
        self.mailer_queue_send.run_job()
        return True

    def send_immediately(self, *args, **kwargs):
        fail_silently = kwargs.pop('fail_silently', False)
        mime_message = self.create_message(*args, **kwargs)

        try:
            self.api_session_manager.mailer.send_immediately(mime_message, fail_silently=fail_silently)
        except Exception as error:
            if isinstance(error, SMTPRecipientsRefused):
                for email, error_message in error.args[0].items():
                    code = error_message[0]
                    if code == 554:
                        raise Error('email', u('Invalid email "%s"') % to_unicode(email))
                    elif code == 450:
                        raise Error('email', u('Invalid email domain "%s"') % to_unicode(email))
            raise
        else:
            return True
