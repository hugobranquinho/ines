# -*- coding: utf-8 -*-

from email.utils import formataddr, formatdate, make_msgid
from os.path import basename, join as join_paths
from socket import getfqdn
from smtplib import SMTPRecipientsRefused

from pyramid.decorator import reify

from ines import HTML_NEW_LINE, lazy_import_module, NEW_LINE
from ines.api import BaseSession, BaseSessionManager
from ines.api.jobs import job
from ines.convert import maybe_list, maybe_string, to_string
from ines.exceptions import Error
from ines.i18n import _
from ines.middlewares.repozetm import RepozeTMMiddleware
from ines.mimetype import find_mimetype
from ines.utils import get_dir_filenames, make_dir, make_unique_hash


def format_email(email, encoding=None, force_development_email=None):
    label = None
    if isinstance(email, dict):
        email = to_string(email['email'], encoding=encoding)
        if 'label' in email:
            label = to_string(email['label'], encoding=encoding)
    elif not isinstance(email, str):
        label = to_string(email[0], encoding=encoding)
        email = to_string(email[1], encoding=encoding)
    else:
        email = to_string(email, encoding=encoding)
    return formataddr((label, force_development_email and to_string(force_development_email) or email))


class BaseMailerSessionManager(BaseSessionManager):
    __api_name__ = 'mailer'

    def __init__(self, *args, **kwargs):
        super(BaseMailerSessionManager, self).__init__(*args, **kwargs)

        pyramid_mailer = lazy_import_module('pyramid_mailer')
        self.mailer = pyramid_mailer.mailer_factory_from_settings(self.settings, prefix='')
        pyramid_mailer_message = lazy_import_module('pyramid_mailer.message')
        self.message_cls = pyramid_mailer_message.Message
        self.attachment_cls = pyramid_mailer_message.Attachment

        if self.settings.get('queue_path'):
            make_dir(self.settings['queue_path'])
            make_dir(join_paths(self.settings['queue_path'], 'cur'))
            make_dir(join_paths(self.settings['queue_path'], 'tmp'))
            make_dir(join_paths(self.settings['queue_path'], 'new'))

            sendmail_queue = lazy_import_module('repoze.sendmail.queue')
            self.queue_processor = sendmail_queue.QueueProcessor

            self.transaction = lazy_import_module('transaction')
            self.__dict__.setdefault('__middlewares__', []).append(RepozeTMMiddleware)

    @reify
    def default_domain(self):
        return getfqdn()


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

        force_development_email = None
        if not self.request.is_production_environ:
            force_development_email = self.settings.get('force_development_email') or None

        if body:
            body = to_string(body, encoding=content_charset)
            if not html:
                html = body.replace(NEW_LINE, HTML_NEW_LINE)

        if html:
            html = to_string(html, encoding=content_charset)
            if not html.lower().startswith('<html'):
                html = '<html><body>%s</body></html>' % html

        options = {}
        # FROM sender
        if sender:
            options['sender'] = format_email(
                sender,
                encoding=content_charset,
                force_development_email=force_development_email)

        # Envelope CC
        if cc:
            if isinstance(cc, dict):
                cc = [cc]
            options['cc'] = [
                format_email(e, content_charset, force_development_email=force_development_email)
                for e in maybe_list(cc)]

        # Envelope BCC
        if bcc:
            if isinstance(bcc, dict):
                bcc = [bcc]
            options['bcc'] = [
                format_email(e, content_charset, force_development_email=force_development_email)
                for e in maybe_list(bcc)]

        if not message_id:
            domain = self.settings.get('message_domain') or self.api_session_manager.default_domain
            message_id = make_msgid(make_unique_hash(10), domain)

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
                f = attachment.get('file') or attachment['fp']
                filename = to_string(attachment.get('filename') or basename(f.name)).replace(' ', '')
                mimetype = to_string(attachment.get('content_type') or find_mimetype(filename, f))

                f.seek(0)
                mime_attachments.append(self.api_session_manager.attachment_cls(
                    data=f,
                    filename=filename,
                    content_type=mimetype))

        return self.api_session_manager.message_cls(
            subject=to_string(subject, encoding=content_charset),
            html=html,
            body=body,
            recipients=[
                format_email(e, content_charset, force_development_email=force_development_email)
                for e in maybe_list(recipients)],
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
                        raise Error('email', 'Invalid email "%s"' % to_string(email))
                    elif code == 450:
                        raise Error('email', 'Invalid email domain "%s"' % to_string(email))
            raise
        else:
            return True
