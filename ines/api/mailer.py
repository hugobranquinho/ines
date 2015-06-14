# -*- coding: utf-8 -*-

from email.utils import formataddr
from email.utils import formatdate
from email.utils import make_msgid
from os.path import basename
from os.path import join as join_path
from smtplib import SMTPRecipientsRefused

from pkg_resources import get_distribution

from ines.api import BaseSessionManager
from ines.api import BaseSession
from ines.api.jobs import job
from ines.convert import force_string
from ines.convert import force_unicode
from ines.exceptions import Error
from ines.middlewares.repozetm import RepozeTMMiddleware
from ines.mimetype import find_mimetype
from ines.utils import get_dir_filenames
from ines.utils import make_dir
from ines.utils import make_unique_hash


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

        import pyramid_mailer.message
        self.mailer = pyramid_mailer.mailer_factory_from_settings(self.settings, prefix='')
        self.message_cls = pyramid_mailer.message.Message
        self.attachment_cls = pyramid_mailer.message.Attachment

        if self.settings.get('queue_path'):
            make_dir(self.settings['queue_path'])

            import repoze.sendmail.queue
            self.queue_processor = repoze.sendmail.queue.QueueProcessor
            self.repoze_sendmail_version = get_distribution('repoze.sendmail').version

            import transaction
            self.transaction = transaction
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
            message_id=None,
            with_signature=True):

        signature = None
        if with_signature and self.settings.get('signature'):
            signature = u'\n\n'
            signature += force_unicode(self.settings['signature'], encoding=content_charset)

        if body:
            body = force_unicode(body, encoding=content_charset)
            if signature:
                body += signature
            if not html:
                html = body.replace(u'\n', u'<br>')

        if html:
            html = force_unicode(html, encoding=content_charset)
            if not html.startswith('<html'):
                html = u'<html><header></header><body>%s</body></html>' % html
            if signature:
                signature_html = signature.replace(u'\n', u'<br>')
                html = html.replace(u'</body>', u'%s</body>' % signature_html)

        options = {}
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

        if not message_id:
            message_id = make_msgid(make_unique_hash(10))

        extra_headers = {
            'Date': formatdate(localtime=True),
            'Message-ID': force_string(message_id)}

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
            subject=force_unicode(subject, encoding=content_charset),
            html=html,
            body=body,
            recipients=[format_email(e, content_charset) for e in recipients],
            attachments=mime_attachments,
            extra_headers=extra_headers,
            **options)

    @job(second=0, minute=[0, 15, 30, 45])
    def mailer_queue_send(self):
        queue_path = self.settings.get('queue_path')
        if queue_path:
            # See: https://github.com/repoze/repoze.sendmail/pull/34
            if self.api_session_manager.repoze_sendmail_version > (4, 2):
                qp = self.api_session_manager.queue_processor(
                    self.api_session_manager.mailer.smtp_mailer,
                    self.settings['queue_path'])
                qp.send_messages()
            else:
                subdir_new = join_path(queue_path, 'new')
                subdir_cur = join_path(queue_path, 'cur')

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
                        raise Error('email', u'Invalid email "%s"' % force_unicode(email))
                    elif code == 450:
                        raise Error('email', u'Invalid email domain "%s"' % force_unicode(email))
            raise
        else:
            return True
