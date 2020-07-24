import traceback

import slack

from py_tools.format import dumps


class Slack:
    def __init__(self, token, channel_name):
        self.client = slack.WebClient(token)
        self.channel_id = None
        channels = self.client.conversations_list(types='public_channel,private_channel')
        for channel in channels['channels']:
            if channel['name'] == channel_name:
                self.channel_id = channel['id']
                break
        if not self.channel_id:
            self.channel_id = self.client.conversations_create(
                name=channel_name.lower(),
                is_private=True
            )['channel']['id']
            admins = [u['id'] for u in self.client.users_list()['members'] if u.get('is_admin') or u.get('is_owner')]
            self.client.conversations_invite(channel=self.channel_id, users=admins)

    def send_snippet(self, title, initial_comment, code, code_type='python'):
        self.client.files_upload(
            channels=self.channel_id,
            title=title,
            initial_comment=initial_comment.replace('<br>', ''),
            content=code,
            filetype=code_type
        )

    def send_exception_snippet(self, domain, event, code_type='python'):
        message = traceback.format_exc() + '\n\n\n' + dumps(event, indent=2)
        subject = 'Error occurred in ' + domain
        self.send_snippet(subject, subject, message, code_type=code_type)

    def send_message(self, message, attachment=None):
        blocks = [
            {
                'type': 'section',
                'text': {
                    'type': 'mrkdwn',
                    'text': message.replace('<br>', '')
                }
            },
            {
                'type': 'divider'
            }
        ]
        if attachment:
            blocks[0]['accessory'] = {
                'type': 'button',
                'text': {
                    'type': 'plain_text',
                    'text': attachment['text'],
                    'emoji': True
                },
                'url': attachment['value']
            }

        self.client.chat_postMessage(
            channel=self.channel_id,
            blocks=blocks
        )
