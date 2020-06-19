import slack
import traceback


class Slack:
    def __init__(self, token, channel):
        self.client = slack.WebClient(token)
        self.group_id = None
        groups = self.client.groups_list()
        for gp in groups['groups']:
            if gp['name'] == channel:
                self.group_id = gp['id']
                break
        if not self.group_id:
            self.group_id = self.client.conversations_create(
                name=channel.lower(),
                is_private=True
            )['channel']['id']
            admins = [u['id'] for u in self.client.users_list()['members'] if u.get('is_admin') or u.get('is_owner')]
            for admin in admins:
                self.client.groups_invite(channel=self.group_id, user=admin)

    def send_snippet(self, title, initial_comment, code, code_type='python'):
        self.client.files_upload(
            channels=self.group_id,
            title=title,
            initial_comment=initial_comment.replace('<br>', ''),
            content=code,
            filetype=code_type
        )

    def send_exception_snippet(self, domain, event, code_type='python'):
        message = traceback.format_exc() + '\n\n\n' + format.dumps(event, indent=2)
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
            channel=self.group_id,
            blocks=blocks
        )
