import traceback

from slack_sdk import WebClient, errors
import time
from py_tools.format import dumps
import backoff


class Slack:
    def __init__(self, bot_token, channel_name=None, channel_id=None, user_token=None):
        self.client = WebClient(bot_token)
        self.channel_id = channel_id
        self.user_token = user_token

        channels = self.client.conversations_list(
            types='public_channel,private_channel')

        if channel_name:
            for channel in channels['channels']:
                if channel['name'] == channel_name:
                    self.channel_id = channel['id']
                    break
        if not self.channel_id:
            self.channel_id = self.client.conversations_create(
                name=channel_name.lower(),
                is_private=True
            )['channel']['id']
            admins = [u['id'] for u in self.client.users_list(
            )['members'] if u.get('is_admin') or u.get('is_owner')]
            self.client.conversations_invite(
                channel=self.channel_id, users=admins)

    def send_snippet(self, title, initial_comment, code, code_type='python', thread_ts=None):
        return self.client.files_upload(
            channels=self.channel_id,
            title=title,
            initial_comment=initial_comment.replace('<br>', ''),
            content=code,
            filetype=code_type,
            thread_ts=thread_ts
        )['ts']

    def send_exception_snippet(self, domain, event, code_type='python', thread_ts=None):
        message = traceback.format_exc() + '\n\n\n' + dumps(event, indent=2)
        subject = 'Error occurred in ' + domain
        self.send_snippet(subject, subject, message,
                          code_type=code_type, thread_ts=thread_ts)

    def send_raw_message(self, blocks, thread_ts=None):
        return self.client.chat_postMessage(
            channel=self.channel_id,
            blocks=blocks,
            thread_ts=thread_ts
        )['ts']

    def update_raw_message(self, ts, blocks):
        self.client.chat_update(
            channel=self.channel_id,
            blocks=blocks,
            ts=ts
        )

    def get_perm_link(self, ts):
        return self.client.chat_getPermalink(
            channel=self.channel_id,
            message_ts=ts
        )['permalink']

    def send_message(self, message, attachment=None, thread_ts=None):
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

        return self.send_raw_message(blocks, thread_ts)

    @backoff.on_exception(backoff.expo, errors.SlackApiError)
    def try_and_delete_message(self, message_ts, as_user=False):
        try:
            self.client.chat_delete(
                channel=self.channel_id,
                ts=message_ts,
                as_user=as_user
            )
        except BaseException:
            print('Error messages from slack')
            traceback.print_exc()

    def delete_message(self, ts):
        as_user = False
        if self.user_token:
            self.client = WebClient(self.user_token)
            as_user = True
        while ts:
            response = self.client.conversations_replies(
                ts=ts,
                limit=999,
                channel=self.channel_id
            )
            for message in response['messages']:
                if message['ts'] != ts:
                    self.try_and_delete_message(message['ts'], as_user)
                    time.sleep(1)
            if not response['has_more']:
                break
        self.try_and_delete_message(ts, as_user)
