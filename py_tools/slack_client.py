import traceback

from slack_sdk import WebClient, errors
import time
from py_tools.format import dumps
import backoff
import tempfile

from py_tools.pylog import get_logger

logger = get_logger("py-tools.slack")


class Slack:
    def __init__(self, bot_token, channel_name=None, channel_id=None, user_token=None):
        self.client = WebClient(bot_token)
        self.channel_id = channel_id
        self.user_token = user_token
        self.channel_name = channel_name

        while not self.channel_id:
            self.channel_id = self.get_channel_id(channel_name)

            if self.channel_id:
                break

            try:
                channel = self.client.conversations_create(
                    name=channel_name.lower(), is_private=True
                )

                # set the channel id
                self.channel_id = channel["channel"]["id"]

                # invite all admins to the channel
                all_members = [u for u in self.client.users_list()["members"]]
                admins = [
                    u["id"]
                    for u in all_members
                    if u.get("is_admin") or u.get("is_owner")
                ]

                self.client.conversations_invite(channel=self.channel_id, users=admins)
                break

            except errors.SlackApiError as e:
                if e.response["error"] == "name_taken":
                    continue # to get try and get the channel id again if the channel name is taken
                raise e

    def get_channel_id(self, name):
        channels = self.client.conversations_list(
            exclude_archived=True, limit=999, types="public_channel,private_channel"
        )

        for channel in channels["channels"]:
            if channel["name"] == name:
                return channel["id"]

    def send_snippet(
        self, title, initial_comment, code, code_type="python", thread_ts=None
    ):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".log") as temp:
            temp.write(code)
            temp.flush()
            return self.client.files_upload(
                channels=self.channel_id,
                title=title,
                initial_comment=initial_comment.replace("<br>", ""),
                file=temp.name,
                filetype=code_type,
                thread_ts=thread_ts,
            )["ts"]

    def send_exception_snippet(self, domain, event, code_type="python", thread_ts=None):
        message = traceback.format_exc() + "\n\n\n" + dumps(event, indent=2)
        subject = "Error occurred in " + domain
        self.send_snippet(
            subject, subject, message, code_type=code_type, thread_ts=thread_ts
        )

    def send_raw_message(self, blocks, thread_ts=None):
        return self.client.chat_postMessage(
            channel=self.channel_id, blocks=blocks, thread_ts=thread_ts
        )["ts"]

    def update_raw_message(self, ts, blocks):
        self.client.chat_update(channel=self.channel_id, blocks=blocks, ts=ts)

    def get_perm_link(self, ts):
        return self.client.chat_getPermalink(channel=self.channel_id, message_ts=ts)[
            "permalink"
        ]

    def send_message(self, message, attachment=None, thread_ts=None):
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": message.replace("<br>", ""),
                },
            },
            {"type": "divider"},
        ]
        if attachment:
            blocks[0]["accessory"] = {
                "type": "button",
                "text": {
                    "type": "plain_text",
                    "text": attachment["text"],
                    "emoji": True,
                },
                "url": attachment["value"],
            }

        return self.send_raw_message(blocks, thread_ts)

    @backoff.on_exception(backoff.expo, errors.SlackApiError, logger="pytest")
    def try_and_delete_message(self, message_ts, as_user=False):
        try:
            self.client.chat_delete(
                channel=self.channel_id, ts=message_ts, as_user=as_user
            )
        except errors.SlackApiError as e:
            if e.response.status_code == 429:
                time.sleep(int(e.response.headers.get("Retry-After", "10")))
            if e.response["error"] == "message_not_found":
                return
            raise e

        except BaseException:
            logger.critical("Error messages from slack")
            logger.critical(traceback.format_exc())

    def delete_message(self, slack_messages):
        as_user = False
        if self.user_token:
            self.client = WebClient(self.user_token)
            as_user = True

        for slack_ts in slack_messages:
            if "channel" in slack_ts:
                self.channel_id = self.get_channel_id(slack_ts["channel"])
                slack_ts = slack_ts["ts"]

            while slack_ts:
                try:
                    response = self.client.conversations_replies(
                        ts=slack_ts, limit=999, channel=self.channel_id
                    )
                    for message in response["messages"]:
                        if message["ts"] != slack_ts:
                            self.try_and_delete_message(message["ts"], as_user)
                    if not response["has_more"]:
                        break
                except errors.SlackApiError as e:
                    if e.response["error"] == "thread_not_found":
                        break
            self.try_and_delete_message(slack_ts, as_user)
