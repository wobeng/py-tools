import uuid
from datetime import datetime

from pytz import UTC


def datetime_utc(dt=None):
    if not dt:
        dt = datetime.utcnow()
    return dt.replace(tzinfo=UTC)


def date_id(prefix="", sep="::"):
    def call(pre=prefix):
        uid = str(uuid.uuid4()).replace("-", "")
        # Getting the current time and formatting it without microseconds
        current_time = datetime.now().replace(tzinfo=None, microsecond=0)
        # Converting to ISO 8601 format without special characters
        formatted_time = current_time.isoformat().replace("-", "").replace(":", "").replace("T", "").replace("Z", "")
        suf = formatted_time + "_" + uid[-14:]
        if pre:
            pre = pre + sep
        return pre + suf

    return call
