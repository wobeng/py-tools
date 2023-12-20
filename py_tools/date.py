import uuid
from datetime import datetime

from pytz import UTC


def datetime_utc(dt=None):
    if not dt:
        dt = datetime.utcnow()
    return dt.replace(tzinfo=UTC)


def id_generator(pre=""):
    return pre + str(uuid.uuid4()).replace("-", "")[-14:]


def date_id(prefix="", sep="::"):
    def call(pre=prefix):
        # Getting the current time and formatting it without microseconds
        current_time = datetime.now().replace(tzinfo=None, microsecond=0)
        # Converting to ISO 8601 format without special characters
        formatted_time = current_time.isoformat().replace("-", "").replace(":", "").replace("T", "").replace("Z", "")
        suf = formatted_time + "_" +  id_generator()
        if pre:
            pre = pre + sep
        return pre + suf

    return call
