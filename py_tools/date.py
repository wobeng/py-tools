import uuid
from datetime import datetime

from pytz import UTC


def datetime_utc(dt=None):
    if not dt:
        dt = datetime.utcnow()
    return dt.replace(tzinfo=UTC)


def date_id(prefix=""):
    def call(pre=prefix):
        suf = datetime_utc().replace(tzinfo=None).isoformat().replace(".", ":")
        suf = suf + "_" + str(uuid.uuid4())
        suf = suf or suf
        if pre:
            pre = pre + "::"
        return pre + suf

    return call
