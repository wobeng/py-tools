import uuid
from datetime import datetime, timezone


def datetime_utc(dt=None):
    if not dt:
        dt = datetime.now(timezone.utc)
    return dt.replace(tzinfo=timezone.utc)


def id_generator(prefix=""):
    def wrapper(pre=prefix):
        return pre + str(uuid.uuid4()).replace("-", "")[-14:]

    return wrapper


def formatted_time(datetime_obj=None):
    # Getting the current time and formatting it without microseconds
    datetime_obj = datetime_obj or datetime_utc()
    # Converting to ISO 8601 format without special characters
    return datetime_obj.strftime("%Y%m%d%H%M%S")


def date_id(prefix="", sep="::"):
    def wrapper(pre=prefix):
        f_time = formatted_time()
        suf = f_time + "_" + id_generator()()
        if pre:
            pre = pre + sep
        return pre + suf

    return wrapper
