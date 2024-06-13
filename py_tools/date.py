import uuid
from datetime import datetime, timezone
import pytz


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


def convert_date_utc(input_date, from_timezone):
    if isinstance(input_date, str):
        # Parse the ISO date string into a datetime object
        input_date = datetime.fromisoformat(input_date)

    # Identify the source timezone
    source_tz = pytz.timezone(from_timezone)

    # Localize the datetime object to the source timezone
    local_time = source_tz.localize(input_date)

    # Convert localized time to UTC
    utc_time = local_time.astimezone(pytz.utc)

    return utc_time
