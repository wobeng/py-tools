import os
from datetime import timezone, datetime, timedelta
from pynamodb.attributes import (
    UTCDateTimeAttribute,
    MapAttribute,
    TTLAttribute,
    JSONAttribute as JSONA,
)
from py_tools import format
from pytz import timezone as pytz_timezone


class JSONAttribute(JSONA):
    def serialize(self, value):
        if value is None:
            return None
        encoded = format.dumps(value)
        return encoded

    def deserialize(self, value):
        return format.loads(value)


class DynamicMapAttribute(MapAttribute):
    element_type = None

    def __init__(self, *args, of=None, **kwargs):
        if of:
            if not issubclass(of, MapAttribute):
                raise ValueError('"of" must be subclass of MapAttribute')
            self.element_type = of
        super(DynamicMapAttribute, self).__init__(*args, **kwargs)

    def _set_attributes(self, **attrs):
        """
        Sets the attributes for this object
        """
        for name, value in attrs.items():
            setattr(self, name, value)

    def deserialize(self, values):
        """
        Decode from map of AttributeValue types.
        """
        if not self.element_type:
            return super(DynamicMapAttribute, self).deserialize(values)

        class_for_deserialize = self.element_type()
        return {
            k: class_for_deserialize.deserialize(attr_value)
            for k, v in values.items()
            for _, attr_value in v.items()
        }

    @classmethod
    def is_raw(cls):
        return cls == DynamicMapAttribute


def _get_timezone():
    """
    Retrieves the user's timezone from the TIMEZONE environment variable or defaults to UTC.
    """
    user_timezone = os.getenv("TIMEZONE", default="UTC")
    try:
        return pytz_timezone(user_timezone)
    except Exception as e:
        raise ValueError(f"Invalid timezone '{user_timezone}': {e}")


class UserTimezoneTTLAttribute(TTLAttribute):
    def _normalize(self, value):
        if value is None:
            return
        if isinstance(value, datetime):
            if value.tzinfo is None:
                raise ValueError("datetime must be timezone-aware")
            # Skip UTC normalization to preserve user's timezone
            return value
        elif isinstance(value, timedelta):
            # Assume timedelta is relative to current UTC time
            return datetime.now(timezone.utc) + value
        else:
            raise ValueError("TTLAttribute value must be a timedelta or datetime")

    def serialize(self, value):
        """
        Converts a datetime object from the user's timezone to UTC and serializes it.
        """
        if isinstance(value, datetime):
            if value.tzinfo is None:
                raise ValueError("Datetime value must be timezone-aware.")

            # Get the user's timezone
            tz = _get_timezone()

            # Convert the datetime from the user's timezone to UTC
            user_time = value.astimezone(tz)
            utc_time = user_time.astimezone(timezone.utc)

            # Serialize the UTC datetime as a timestamp
            return super().serialize(utc_time)
        elif isinstance(value, timedelta):
            # For timedelta, directly pass it to the parent serialize
            return super().serialize(value)
        else:
            raise ValueError("TTLAttribute value must be a timedelta or datetime")

    def deserialize(self, value):
        """
        Deserializes a timestamp (Unix time) to a datetime object in the user's timezone.
        """
        # Deserialize the value as UTC datetime using the base class
        utc_datetime = super().deserialize(value)

        # Get the user's timezone
        tz = _get_timezone()

        # Convert the UTC datetime to the user's timezone
        return utc_datetime.astimezone(tz)


class UserTimezoneDateTimeAttribute(UTCDateTimeAttribute):
    def deserialize(self, value):
        """
        Converts a UTC datetime string to the user's timezone based on the TIMEZONE environment variable.
        """
        # Parse the UTC datetime string using the base class
        utc_datetime = super().deserialize(value)

        # Get the timezone from the environment
        tz = _get_timezone()

        # Convert the UTC datetime to the user's timezone
        return utc_datetime.astimezone(tz)


class UTCZoneAwareDateTimeAttribute(UserTimezoneDateTimeAttribute):
    def serialize(self, value):
        """
        Converts a datetime object from the user's timezone to UTC before saving.
        """
        if value.tzinfo is None:
            raise ValueError("Datetime value must be timezone-aware.")

        # Get the timezone from the environment
        tz = _get_timezone()

        # Convert the datetime from the user's timezone to UTC
        user_time = value.astimezone(tz)
        utc_time = user_time.astimezone(timezone.utc)

        # Serialize the UTC datetime using the base class
        return super().serialize(utc_time)
