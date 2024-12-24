import os
from datetime import timezone
from pynamodb.attributes import (
    UTCDateTimeAttribute,
    MapAttribute,
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


class UTCZoneAwareDateTimeAttribute(UTCDateTimeAttribute):
    def serialize(self, value):
        """
        Converts a datetime object from the user's timezone to UTC before saving.
        """
        if value.tzinfo is None:
            raise ValueError("Datetime value must be timezone-aware.")

        # Get the timezone from the environment or default to UTC
        user_timezone = os.getenv("TIMEZONE")

        try:
            tz = pytz_timezone(user_timezone)
        except Exception as e:
            raise ValueError(
                f"Invalid timezone '{user_timezone}' in environment variable TIMEZONE: {e}"
            )

        # Convert the datetime from the user's timezone to UTC
        user_time = value.astimezone(tz)
        utc_time = user_time.astimezone(timezone.utc)

        # Serialize the UTC datetime using the base class
        return super().serialize(utc_time)

    def deserialize(self, value):
        """
        Converts a UTC datetime string to the user's timezone based on the TIMEZONE environment variable.
        """
        # Parse the UTC datetime string using the base class
        utc_datetime = super().deserialize(value)

        # Get the timezone from the environment or default to UTC
        user_timezone = os.getenv("TIMEZONE", "UTC")

        try:
            tz = pytz_timezone(user_timezone)
        except Exception as e:
            raise ValueError(
                f"Invalid timezone '{user_timezone}' in environment variable TIMEZONE: {e}"
            )

        # Convert the UTC datetime to the user's timezone
        local_datetime = utc_datetime.astimezone(tz)
        return local_datetime
