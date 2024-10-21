import functools
import operator
import os
from copy import deepcopy
from py_tools.date import datetime_utc
from typing import Any, Dict, List, Optional, Union

from pynamodb.attributes import (
    UTCDateTimeAttribute,
    MapAttribute,
    JSONAttribute as JSONA,
)
from pynamodb.exceptions import DoesNotExist
from pynamodb.models import Model
from pynamodb.transactions import TransactWrite as _TransactWrite
from py_tools import format
from py_tools.pylog import get_logger


logger = get_logger("py-tools.dydb")


class ModelEncoder(format.ModelEncoder):
    def default(self, obj):
        if hasattr(obj, "attribute_values"):
            return obj.attribute_values
        return super(ModelEncoder, self).default(obj)


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


class DbModel(Model):
    _db_conditions = {}
    created_on = UTCDateTimeAttribute()
    updated_on = UTCDateTimeAttribute()

    def __init__(self, hash_key=None, range_key=None, **attrs):
        super(DbModel, self).__init__(hash_key, range_key, **attrs)
        self._hash_key = getattr(self.__class__, self._hash_keyname)

    def dict(self):
        return format.loads(format.dumps(self, cls=ModelEncoder))

    @staticmethod
    def get_first(items):
        items = [i.dict() for i in items]
        if not items:
            raise DoesNotExist
        return items[0]

    @property
    def key(self):
        key = {self._hash_keyname: getattr(self, self._hash_keyname)}
        if self._range_keyname:
            key[self._range_keyname] = getattr(self, self._range_keyname)
        return key

    @classmethod
    def add_db_conditions(cls, condition: Optional[Any]):
        logger.debug("Adding condition %s from class %s" % (condition, cls.__name__))
        if cls.__name__ not in cls._db_conditions:
            cls._db_conditions[cls.__name__] = []
        cls._db_conditions[cls.__name__].append(condition)

    @classmethod
    def _output_db_condition(cls):
        logger.debug("Getting conditions for class %s" % cls.__name__)
        items = deepcopy(cls._db_conditions.pop(cls.__name__, []))
        if not items:
            logger.debug("Conditions is empty")
            return None
        cond_len = len(items)
        logger.debug("Conditions contains %s items" % cond_len)
        if cond_len == 1:
            output = next(iter(items))
        else:
            output = functools.reduce(operator.iand, items)

        return output

    @classmethod
    def get(
        cls,
        hash_key=None,
        range_key=None,
        consistent_read=False,
        attributes_to_get=None,
    ):
        hash_key = hash_key or os.environ.get("HASH_KEY", None)
        item = super(DbModel, cls).get(
            hash_key, range_key, consistent_read, attributes_to_get
        )
        return item

    def save(self, condition=None, overwrite=False):
        if not overwrite:
            self.add_db_conditions(self._hash_key.does_not_exist())
        if condition is not None:
            self.add_db_conditions(condition)
        return super(DbModel, self).save(self.__class__._output_db_condition())

    def update(self, actions, condition=None, overwrite=False):
        if not overwrite:
            self.add_db_conditions(self._hash_key.exists())
        if condition is not None:
            self.add_db_conditions(condition)
        return super(DbModel, self).update(
            actions, self.__class__._output_db_condition()
        )

    def delete(self, condition=None):
        self.add_db_conditions(self._hash_key.exists())
        if condition is not None:
            self.add_db_conditions(condition)
        return super(DbModel, self).delete(self.__class__._output_db_condition())

    @classmethod
    def save_attributes(cls, item, **kwargs):
        item.update(kwargs)
        c = cls(**item)
        now = datetime_utc()
        c.created_on = now
        c.updated_on = now
        return deepcopy(c)

    @classmethod
    def put_item(cls, item, overwrite=False, **kwargs):
        cls_obj = cls.save_attributes(item, **kwargs)
        hash_key_name = cls_obj._hash_key.attr_name
        if hash_key_name not in item:
            if "HASH_KEY" in os.environ:
                setattr(cls_obj, hash_key_name, os.environ["HASH_KEY"])
        cls_obj.save(overwrite=overwrite)
        return cls_obj

    @classmethod
    def deletes(cls, attr, value=None):
        """remove field from item or delete item from set"""
        if isinstance(attr, str):
            attr = operator.attrgetter(attr)(cls)
        return attr.delete(value) if value else attr.remove()

    @classmethod
    def add(cls, attr, value):
        """increment or decrement value number or add to set"""
        if isinstance(attr, str):
            attr = operator.attrgetter(attr)(cls)
        return attr.add(value)

    @classmethod
    def append_or_prepend(cls, attr, value, choice="append"):
        """append_or_prepend to list"""
        if isinstance(attr, str):
            attr = operator.attrgetter(attr)(cls)
        return attr.set(getattr((attr | []), choice)(value))

    @classmethod
    def update_attributes(
        cls,
        updates: Optional[Dict[Any, Any]] = None,
        deletes: Optional[Union[List[Any], Dict[Any, Any]]] = None,
        adds: Optional[Dict[Any, Any]] = None,
        appends: Optional[Dict[Any, Any]] = None,
        prepends: Optional[Dict[Any, Any]] = None,
        actions=None,
    ):
        updates = updates or {}
        adds = adds or {}
        appends = appends or {}
        prepends = prepends or {}
        actions = actions or []
        for k, v in updates.items():
            try:
                actions.append(operator.attrgetter(k)(cls).set(v)) if isinstance(
                    k, str
                ) else k.set(v)
            except AttributeError:
                key = str(k.split(".")[-1])
                k = k.replace("." + key, "")
                actions.append(operator.attrgetter(k)(cls)[key].set(v))
        for k, v in adds.items():
            actions.append(cls.add(k, v))
        for k, v in appends.items():
            actions.append(cls.append_or_prepend(k, v))
        for k, v in prepends.items():
            actions.append(cls.append_or_prepend(k, v, "prepend"))
        if deletes:
            if isinstance(deletes, dict):
                for k, v in deletes.items():
                    actions.append(cls.deletes(k, v))
            else:
                for k in deletes:
                    actions.append(cls.deletes(k))
        actions.append(cls.updated_on.set(datetime_utc()))
        return actions

    @classmethod
    def update_item(
        cls,
        hash_key=None,
        range_key=None,
        updates: Optional[Dict[Any, Any]] = None,
        deletes: Optional[List[Any]] = None,
        adds: Optional[Dict[Any, Any]] = None,
        appends: Optional[Dict[Any, Any]] = None,
        prepends: Optional[Dict[Any, Any]] = None,
        actions=None,
        overwrite=False,
    ):
        hash_key = hash_key or os.environ.get("HASH_KEY", None)
        cls_obj = cls(hash_key, range_key)
        cls_obj.update(
            cls.update_attributes(updates, deletes, adds, appends, prepends, actions),
            overwrite=overwrite,
        )
        return cls_obj

    @classmethod
    def delete_item(cls, hash_key=None, range_key=None):
        hash_key = hash_key or os.environ.get("HASH_KEY", None)
        cls_obj = cls(hash_key, range_key)
        cls_obj.delete()
        return cls_obj

    @classmethod
    def query(
        cls,
        hash_key=None,
        range_key_condition=None,
        consistent_read=False,
        index_name=None,
        scan_index_forward=None,
        limit=None,
        last_evaluated_key=None,
        attributes_to_get=None,
        page_size=None,
        rate_limit=None,
        **filters,
    ):
        hash_key = hash_key or os.environ.get("HASH_KEY", None)
        items = super(DbModel, cls).query(
            hash_key=hash_key,
            range_key_condition=range_key_condition,
            filter_condition=cls._output_db_condition(),
            consistent_read=consistent_read,
            index_name=index_name,
            scan_index_forward=scan_index_forward,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
            attributes_to_get=attributes_to_get,
            page_size=page_size,
            rate_limit=rate_limit,
        )
        return items


class TransactWrite(_TransactWrite):
    def save(self, model, condition=None, return_values=None, **kwargs):
        key_name = model._hash_keyname
        overwrite = kwargs.get("overwrite", False)
        hash_key = getattr(model.__class__, key_name)
        if not overwrite:
            model.add_db_conditions(hash_key.does_not_exist())
        if condition is not None:
            model.add_db_conditions(condition)
        condition = model._output_db_condition()
        # set hash key if missing
        if not getattr(model, key_name):
            if "HASH_KEY" in os.environ:
                setattr(model, key_name, os.environ["HASH_KEY"])
        return super(TransactWrite, self).save(model, condition, return_values)

    def update(self, model, actions, condition=None, return_values=None, **kwargs):
        key_name = model._hash_keyname
        overwrite = kwargs.get("overwrite", False)
        hash_key = getattr(model.__class__, key_name)
        if not overwrite:
            model.add_db_conditions(hash_key.exists())
        if condition is not None:
            model.add_db_conditions(condition)
        condition = model._output_db_condition()
        # set hash key if missing
        if not getattr(model, key_name):
            if "HASH_KEY" in os.environ:
                setattr(model, key_name, os.environ["HASH_KEY"])
        return super(TransactWrite, self).update(
            model, actions, condition, return_values
        )

    def delete(self, model, condition=None):
        key_name = model._hash_keyname
        hash_key = getattr(model.__class__, key_name)
        model.add_db_conditions(hash_key.exists())
        if condition is not None:
            model.add_db_conditions(condition)
        condition = model._output_db_condition()
        # set hash key if missing
        if not getattr(model, key_name):
            if "HASH_KEY" in os.environ:
                setattr(model, key_name, os.environ["HASH_KEY"])
        super(TransactWrite, self).delete(model, condition)

    def condition_check(self, model_cls, hash_key=None, range_key=None, condition=None):
        # set hash key if missing
        if hash_key is None:
            hash_key = os.environ["HASH_KEY"]

        if condition is not None:
            model_cls.add_db_conditions(condition)
        condition = model_cls._output_db_condition()

        if condition is None:
            return

        return super(TransactWrite, self).condition_check(
            model_cls, hash_key, range_key, condition
        )
