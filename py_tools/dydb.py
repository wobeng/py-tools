import functools
import operator
import os
from copy import deepcopy
from datetime import datetime
from typing import Any, Dict, List, Optional, Text, TypeVar, Union, Tuple

from pynamodb.attributes import UTCDateTimeAttribute
from pynamodb.constants import KEY
from pynamodb.exceptions import DoesNotExist
from pynamodb.expressions.condition import Condition
from pynamodb.models import Model
from pynamodb.transactions import TransactWrite as _TransactWrite

from py_tools import format

_T = TypeVar('_T', bound='Model')

KeyType = Union[Text, bytes, float, int, Tuple]
ModelType = TypeVar('ModelType', bound=Model)


def get_operation_kwargs_from_instance(self,
                                       key=KEY,
                                       actions=None,
                                       condition=None,
                                       return_values_on_condition_failure=None):
    is_update = actions is not None
    is_delete = actions is None and key is KEY
    null_check = not (is_update or is_delete)
    args, save_kwargs = self._get_save_args(attributes=(not is_delete), null_check=null_check)

    version_condition = self._handle_version_attribute(
        serialized_attributes={} if is_delete else save_kwargs,
        actions=actions
    )
    if version_condition is not None:
        condition &= version_condition

    kwargs = dict(
        key=key,
        actions=actions,
        condition=condition,
        return_values_on_condition_failure=return_values_on_condition_failure
    )
    if not is_update:
        kwargs.update(save_kwargs)
    elif 'range_key' in save_kwargs:
        kwargs['range_key'] = save_kwargs['range_key']
    return self._get_connection().get_operation_kwargs(*args, **kwargs)


Model.get_operation_kwargs_from_instance = get_operation_kwargs_from_instance


class ModelEncoder(format.ModelEncoder):
    def default(self, obj):
        if hasattr(obj, 'attribute_values'):
            return obj.attribute_values
        return super(ModelEncoder, self).default(obj)


class DbModel(Model):
    _db_conditions = []
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
            raise DoesNotExist()
        return items[0]

    @property
    def key(self):
        key = {self._hash_keyname: getattr(self, self._hash_keyname)}
        if self._range_keyname:
            key[self._range_keyname] = getattr(self, self._range_keyname)
        return key

    @staticmethod
    def add_db_conditions(condition: Optional[Any]):
        DbModel._db_conditions.append(condition)

    @staticmethod
    def db_filter_conditions(condition: Optional[Condition]):
        DbModel._db_conditions.append(condition)

    @staticmethod
    def _output_db_condition():
        if not DbModel._db_conditions:
            return
        if len(DbModel._db_conditions) == 1:
            output = DbModel._db_conditions[0]
        else:
            output = functools.reduce(operator.iand, DbModel._db_conditions)
        DbModel._db_conditions = []
        return output

    @classmethod
    def get(cls, hash_key=None, range_key=None, consistent_read=False, attributes_to_get=None):
        hash_key = hash_key or os.environ.get('HASH_KEY', None)
        item = super(DbModel, cls).get(hash_key, range_key, consistent_read, attributes_to_get)
        return item

    def save(self, condition=None):
        self.add_db_conditions(self._hash_key.does_not_exist())
        if condition is not None:
            self.add_db_conditions(condition)
        return super(DbModel, self).save(DbModel._output_db_condition())

    def update(self, actions, condition=None):
        self.add_db_conditions(self._hash_key.exists())
        if condition is not None:
            self.add_db_conditions(condition)
        return super(DbModel, self).update(actions, DbModel._output_db_condition())

    def delete(self, condition=None):
        self.add_db_conditions(self._hash_key.exists())
        if condition is not None:
            self.add_db_conditions(condition)
        return super(DbModel, self).delete(DbModel._output_db_condition())

    @classmethod
    def save_attributes(cls, item, **kwargs):
        item.update(kwargs)
        c = cls(**item)
        now = datetime.utcnow()
        c.created_on = now
        c.updated_on = now
        return deepcopy(c)

    @classmethod
    def put_item(cls, item, **kwargs):
        cls_obj = cls.save_attributes(item, **kwargs)
        hash_key_name = cls_obj._hash_key.attr_name
        if hash_key_name not in item:
            if 'HASH_KEY' in os.environ:
                setattr(cls_obj, hash_key_name, os.environ['HASH_KEY'])
        cls_obj.save()
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
        return attr.set(attr + value) if isinstance(value, int) else attr.add(value)

    @classmethod
    def append_or_prepend(cls, attr, value, choice='append'):
        """append_or_prepend to list"""
        if isinstance(attr, str):
            attr = operator.attrgetter(attr)(cls)
        return attr.set(getattr((attr | []), choice)(value))

    @classmethod
    def update_attributes(cls, updates: Optional[Dict[Any, Any]] = None,
                          deletes: Optional[Union[List[Any], Dict[Any, Any]]] = None,
                          adds: Optional[Dict[Any, Any]] = None,
                          appends: Optional[Dict[Any, Any]] = None, prepends: Optional[Dict[Any, Any]] = None,
                          actions=None):
        updates = updates or {}
        adds = adds or {}
        appends = appends or {}
        prepends = prepends or {}
        actions = actions or []
        for k, v in updates.items():
            actions.append(operator.attrgetter(k)(cls).set(v)) if isinstance(k, str) else k.set(v)
        for k, v in adds.items():
            actions.append(cls.add(k, v))
        for k, v in appends.items():
            actions.append(cls.append_or_prepend(k, v))
        for k, v in prepends.items():
            actions.append(cls.append_or_prepend(k, v, 'prepend'))
        if deletes:
            if isinstance(deletes, dict):
                for k, v in deletes.items():
                    actions.append(cls.deletes(k, v))
            else:
                for k in deletes:
                    actions.append(cls.deletes(k))
        actions.append(cls.updated_on.set(datetime.utcnow()))
        return actions

    @classmethod
    def update_item(cls, hash_key=None, range_key=None,
                    updates: Optional[Dict[Any, Any]] = None,
                    deletes: Optional[List[Any]] = None, adds: Optional[Dict[Any, Any]] = None,
                    appends: Optional[Dict[Any, Any]] = None,
                    prepends: Optional[Dict[Any, Any]] = None,
                    actions=None):
        hash_key = hash_key or os.environ.get('HASH_KEY', None)
        cls_obj = cls(hash_key, range_key)
        cls_obj.update(cls.update_attributes(updates, deletes, adds, appends, prepends, actions))
        return cls_obj

    @classmethod
    def delete_item(cls, hash_key=None, range_key=None):
        hash_key = hash_key or os.environ.get('HASH_KEY', None)
        cls_obj = cls(hash_key, range_key)
        cls_obj.delete()
        return cls_obj

    @classmethod
    def query(cls,
              hash_key=None,
              range_key_condition=None,
              consistent_read=False,
              index_name=None,
              scan_index_forward=None,
              conditional_operator=None,
              limit=None,
              last_evaluated_key=None,
              attributes_to_get=None,
              page_size=None,
              rate_limit=None,
              **filters):
        hash_key = hash_key or os.environ.get('HASH_KEY', None)
        items = super(DbModel, cls).query(
            hash_key=hash_key, range_key_condition=range_key_condition,
            filter_condition=DbModel._output_db_condition(),
            consistent_read=consistent_read, index_name=index_name,
            scan_index_forward=scan_index_forward, limit=limit,
            last_evaluated_key=last_evaluated_key, attributes_to_get=attributes_to_get,
            page_size=page_size, rate_limit=rate_limit
        )
        return items


class TransactWrite(_TransactWrite):

    def save(self, model, condition=None, return_values=None, **kwargs):
        hash_key = getattr(model.__class__, model._hash_keyname)
        overwrite = kwargs.get('overwrite', False)
        if condition is not None:
            condition = condition & (hash_key.does_not_exist())
        elif not overwrite:
            condition = hash_key.does_not_exist()
        return super(TransactWrite, self).save(model, condition, return_values)

    def update(self, model, actions, condition=None, return_values=None, **kwargs):
        hash_key = getattr(model.__class__, model._hash_keyname)
        overwrite = kwargs.get('overwrite', False)
        if condition is not None:
            condition = condition & (hash_key.exists())
        elif not overwrite:
            condition = hash_key.exists()
        return super(TransactWrite, self).update(model, actions, condition, return_values)

    def delete(self, model, condition=None, **kwargs):
        hash_key = getattr(model.__class__, model._hash_keyname)
        overwrite = kwargs.get('overwrite', False)
        if condition is not None:
            condition = condition & (hash_key.exists())
        elif not overwrite:
            condition = hash_key.exists()
        super(TransactWrite, self).delete(model, condition)
