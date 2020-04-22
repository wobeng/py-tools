import operator
from datetime import datetime
from functools import reduce

import simplejson


class ModelEncoder(simplejson.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, set):
            return list(obj)
        return simplejson.JSONEncoder.default(self, obj)


def loads(*args, **kwargs):
    return simplejson.loads(*args, **kwargs)


def dumps(*args, **kwargs):
    kwargs['cls'] = kwargs.get('cls', ModelEncoder)
    return simplejson.dumps(*args, **kwargs)


def clean_empty(d):
    if not isinstance(d, (dict, list)):
        return d
    if isinstance(d, list):
        return [v for v in (clean_empty(v) for v in d) if v]
    return {k: v for k, v in ((k, clean_empty(v)) for k, v in d.items()) if v != ''}


class FormatData:
    def __init__(self, item):
        self.output = {}
        self.item = dict(item)

    @staticmethod
    def get_by_path(item, path_list):
        return reduce(operator.getitem, path_list, item)

    @staticmethod
    def set_by_path(item, path_list, value):
        FormatData.get_by_path(item, path_list[:-1])[path_list[-1]] = value

    @staticmethod
    def delete_in_dict(item, path_list):
        del FormatData.get_by_path(item, path_list[:-1])[path_list[-1]]

    def inc(self, keys):
        for k in keys:
            try:
                value = self.get_by_path(self.item, k.split('.'))
                FormatData.set_by_path(self.output, k.split('.'), value)
            except KeyError as e:
                continue
        return self.output

    def exc(self, keys):
        for k in keys:
            try:
                FormatData.delete_in_dict(self.item, k.split('.'))
            except KeyError as e:
                continue
        return self.item
