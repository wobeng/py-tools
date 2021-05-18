from datetime import datetime

import simplejson
from dotty_dict import Dotty


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
    def __init__(self, item, separator='.'):
        self.output = {}
        self.separator = separator
        self.item = Dotty(item, self.separator)

    def inc(self, keys):
        output = Dotty({}, self.separator)
        for k in keys:
            output[k] = self.item.get(k)
        return dict(output)

    def exc(self, keys):
        for k in keys:
            try:
                del self.item[k]
            except KeyError:
                continue
        return dict(self.item)
