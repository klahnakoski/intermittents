# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from collections import Mapping
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import unwrap, tuplewrap
from pyLibrary.dot.objects import dictwrap


class UniqueIndex(object):
    """
    DEFINE A SET OF ATTRIBUTES THAT UNIQUELY IDENTIFIES EACH OBJECT IN A list.
    THIS ALLOWS set-LIKE COMPARISIONS (UNION, INTERSECTION, DIFFERENCE, ETC) WHILE
    STILL MAINTAINING list-LIKE FEATURES
    """

    def __init__(self, keys, data=None, fail_on_dup=True):
        self._data = {}
        self._keys = tuplewrap(keys)
        self.count = 0
        self.fail_on_dup = fail_on_dup
        if data:
            for d in data:
                self.add(d)

    def __getitem__(self, key):
        try:
            key = value2key(self._keys, key)
            d = self._data.get(key)
            return dictwrap(d)
        except Exception, e:
            Log.error("something went wrong", e)

    def __setitem__(self, key, value):
        Log.error("Use add() to ad to an index")
        # try:
        #     key = value2key(self._keys, key)
        #     d = self._data.get(key)
        #     if d != None:
        #         Log.error("key already filled")
        #     self._data[key] = unwrap(value)
        #     self.count += 1
        #
        # except Exception, e:
        #     Log.error("something went wrong", e)

    def keys(self):
        return self._data.keys()

    def add(self, val):
        val = dictwrap(val)
        key = value2key(self._keys, val)
        if key == None:
            Log.error("Expecting key to not be None")

        d = self._data.get(key)
        if d is None:
            self._data[key] = unwrap(val)
            self.count += 1
        elif d is not val:
            if self.fail_on_dup:
                Log.error("key {{key|json}} already filled",  key=key)
            else:
                Log.warning("key {{key|json}} already filled\nExisting\n{{existing|json|indent}}\nValue\n{{value|json|indent}}",
                    key=key,
                    existing=d,
                    value=val
                )

    def remove(self, val):
        key = value2key(self._keys, dictwrap(val))
        if key == None:
            Log.error("Expecting key to not be None")

        d = self._data.get(key)
        if d is None:
            # ALREADY GONE
            return
        else:
            del self._data[key]
            self.count -= 1

    def __contains__(self, key):
        return self[key] != None

    def __iter__(self):
        return (dictwrap(v) for v in self._data.itervalues())

    def __sub__(self, other):
        output = UniqueIndex(self._keys)
        for v in self:
            if v not in other:
                output.add(v)
        return output

    def __and__(self, other):
        output = UniqueIndex(self._keys)
        for v in self:
            if v in other:
                output.add(v)
        return output

    def __or__(self, other):
        output = UniqueIndex(self._keys)
        for v in self:
            output.add(v)
        for v in other:
            try:
                output.add(v)
            except Exception, e:
                pass
        return output

    def __len__(self):
        if self.count == 0:
            for d in self:
                self.count += 1
        return self.count

    def subtract(self, other):
        return self.__sub__(other)

    def intersect(self, other):
        return self.__and__(other)

def value2key(keys, val):
    if len(keys)==1:
        if isinstance(val, Mapping):
            return val[keys[0]]
        elif isinstance(val, (list, tuple)):
            return val[0]
        else:
            return val
    else:
        if isinstance(val, Mapping):
            return dictwrap({k: val[k] for k in keys})
        elif isinstance(val, (list, tuple)):
            return dictwrap(dict(zip(keys, val)))
        else:
            Log.error("do not know what to do here")
