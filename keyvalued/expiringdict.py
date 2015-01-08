# Copyright (c) 2014, William Pitcock <nenolod@dereferenced.org>
#
# Permission to use, copy, modify, and/or distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

from collections import OrderedDict

import time

class ExpiringDict(OrderedDict):
    def __init__(self, max_len, max_age_seconds):
        OrderedDict.__init__(self)
        self.max_len = max_len
        self.max_age = max_age_seconds

    def __contains__(self, key):
        try:
            item = OrderedDict.__getitem__(self, key)
            if time.time() - item[1] < self.max_age:
                return True
            else:
                del self[key]
        except KeyError:
            pass
        return False

    def __getitem__(self, key, with_age=False, max_age=None):
        item = OrderedDict.__getitem__(self, key)
        item_age = time.time() - item[1]
        if not max_age:
            max_age = self.max_age
        if item_age < max_age:
            if with_age:
                return item[0], item_age
            else:
                return item[0]
        else:
            del self[key]
            raise KeyError(key)

    def __setitem__(self, key, value):
        if len(self) == self.max_len:
            self.popitem(last=False)
        OrderedDict.__setitem__(self, key, (value, time.time()))

    def pop(self, key, default=None):
        try:
            item = OrderedDict.__getitem__(self, key)
            del self[key]
            return item[0]
        except KeyError:
            return default

    def get(self, key, default=None, with_age=False, max_age=None):
        try:
            return self.__getitem__(key, with_age, max_age)
        except KeyError:
            if with_age:
                return default, None
            else:
                return default

    def put(self, key, value, ts=None):
        if len(self) == self.max_len:
            self.popitem(last=False)
        if not ts:
            ts = time.time()
        OrderedDict.__setitem__(self, key, (value, ts))

    def items(self):
        r = []
        for key in self:
            try:
                r.append((key, self[key]))
            except KeyError:
                pass
        return r

    def values(self):
        r = []
        for key in self:
            try:
                r.append(self[key])
            except KeyError:
                pass
        return r

    def fromkeys(self):
        raise NotImplementedError()
    def iteritems(self):
        raise NotImplementedError()
    def itervalues(self):
        raise NotImplementedError()
    def viewitems(self):
        raise NotImplementedError()
    def viewkeys(self):
        raise NotImplementedError()
    def viewvalues(self):
        raise NotImplementedError()
