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

import simplejson as json
import asyncio
import sys
import time
import os

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

indexes = dict()

class Client(asyncio.Protocol):
    def connection_made(self, transport):
        self.transport = transport

    def lock(self, index, key, token, max_len=256000, max_age=600):
        idx = indexes.get(index, ExpiringDict(max_len=max_len, max_age_seconds=max_age))

        ldict = idx.get('_locks', dict())
        lock = None

        if key in ldict:
            lock = ldict[key]
        if not lock and token:
            lock = token
        if not lock:
            return None

        ldict[key] = lock
        indexes[index]['_locks'] = ldict

        return lock

    def unlock(self, index, key, token, max_len=256000, max_age=600):
        idx = indexes.get(index, ExpiringDict(max_len=max_len, max_age_seconds=max_age))

        ldict = idx.get('_locks', dict())
        lock = None

        if key in ldict:
            lock = ldict[key]
        if not lock:
            return False

        if lock == token:
            ldict.pop(key)
        else:
            return False

        indexes[index]['_locks'] = ldict
        return True

    def lookup(self, index, key):
        idx = indexes.get(index, None)
        if not idx:
            return self.error('Index not found')

        lock = self.lock(index, key, None)
        if lock:
            return self.reply({'index': index, 'key': key, '_locked': lock})

        obj = idx.get(key)
        return self.reply({'index': index, 'key': key, '_source': obj, '_version': 1})

    def index(self, index, key, obj, expiry=600, max_len=256000, max_age=600):
        idx = indexes.get(index, ExpiringDict(max_len=max_len, max_age_seconds=max_age))
        idx.put(key, obj, time.time() + expiry)
        indexes[index] = idx
        return self.reply({'index': index, 'key': key, '_source': obj, '_version': 1})

    def r_lock_op(self, index, key, token):
        l = self.lock(index, key, token)
        if l != token:
            return self.reply({'index': index, 'key': key, '_locked': l})

        return self.reply({'index': index, 'key': key, 'locked': True, 'token': l})

    def r_unlock_op(self, index, key, token):
        s = self.unlock(index, key, token)
        return self.reply({'index': index, 'key': key, 'unlocked': s})

    # rough idea for get/index ops:
    # > {'index': '20141217.hits', 'key': 12}
    # < {'index': '20141217.hits', 'key': 12, '_source': {'count': 12}}
    def handle_get_or_index(self, o):
        if not o['key'] or not o['index']:
            return self.error('no index or key')

        if o.get('_source', None):
            return self.index(o['index'], o['key'], o['_source'], expiry=o.get('_expiry', 600), max_len=o.get('index.max_len', 256000), max_age=o.get('index.max_age', 600))

        return self.lookup(o['index'], o['key'])

    def data_received(self, data):
        o = json.loads(data.decode('UTF-8', 'replace').strip('\r\n'))

        act = o.get('_action', None)
        if not act:
            return self.handle_get_or_index(o)

        if act == 'r_lock':
            return self.r_lock_op(o['index'], o['key'], o['token'])

        if act == 'r_unlock':
            return self.r_unlock_op(o['index'], o['key'], o['token'])

        return self.error('Unknown action requested')

    def reply(self, message):
        self.transport.write(bytes(json.dumps(message) + "\r\n", 'UTF-8'))
        self.transport.close()
        return True

    def error(self, error_message):
        self.reply({'error': error_message})
        return False

def main():
    try:
        os.unlink('/tmp/keyvalued.sock')
    except:
        pass

    loop = asyncio.get_event_loop()
    coro = loop.create_unix_server(Client, '/tmp/keyvalued.sock', backlog=65535)
    server = loop.run_until_complete(coro)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()

if __name__ == '__main__':
    main()
