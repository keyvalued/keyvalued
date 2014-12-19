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

    # rough idea:
    # > {'index': '20141217.hits', 'key': 12}
    # < {'index': '20141217.hits', 'key': 12, '_source': {'count': 12}}
    def data_received(self, data):
        o = json.loads(data.decode('UTF-8', 'replace').strip('\r\n'))
        if not o['key'] or not o['index']:
            return self.error('no index or key')
        if o.get('_source', None):
            idx = indexes.get(o['index'], ExpiringDict(max_len=o.get('index.max_len', 256000), max_age_seconds=o.get('index.max_age', 600)))
            idx.put(o['key'], o['_source'], time.time() + float(o.get('_expiry', 600)))
            indexes[o['index']] = idx
            return self.reply({'index': o['index'], 'key': o['key'], '_source': o['_source'], '_version': 1})

        idx = indexes.get(o['index'], None)
        if not idx:
            return self.error('Index not found')

        obj = idx.get(o['key'])
        return self.reply({'index': o['index'], 'key': o['key'], '_source': obj, '_version': 1})

    def reply(self, message):
        self.transport.write(bytes(json.dumps(message) + "\r\n", 'UTF-8'))
        self.transport.close()
        return True

    def error(self, error_message):
        self.reply({'error': error_message})
        return False

def main():
    os.unlink('/tmp/keyvalued.sock')

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
