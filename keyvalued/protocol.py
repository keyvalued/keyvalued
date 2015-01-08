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

from .expiringdict import ExpiringDict

import simplejson as json
import sys
import time
import os

indexes = dict()

class Client(object):
    def __init__(self, transport):
        self.transport = transport

    def lock(self, index, key, token, max_len=256000, max_age=600):
        idx = indexes.get(index, ExpiringDict(max_len=max_len, max_age_seconds=max_age))
        indexes[index] = idx

        ldict = idx.get('_locks', ExpiringDict(max_len=256000, max_age_seconds=10))
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
        indexes[index] = idx

        ldict = idx.get('_locks', ExpiringDict(max_len=256000, max_age_seconds=10))
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
        self.transport.send(bytes(json.dumps(message) + "\r\n", 'UTF-8'))
        self.transport.close()
        return True

    def error(self, error_message):
        self.reply({'error': error_message})
        return False
