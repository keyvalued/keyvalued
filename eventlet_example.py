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

import simplejson as json
import eventlet
import socket

from pprint import pprint

class EventletKeyValueClient(object):
    def __init__(self, path='/tmp/keyvalued.sock'):
        self.path = path

    def index(self, index, key, doc):
        payload = {
            'index': index,
            'key': key,
            '_source': doc
        }
        s = eventlet.connect(self.path, family=socket.AF_UNIX)
        s.send(bytes(json.dumps(payload) + '\r\n', 'UTF-8'))
        result = s.recv(16384)
        return json.loads(result)

    def fetch(self, index, key):
        payload = {
            'index': index,
            'key': key,
        }
        s = eventlet.connect(self.path, family=socket.AF_UNIX)
        s.send(bytes(json.dumps(payload) + '\r\n', 'UTF-8'))
        result = s.recv(16384)
        return json.loads(result)

kv_client = EventletKeyValueClient()
pprint(kv_client.index('test', 'test-1', {'status': 'keyvalued is cool'}))
pprint(kv_client.fetch('test', 'test-1'))
