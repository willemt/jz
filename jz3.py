#!/usr/bin/env python

from __future__ import print_function
from gevent.server import StreamServer

import random
import lmdb
import struct

try:
    from http_parser.parser import HttpParser
except ImportError:
    from http_parser.pyparser import HttpParser


from http_parser.http import HttpStream, NoMoreData
from http_parser.reader import SocketReader


def open_db():
    db_path = 'db'
    MAP_SIZE = 1048576 * 400
    return lmdb.open(db_path, map_size=MAP_SIZE)


def http_read(s, env):
    while True:
        try:
            p = HttpStream(SocketReader(s))
            p.headers()

            key = random.getrandbits(64)
            key = struct.pack('L', key)

            with env.begin(write=True, buffers=True) as txn:
                txn.put(key, p.body_file(binary=True).read())

            s.sendall(
                'HTTP/1.1 200 OK\n'
                'Cache-Control: no-cache\n'
                'Content-Language: en-us\n'
                'Content-Type: application/json\n'
                'Content-Length: 0\n'
                'Server: jz/0.1.0\n\r\n\r\n')
        except NoMoreData:
            break


class Server(object):
    def __init__(self):
        pass


server = Server()


def echo(s, address):
    http_read(s, env)


if __name__ == '__main__':
    env = open_db()
    s = StreamServer(('0.0.0.0', 8888), echo)
    s.serve_forever()
