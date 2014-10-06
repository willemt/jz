#!/usr/bin/env python

from __future__ import print_function
from gevent.server import StreamServer

import random
import lmdb
import struct

import socket
import time

# try to import C parser then fallback in pure python parser.
try:
    from http_parser.parser import HttpParser
except ImportError:
    from http_parser.pyparser import HttpParser


from http_parser.http import HttpStream, NoMoreData
from http_parser.reader import SocketReader


env = None


import multiprocessing
from multiprocessing import reduction


def http_read(s):
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


def worker(conn):
    time.sleep(.5)
    conn.poll(None)
    s = socket.fromfd(reduction.recv_handle(conn), socket.AF_INET, socket.SOCK_STREAM)
    http_read(s)


class Server(object):
    def __init__(self):
        self.parent, child = multiprocessing.Pipe()
        self.ch = multiprocessing.Process(target=worker, args=(child,))


server = Server()


def echo(s, address):
    #print('New connection from %s:%s' % address)
    #s.sendall('Welcome to the echo server! Type quit to exit.\r\n')
    http_read(s)
    reduction.send_handle(server.parent, s.fileno(), server.ch.pid)


if __name__ == '__main__':

    db_path = 'db'
    MAP_SIZE = 1048576 * 400
    env = lmdb.open(db_path, map_size=MAP_SIZE)

    s = StreamServer(('0.0.0.0', 8888), echo)
    s.serve_forever()
