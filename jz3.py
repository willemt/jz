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


from queryplan import QueryPlan


def open_db():
    db_path = 'db'
    MAP_SIZE = 1048576 * 400
    return lmdb.open(db_path, map_size=MAP_SIZE)



def http_get(s, env, p):
    body = p.body_file(binary=True).read()

    print("get")

    for op in QueryPlan(body).ops():
        print("op")
        print(op.column)
        print(op.target)


    #with env.begin(write=True, buffers=True) as txn:
    #    txn.put(key, )

    s.sendall(
        'HTTP/1.1 200 OK\n'
        'Cache-Control: no-cache\n'
        'Content-Language: en-us\n'
        'Content-Type: application/json\n'
        'Content-Length: 0\n'
        'Server: jz/0.1.0\n\r\n\r\n')


def http_post(s, env, p):
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


def http_request(s, env):
    while True:
        try:
            p = HttpStream(SocketReader(s))
            p.headers()
            if 'GET' == p.method():
                http_get(s, env, p)
            elif 'POST' == p.method():
                http_post(s, env, p)
        except NoMoreData:
            break


def worker(conn):
    env = open_db()
    #time.sleep(.5)
    conn.poll(None)
    while True:
        s = socket.fromfd(reduction.recv_handle(conn), socket.AF_INET, socket.SOCK_STREAM)
        s.setblocking(1)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
        http_request(s, env)


class Server(object):
    def __init__(self):
        self.children = []

        class Child(object):
            pass

        for i in range(2):
            c = Child()
            c.pipe_parent, c.pipe_child = multiprocessing.Pipe()
            c.ch = multiprocessing.Process(target=worker, args=(c.pipe_child,))
            c.ch.start()
            self.children.append(c)


server = Server()


def echo(s, address):
    #print('New connection from %s:%s' % address)
    #s.sendall('Welcome to the echo server! Type quit to exit.\r\n')
    #http_read(s)
    child = server.children[random.randint(0, len(server.children)-1)]
    reduction.send_handle(child.pipe_parent, s.fileno(), child.ch.pid)


if __name__ == '__main__':
    env = open_db()
    s = StreamServer(('0.0.0.0', 8888), echo)
    s.serve_forever()
