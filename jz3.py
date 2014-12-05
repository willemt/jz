#!/usr/bin/env python

from __future__ import print_function
from gevent.server import StreamServer

import os
import random
import lmdb
import struct
import socket
import simplejson as json

# try to import C parser then fallback in pure python parser.
try:
    from http_parser.parser import HttpParser
except ImportError:
    from http_parser.pyparser import HttpParser
from http_parser.http import HttpStream, NoMoreData
from http_parser.reader import SocketReader

from multiprocessing import reduction, Pipe, Process

from queryplan import QueryPlan
import storage

import monitor


monitor.start(interval=1.0)
monitor.track(os.path.join(os.path.dirname(__file__), 'site.cf'))


class Request(object):
    pass


class Worker(object):
    @staticmethod
    def entry(conn, w):
        conn.poll(None)
        w.conn = conn
        w.poll()

    def __init__(self, server):
        self.server = server

    def poll(self):
        while True:
            s = socket.fromfd(reduction.recv_handle(self.conn), socket.AF_INET, socket.SOCK_STREAM)
            s.setblocking(1)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
            r = Request()
            r.socket = s
            self.http_request(r)

    def data_response(self, r, data):
        r.socket.sendall(
            'HTTP/1.1 200 OK\n'
            'Content-Language: en-us\n'
            'Content-Type: application/json\n'
            'Content-Length: {len}\n'
            'Server: jz/0.1.0\n\r\n\r\n'
            '{body}\n'.format(len=len(data) + 2, body=data))

    def http_request(self, r):
        while True:
            try:
                r.parser = HttpStream(SocketReader(r.socket))
                r.parser.headers()
                if r.parser.method() == 'GET':
                    self.get(r)
                elif r.parser.method() == 'POST':
                    self.post(r)
            except NoMoreData:
                break
            except Exception as e:
                self.data_response(r, unicode(e))

    def get(self, r):
        body = r.parser.body_file(binary=True).read()
        qp = QueryPlan(body)
        result = qp.build(self.server.storage)
        body = '[' + u','.join([unicode(self.server.storage.get(row[1]))
                                for row in result]) + ']'
        self.data_response(r, body)

    def post(self, r):
        path = r.parser.path()
        if '/user/' == path:
            apikey = os.urandom(18).encode("hex")
            self.data_response(r, apikey)
        elif '/column/' == path:
            self.server.storage.create_column(r.parser.body_file(binary=True).read())
            self.data_response(r, '')
        else:
            self.server.storage.put(r.parser.body_file(binary=True).read())
            self.data_response(r, '')


class Server(object):
    def __init__(self):
        self.workers = []
        self.storage = storage.Storage()

        # Spawn workers
        for i in range(4):
            w = Worker(self)
            w.pipe_parent, w.pipe_child = Pipe()
            w.ch = Process(target=Worker.entry, args=(w.pipe_child, w))
            w.ch.start()
            self.workers.append(w)


server = Server()


def entry(s, address):
    child = server.workers[random.randint(0, len(server.workers)-1)]
    reduction.send_handle(child.pipe_parent, s.fileno(), child.ch.pid)


if __name__ == '__main__':
    s = StreamServer(('0.0.0.0', 8888), entry)
    s.serve_forever()
