#!/usr/bin/env python

"""jz, the JSON database.

Usage:
  jz.py [-p <port_num> -w <count> -r]
  jz.py (-h | --help)
  jz.py --version

Options:
  -h --help                  Show this screen
  --version                  Show version
  -w, --workers <count>      Number of workers [default: 4]
  -p, --port <port_num>      Port to listen on [default: 8888]
  -r, --autoreload           Autoreload when source code changes

"""
from __future__ import print_function
from docopt import docopt
from gevent.server import StreamServer
import os
import random
import socket
from multiprocessing import reduction, Pipe, Process
try:
    from http_parser.parser import HttpParser
except ImportError:
    from http_parser.pyparser import HttpParser
from http_parser.http import HttpStream, NoMoreData
from http_parser.reader import SocketReader
from cherrypy.wsgiserver import HTTPRequest

from queryplan import QueryPlan
import storage
import monitor

VERSION = "0.0.1"


class Request(object):
    pass


class ReplaySocket(object):
    def __init__(self, sock):
        self.sock = sock

    def read(self, size):
        self.sock.read(size)


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
        elif '/test/' == path:
            pass
            
            class FakeServer(object):
                pass

            class FakeConn(object):
                def __init__(self, rfile, wfile):
                    self.rfile = rfile
                    self.wfile = wfile

            server = FakeServer()
            server.max_request_header_size = 100000
            server.max_request_body_size = 100000
            server.ssl_adapter = None
            server.protocol = 'HTTP/1.1'
            server.gateway = Gateway
            conn = FakeConn(r.socket)
            req = HTTPRequest(server, conn)
            req.parse_request()

        else:
            self.server.storage.put(r.parser.body_file(binary=True).read())
            self.data_response(r, '')


class Gateway(object):

    """A base class to interface HTTPServer with other systems, such as WSGI.
    """

    def __init__(self, req):
        self.req = req

    def respond(self):
        """Process the current request. Must be overridden in a subclass."""
        raise NotImplemented


class Server(object):
    def __init__(self):
        self.workers = []
        self.storage = storage.Storage()

    def run(self):
        # Spawn workers
        for i in range(self.num_workers):
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
    args = docopt(__doc__, version=VERSION)
    print(args)

    if args['--autoreload']:
        monitor.start(interval=1.0)
        monitor.track(os.path.join(os.path.dirname(__file__), 'site.cf'))

    server.num_workers = int(args['--workers'])
    server.run()
    s = StreamServer(('0.0.0.0', int(args['--port'])), entry)
    s.serve_forever()
