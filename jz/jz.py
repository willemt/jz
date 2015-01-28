#!/usr/bin/env python

"""jz, the JSON database.

Usage:
  jz.py [-p <port_num> -w <count> -d -r -z]
  jz.py stop
  jz.py (-h | --help)
  jz.py --version

Options:
  -h --help                  Show this screen
  --version                  Show version
  -w, --workers <count>      Number of workers [default: 1]
  -p, --port <port_num>      Port to listen on [default: 8888]
  -r, --autoreload           Autoreload when source code changes
  -d, --debug                Enable debugging
  -z, --daemonize            Daemonize

jz.py stop kills the jz daemon

"""
from __future__ import print_function
from docopt import docopt
from multiprocessing import reduction, Pipe, Process
import atexit
import os
import random
import signal
import socket
import sys
import traceback
try:
    from http_parser.parser import HttpParser
except ImportError:
    from http_parser.pyparser import HttpParser
from http_parser.http import HttpStream, NoMoreData
from http_parser.reader import SocketReader

from queryplan import QueryPlan
import storage
import monitor


VERSION = "0.0.1"


class Request(object):
    def __init__(self, socket):
        self.socket = socket

    @property
    def method(self):
        return self.parser.method()

    @property
    def body(self):
        return self.parser.body_file(binary=True).read()

    def get_environ(self):
        return {
            'ACTUAL_SERVER_PROTOCOL': 'HTTP/1.0',
            'PATH_INFO': self.parser.path(),
            'QUERY_STRING': self.parser.query_string(),
            'REMOTE_ADDR': '',
            'REMOTE_PORT': '',
            'REQUEST_METHOD': self.method,
            'REQUEST_URI': '',
            'SCRIPT_NAME': '',
            'SERVER_NAME': '',
            'SERVER_PROTOCOL': '',
            'SERVER_SOFTWARE': '',
            'wsgi.errors': sys.stderr,
            'wsgi.input': self.parser.body_file(binary=True),
            'wsgi.multiprocess': False,
            'wsgi.multithread': True,
            'wsgi.run_once': False,
            'wsgi.url_scheme': '',
            'wsgi.version': (1, 0),
        }


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
            r = Request(socket=s)
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
                print(traceback.format_exc())

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
    if 0 == len(server.workers):
        w = Worker(server)
        w.http_request(Request(socket=s))
    else:
        child = server.workers[random.randint(0, len(server.workers)-1)]
        reduction.send_handle(child.pipe_parent, s.fileno(), child.ch.pid)


def run(self):
    if args['--debug']:
        # gevent's monkey patch messes pudb up
        from streamserver import StreamServer
        # keep it single threaded
        args['--workers'] = 0
    else:
        from gevent.server import StreamServer

    server.num_workers = int(args['--workers'])
    server.run()
    s = StreamServer(('0.0.0.0', int(args['--port'])), entry)
    s.serve_forever()


@atexit.register
def goodbye():
    """
    Kill children
    """
    for child in server.workers:
        os.kill(child.ch.pid, signal.SIGKILL)


if __name__ == '__main__':
    args = docopt(__doc__, version=VERSION)

    if args['--autoreload']:
        monitor.start(interval=1.0)
        monitor.track(os.path.join(os.path.dirname(__file__), 'site.cf'))

    if args['--daemonize']:
        if os.getuid() != 0:
            print("I need to be root")
            exit()

        from pep3143daemon import DaemonContext
        from pep3143daemon import PidFile

        pid = '/var/run/jz.pid'
        pidfile = PidFile(pid)
        daemon = DaemonContext(pidfile=pidfile)

        print('pidfile is: {0}'.format(pid))
        print('daemonizing...')

        daemon.open()

        run(args)
    else:
        run(args)
