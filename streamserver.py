import SocketServer


class Handler(SocketServer.BaseRequestHandler):
    def handle(self):
        self.server.func(self.request, None)


class StreamServer(object):
    """ Mimic gevent.server.StreamServer """

    def __init__(self, binding, func, *args, **kwargs):
        self.binding = binding
        self.func = func

    def serve_forever(self):
        server = SocketServer.TCPServer(self.binding, Handler)
        server.func = self.func
        server.serve_forever()
