import sys
import simplejson as json


class Response(object):
    def __init__(self, data='', status=200):
        self.data = data
        self.status = status

    def _text(self, data):
        return ('HTTP/1.1 200 OK\n'
                'Content-Language: en-us\n'
                'Content-Type: application/json\n'
                'Content-Length: {len}\n'
                'Server: jz/0.1.0\n\r\n\r\n'
                '{body}\n'.format(len=len(data) + 2, body=data))

    @property
    def text(self):
        s = json.dumps(self.data) if isinstance(self.data, dict) else self.data
        return self._text(s)


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
