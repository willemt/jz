from restless.resources import Resource

from http import Response


class IndexResource(Resource):
    def __init__(self, request=None, **kwargs):
        super(IndexResource, self).__init__(**kwargs)
        self.request = request

    def list(self):
        return {}

    def build_response(self, data, status=200):
        return Response(data, status=status)
