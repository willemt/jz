from heapq import *
from collections import defaultdict


class Op(object):
    def __iter__(self):
        return self


class Scan(Op):
    def __init__(self, storage, column):
        self.storage = storage
        self.column = column

    def produce(self):
        """
        Pull tuple from storage
        """
        for row in self.storage.produce():
            yield row


class Filter(Op):
    def __init__(self, source, test):
        self.source = source
        self.test = test

    def produce(self):
        for row in self.source.produce():
            if self.test.run(row):
                yield row


class IdSort(Op):
    def __init__(self, source):
        self.source = source
        self.heap = []

    def produce(self):
        for row in self.source.produce():
            heappush(self.heap, (id(row), row))

        for i in range(len(self.heap)):
            yield heappop(self.heap)[1]


class HashJoin(Op):
    def produce(self):
        return self


class MergeJoin(Op):
    def __init__(self, a, b):
        self.sources = (a, b)

    def produce(self):
        ap = self.sources[0].produce()
        bp = self.sources[1].produce()
        a = ap.next()
        b = bp.next()
        while a and b:
            if id(a) < id(b):
                a = ap.next()
            elif id(a) > id(b):
                b = bp.next()
            else:
                yield a
                a = ap.next()
                b = bp.next()
