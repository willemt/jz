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


class Sort(Op):
    def __init__(self, source, column):
        self.source = source
        self.column = column
        self.heap = []

    def produce(self):
        for row in self.source.produce():
            heappush(self.heap, row)

        for i in range(len(self.heap)):
            yield heappop(self.heap)


class HashJoin(Op):
    def produce(self):
        return self


class MergeJoin(Op):
    def __init__(self, sources):
        self.sources = sources

    def produce(self):
        ap = self.sources[0].produce()
        bp = self.sources[1].produce()
        a = ap.next()
        b = bp.next()
        while a and b:
            #print(a, b)
            if id(a) < id(b):
                a = ap.next()
            elif id(a) > id(b):
                b = bp.next()
            else:
                yield a
                a = ap.next()
                b = bp.next()
