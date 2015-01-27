from heapq import *
from collections import defaultdict


class Op(object):
    def __iter__(self):
        return self


class Scan(Op):
    """
    Pull tuples from storage
    """

    def __init__(self, storage, column):
        self.storage = storage
        self.column = column

    def produce(self):
        for row in self.storage.column_scanner(self.column).produce():
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
    """
    Sorts stream based off document ID
    """

    def __init__(self, source):
        self.source = source
        self.heap = []

    def produce(self):
        for row in self.source.produce():
            heappush(self.heap, (row[1], row))

        for i in range(len(self.heap)):
            yield heappop(self.heap)[1]


#
# Sort vs. Hash Revisited: Fast Join Implementation on Modern Multi-Core CPUs, Changkyu Kim et all
#

class HashJoin(Op):
    """
    Joins 2 streams into 1

    The join uses the document ID
    """

    def __init__(self, a, b):
        self.sources = (a, b)

    def produce(self):
        ids = set()
        for a in self.sources[0].produce():
            ids.add(a[1])
        for b in self.sources[1].produce():
            if b[1] in ids:
                yield b


class MergeJoin(Op):
    """
    Joins 2 streams into 1

    The join uses the document ID
    Input must be sorted for the join to work.
    Output is sorted
    """

    def __init__(self, a, b):
        self.sources = (a, b)

    def produce(self):
        _a = self.sources[0].produce()
        _b = self.sources[1].produce()
        a = _a.next()
        b = _b.next()
        while a and b:
            if a[1] < b[1]:
                a = _a.next()
            elif a[1] > b[1]:
                b = _b.next()
            else:
                yield a
                a = _a.next()
                b = _b.next()
