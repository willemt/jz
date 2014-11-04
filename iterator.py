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


class Aggregate(Op):
    def __init__(self, source, column, op):
        self.source = source
        self.column = column
        self.op = op

    def produce(self):
        yield self.op.run([row[self.column] for row in self.source.produce()])


class HashGroupbyAggregate(Op):
    def __init__(self, source, columns, ops):
        self.source = source
        self.columns = columns
        self.ops = ops

    def produce(self):
        new_rows = {}
        for row in self.source.produce():
            new = new_rows
            for col in self.columns[:-1]:
                aggcol = row[col]
                if aggcol not in new:
                    new[aggcol] = {}
                new = new[aggcol]

            aggcol = row[self.columns[-1]]
            if aggcol not in new:
                new[aggcol] = []
                for op in self.ops:
                    new[aggcol].append(op.init(row[op.column]))
            else:
                for i, op in enumerate(self.ops):
                    new[aggcol][i] = op.accum(new[aggcol][i], row[op.column])

        def flatten(rows, row):
            if not isinstance(rows, dict):
                yield row + rows
            else:
                for k, v in rows.items():
                    for i in flatten(v, row + [k]):
                        yield i

        for k in flatten(new_rows, row=[]):
            calcd_cols = ['{0}_{1}'.format(op.column, op.name) for op in self.ops]
            yield dict(zip(self.columns + calcd_cols, k))


class SingleHashGroupbyAggregate(Op):
    def __init__(self, source, column, ops):
        self.source = source
        self.column = column
        self.ops = ops

    def produce(self):
        new_rows = defaultdict(lambda: 0)
        for row in self.source.produce():
            aggcol = row[self.column]
            for op in self.ops:
                new_rows[aggcol] = op.run(new_rows[aggcol], row[op.column])

        for k, v in new_rows.items():
            yield {self.column: k, '{0}_of_{1}'.format(op.name, self.column): v}




class Groupby(Op):
    def __init__(self, source, column):
        self.source = source
        self.column = column

    def produce(self):
        keys = {}
        for row in self.source.produce():
            val = row[self.column]
            if val not in keys:
                keys[val] = True
                yield val


class Sum(Op):
    def __init__(self, source, column):
        self.source = source
        self.column = column

    def produce(self):
        yield sum([row[self.column] for row in self.source.produce()])
