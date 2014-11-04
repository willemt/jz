
import simplejson as json
from collections import defaultdict

import iterator
import sqlparse


class OpRegister(object):
    ops = defaultdict(list)

    def add(self, name):
        def register_class(cls):
            cls.name = name
            self.ops[name].append(cls)
            return cls
        return register_class

operators = OpRegister()


def get_operator(name, param_types):
    try:
        ops = operators.ops[name]
    except KeyError:
        pass
    else:
        for op in ops:
            if op.types == param_types:
                return op
    return None


class ColumnType(object):
    pass



def casttype(arg, atype):
    if atype == int:
        return int(arg)
    return arg


class ColOp(object):
    def __init__(self, *args):
        self.args = [casttype(arg, type) for arg, type in zip(args, self.types)]

    def __repr__(self):
        return "{0} '{1}' ({2})".format(type(self), self.name, self.args)


@operators.add('<')
class GtColOp(ColOp):
    types = [ColumnType, int]

    def run(self, row):
        return row[self.args[0]] < self.args[1]


@operators.add('<')
class GtColOp2(ColOp):
    types = [int, ColumnType]

    def run(self, row):
        return self.args[0] < row[self.args[1]]


@operators.add('>')
class LtColOp(ColOp):
    types = [ColumnType, int]

    def run(self, row):
        return row[self.args[0]] > self.args[1]


@operators.add('>')
class LtColOp2(ColOp):
    types = [int, ColumnType]

    def run(self, row):
        return self.args[0] > row[self.args[1]]


@operators.add('sum')
class SumOp(ColOp):
    types = [ColumnType]

    def init(self, value):
        return value

    def accum(self, current, value):
        return current + value


@operators.add('unique')
class UniqueOp(ColOp):
    def __init__(self, *args, **kwargs):
        super(UniqueOp, self).__init__(*args, **kwargs)
        self.keys = {}

    def run(self, rows):
        for r in rows:
            self.keys[r] = True
        return len(self.keys)


@operators.add('count')
class CountOp(ColOp):
    types = [ColumnType]

    def init(self, value):
        return 1

    def accum(self, current, value):
        return current + 1


def determine_type(arg):
    try:
        int(arg)
    except ValueError:
        return ColumnType
    else:
        return int


def determine_types(*args):
    return [determine_type(arg) for arg in args]


class QueryPlan(object):
    def __init__(self, raw):
        self.raw = raw

    def build(self, db):

        parser = sqlparse.sqlParser()
        ast = parser.parse(self.raw, rule_name='query')
        print(ast)
        print(json.dumps(ast, indent=2))

        sops = []
        wops = []
        gops = []

        def _build_where(node):
            if not node:
                return
            for clause in node:
                if clause[0] == 'AND':
                    _build_where(clause[1])
                    continue

                types = determine_types(clause[0], clause[2])
                op_class = get_operator(clause[1], types)
                if op_class:
                    op = op_class(clause[0], clause[2])
                    sops.append(op)
                    try:
                        _build_where(clause[3])
                    except IndexError:
                        pass
                else:
                    print("ERROR: Unknown op: {0} ({1})".format(clause[1], types))

        def _build_select(node):
            if not node:
                return
            for clause in node:
                types = determine_types(clause[0], clause[2])
                op_class = get_operator(clause[1], types)
                if op_class:
                    op = op_class(clause[0], clause[2])
                    sops.append(op)
                    try:
                        _build_select(clause[3])
                    except IndexError:
                        pass
                else:
                    print("ERROR: Unknown op: {0} ({1})".format(clause[1], types))

        def _build_groupby(node):
            if not node:
                return
            for clause in node:
                gops.append(clause)

        _build_where(ast['where'])
        _build_select(ast['select'])
        _build_groupby(ast['groupby'])

        print('sops', sops)
        print('wops', wops)

        def columns(ops):
            by_column = defaultdict(list)
            for op in ops:
                cols = [col for ptype, col in zip(op.types, op.args) if ptype == ColumnType]
                for col in cols:
                    by_column[col].append(op)
            return by_column

        sources = {}
        for col, ops in columns(sops+wops).items():
            sources[col] = iterator.Scan(db, col)

        print 'sourcesx', sources

        for col, ops in columns(wops).items():
            print("column {0}".format(col))
            prev = sources[col]
            for op in ops:
                prev = iterator.Filter(prev, op)
            sources[col] = prev

        print("Sources {0}".format(sources))

        # Final merge
#        if 1 < len(sources):
            #iterator.Sort(prev, col))
#            sources = {'a': m}
            #for i in m.produce():
            #    print i

        if gops:
            m = sources.values()[0]
            ops2 = []
            for col2, ops1 in columns(sops).items():
                for op in ops1:
                    ops2.append(op)
            j = iterator.HashGroupbyAggregate(m, gops, ops2)
            for i in j.produce():
                print(i)
        else:
            if 1 < len(sources):
                m = iterator.MergeJoin(sources.values())
                for i in m.produce():
                    print i
            else:
                print "RESULTS"
                for i in sources.values()[0].produce():
                    print i

    def run(self, db):
        pass


if __name__ == '__main__':
    pass
