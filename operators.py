from collections import defaultdict


class ColumnType(object):
    pass


def casttype(arg, atype):
    if atype == int:
        return int(arg)
    return arg


def determine_type(arg):
    try:
        int(arg)
    except ValueError:
        return ColumnType
    else:
        return int


def determine_types(*args):
    return [determine_type(arg) for arg in args]


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
