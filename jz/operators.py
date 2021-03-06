from collections import defaultdict
import ctypes
import struct


class ColumnType(object):
    pass


def casttype(arg, atype):
    if atype == int:
        return struct.pack('L', int(arg))
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
        return row[0] < self.args[1]


@operators.add('<')
class GtColOp2(ColOp):
    types = [int, ColumnType]

    def run(self, row):
        return self.args[0] < row[0]


@operators.add('>')
class LtColOp(ColOp):
    types = [ColumnType, int]

    def run(self, row):
        return row[0] > self.args[1]


@operators.add('>')
class LtColOp2(ColOp):
    types = [int, ColumnType]

    def run(self, row):
        return self.args[0] > row[0]


@operators.add('=')
class EqColOp(ColOp):
    types = [ColumnType, int]

    def run(self, row):
        return self.args[1] == row[0]


@operators.add('=')
class EqColOp2(ColOp):
    types = [int, ColumnType]

    def run(self, row):
        return self.args[0] == row[0]
