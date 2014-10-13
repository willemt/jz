
import simplejson as json
from collections import defaultdict


class OpRegister(object):
    ops = {}

    def add(self, cls):
        self.ops[cls.name] = cls
        return cls

ops = OpRegister()


class ColOp(object):
    def __init__(self, column, target):
        self.column = column
        self.target = target

    def __repr__(self):
        return "{0}({1}: {2})".format(type(self), self.column, self.target)


@ops.add
class AndColOp(ColOp):
    name = "and"


@ops.add
class EqColOp(ColOp):
    name = "eq"


@ops.add
class LtColOp(ColOp):
    name = "lt"


@ops.add
class NotColOp(ColOp):
    name = "not"


@ops.add
class GtColOp(ColOp):
    name = "gt"


class Clause(object):
    def __init__(self, func, *args):
        if func == 'equal':
            print("Is equal?")
            print(*args)

    def get_columns(self):
        """ Get the columns we need """
        return []


class QueryPlan(object):
    def __init__(self, raw):
        self.raw = raw

    def ops(self):
        j = json.loads(self.raw)

        column_ops = []

        for k, v in j.items():
            if k == 'where':
                for clause in v:
                    try:
                        op_class = ops.ops[clause[0]]
                    except KeyError:
                        pass
                    else:
                        op = op_class(column=clause[1], target=clause[2])
                        column_ops.append(op)

        print(column_ops)

        def merge(ops):
            by_column = defaultdict(list)
            for op in ops:
                by_column[op.column].append(op)
            for column, ops in 
                and_ = AndColOp(
                for op in ops[]:
                    and_a.append(op)
                yield and_a

        column_ops = merge(list(column_ops))

        return column_ops
#       Clause(*clause)



if __name__ == '__main__':
    pass
