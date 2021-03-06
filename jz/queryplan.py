from collections import defaultdict

import iterator
import operators
import sqlparse


def mux(sources, join_type='sort-merge'):
    """
    Join multiple streams into one using join(s)
    """
    if join_type == 'sort-merge':
        m = iterator.MergeJoin(iterator.IdSort(sources[0]),
                               iterator.IdSort(sources[1]))
        for it in sources[2:]:
            m = iterator.MergeJoin(m, iterator.IdSort(it))
    else:
        # TODO: create a multi-hash join
        #  we can reuse one hash table for multiple inputs
        m = iterator.HashJoin(sources[0], sources[1])
        for it in sources[2:]:
            m = iterator.HashJoin(m, it)
    return m


def columns(ops):
    by_column = defaultdict(list)
    for op in ops:
        cols = [col for ptype, col in zip(op.types, op.args)
                if ptype == operators.ColumnType]
        for col in cols:
            by_column[col].append(op)
    return by_column


def _build_where(node):
    if not node:
        return []

    if node[0] == 'AND':
        return _build_where([node[1]])

    wops = []
    for clause in node:
        types = operators.determine_types(clause[0], clause[2])
        op_class = operators.get_operator(clause[1], types)

        if op_class:
            op = op_class(clause[0], clause[2])
            wops.append(op)
            try:
                wops.extend(_build_where(clause[3]))
            except IndexError:
                pass
        else:
            print("ERROR: Unknown op: {0} ({1})".format(clause[1], types))
    return wops


class QueryPlan(object):
    def __init__(self, raw):
        self.raw = raw

    def build(self, db):
        parser = sqlparse.sqlParser()
        ast = parser.parse(self.raw, rule_name='query')

        # print(ast)
        # print(json.dumps(ast, indent=2))

        wops = _build_where(ast['where'])

        # print(wops)

        sources = {}

        # Get scans
        # TODO: need to choose SARAGBLEs here
        # https://en.wikipedia.org/wiki/Sargable
        for col, ops in columns(wops).items():
            for op in ops:
                if isinstance(op, (operators.LtColOp2, operators.GtColOp2)):
                    sources[col] = iterator.Seek(db, col, start=op.args[0])
                else:
                    sources[col] = iterator.Scan(db, col)

        # Apply filters
        for col, ops in columns(wops).items():
            prev = sources[col]
            for op in ops:
                prev = iterator.Filter(prev, op)
            sources[col] = prev

        # Multiplex
        if 1 < len(sources):
            source = mux(sources.values(), join_type='hash')
            return list(source.produce())
        else:
            return list(sources.values()[0].produce())

    def run(self, db):
        pass


if __name__ == '__main__':
    pass
