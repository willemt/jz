
import simplejson as json
from collections import defaultdict

import iterator
import operators
import sqlparse


class QueryPlan(object):
    def __init__(self, raw):
        self.raw = raw

    def build(self, db):

        parser = sqlparse.sqlParser()
        ast = parser.parse(self.raw, rule_name='query')
        print(ast)
        print(json.dumps(ast, indent=2))

        wops = []

        def _build_where(node):
            if not node:
                return
            for clause in node:
                if clause[0] == 'AND':
                    _build_where(clause[1])
                    continue

                types = operators.determine_types(clause[0], clause[2])
                op_class = operators.get_operator(clause[1], types)
                if op_class:
                    op = op_class(clause[0], clause[2])
                    wops.append(op)
                    try:
                        _build_where(clause[3])
                    except IndexError:
                        pass
                else:
                    print("ERROR: Unknown op: {0} ({1})".format(clause[1], types))

        _build_where(ast['where'])

        print('wops', wops)

        def columns(ops):
            by_column = defaultdict(list)
            for op in ops:
                cols = [col for ptype, col in zip(op.types, op.args) if ptype == operators.ColumnType]
                for col in cols:
                    by_column[col].append(op)
            return by_column

        sources = {}
        for col, ops in columns(wops).items():
            sources[col] = iterator.Scan(db, col)

        for col, ops in columns(wops).items():
            print("column {0}".format(col))
            prev = sources[col]
            for op in ops:
                prev = iterator.Filter(prev, op)
            sources[col] = prev

        print("Sources {0}".format(sources))


        if 1 < len(sources):
            m = iterator.MergeJoin(sources.values()[0].values())
            for i in m.produce():
                print i

        return list(sources.values()[0].produce())

    def run(self, db):
        pass


if __name__ == '__main__':
    pass
