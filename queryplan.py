
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

                types = operators.determine_types(clause[0], clause[2])
                op_class = operators.get_operator(clause[1], types)
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
                types = operators.determine_types(clause[0], clause[2])
                op_class = operators.get_operator(clause[1], types)
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
                cols = [col for ptype, col in zip(op.types, op.args) if ptype == operators.ColumnType]
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
