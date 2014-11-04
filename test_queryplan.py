

from queryplan import QueryPlan


#def test_ands():
#    query = '{"where": [["gt", "j", 5], ["lt", "j", 10]]}'
#    qp = QueryPlan(query)
#
#    assert 1 == len(qp.ops())


class Storage(object):
    def __init__(self):
        self.contents = []

    def produce(self):
        for i in self.contents:
            yield i


def test_ands2():
    db = Storage()
    db.contents = [
        {'j': 4},
        {'j': 6},
        {'j': 9},
        {'j': 10},
        {'j': 20},
        ]
    #query = '{"where": [["gt", "j", 5], ["lt", "j", 10]]}'
    query = 'WHERE 5 < j AND j < 10'
    qp = QueryPlan(query)
    qp.build(db)
    qp.run(db)

def test_ands3():
    db = Storage()
    db.contents = [
        {'j': 4, 'k': 3},
        {'j': 6, 'k': 3},
        {'j': 9, 'k': 10},
        {'j': 10, 'k': 3},
        {'j': 20, 'k': 20},
        ]
    query = '{"where": [["gt", "j", 5], ["lt", "k", 10]]}'
    print(query)
    qp = QueryPlan(query)
    qp.build(db)
    qp.run(db)
    assert 1 == 0


def test_sum():
    db = Storage()
    db.contents = [
        {'j': 'a', 'k': 3},
        {'j': 'a', 'k': 3},
        {'j': 'b', 'k': 10},
        {'j': 'a', 'k': 3},
        {'j': 'c', 'k': 20},
        {'j': 'b', 'k': 20},
        ]
    #query = '{"select": [["unique", "j"], ["sum", "k"]]}'
    query = '{"select": [["count", "j"], ["unique", "j"], ["sum", "k"]],' \
            ' "where": [["gt", "k", 5]]}'
#    query = 'SELECT count(j), unique(j), sum(k) ' \
#            'WHERE gt(k, 5) ' \
#            'GROUP BY k'
    print(query)
    qp = QueryPlan(query)
    qp.build(db)
    qp.run(db)
    assert 1 == 0


def test_groupby():
    db = Storage()
    db.contents = [
        {'letter': 'a', 'k': 3},
        {'letter': 'a', 'k': 3},
        {'letter': 'b', 'k': 10},
        {'letter': 'a', 'k': 3},
        {'letter': 'c', 'k': 20},
        {'letter': 'b', 'k': 20},
        ]
#    query = '{"select": [["sum", "k"]], '\
#            ' "groupby": ["letter"]}'
    query = 'SELECT sum(k) GROUP BY letter'
    print(query)
    qp = QueryPlan(query)
    qp.build(db)
    qp.run(db)
    assert 1 == 0


def test_groupby_multiple():
    db = Storage()
    db.contents = [
        {'number': 1, 'letter': 'a', 'k': 3},
        {'number': 2, 'letter': 'a', 'k': 3},
        {'number': 1, 'letter': 'b', 'k': 10},
        {'number': 1, 'letter': 'b', 'k': 1},
        {'number': 2, 'letter': 'a', 'k': 3},
        {'number': 1, 'letter': 'c', 'k': 20},
        {'number': 2, 'letter': 'b', 'k': 20},
        ]
#    query = '{"select": [["sum", "k"], ["count", "k"]], '\
#            ' "groupby": ["letter", "number"]}'

    query = '{"select": [["sum", "k"], ["count", "k"]], '\
            ' "groupby": ["letter", "number"]}'
    print(query)
    qp = QueryPlan(query)
    qp.build(db)
    qp.run(db)
    assert 1 == 0


def test_groupby_multiple2():
    db = Storage()
    db.contents = [
        {'type': 'test', 'number': 1, 'letter': 'a', 'k': 3},
        {'type': 'test', 'number': 2, 'letter': 'a', 'k': 4},
        {'type': 'post', 'number': 2, 'letter': 'a', 'k': 3},
        {'type': 'test', 'number': 1, 'letter': 'b', 'k': 10},
        {'type': 'test', 'number': 1, 'letter': 'b', 'k': 1},
        {'type': 'test', 'number': 2, 'letter': 'a', 'k': 3},
        {'type': 'test', 'number': 1, 'letter': 'c', 'k': 20},
        {'type': 'test', 'number': 2, 'letter': 'b', 'k': 20},
        ]
#    query = '{"select": [["sum", "k"], ["count", "k"]], '\
#            ' "groupby": ["type", "letter", "number"]}'
    query = 'SELECT sum(k), count(k) GROUP BY type, letter, number'
    print(query)
    qp = QueryPlan(query)
    qp.build(db)
    qp.run(db)
    assert 1 == 0


if __name__ == '__main__':
    #test_ands2()
    test_ands2()
    #test_sum()
    #test_groupby()
    #test_groupby_multiple()
    #test_groupby_multiple2()
