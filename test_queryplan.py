

from queryplan import QueryPlan


#def test_ands():
#    query = '{"where": [["gt", "j", 5], ["lt", "j", 10]]}'
#    qp = QueryPlan(query)
#
#    assert 1 == len(qp.ops())



def test_simple_select():
    db = Storage()
    db.contents = [
        {'j': 4},
        {'j': 6},
        {'j': 9},
        {'j': 10},
        {'j': 20},
        ]
    query = 'WHERE 6 < j'
    qp = QueryPlan(query)
    result = qp.build(db)
    assert result == [{'j': 9}, {'j': 10}, {'j': 20}]


def test_select_with_two_clauses():
    db = Storage()
    db.contents = [
        {'j': 4},
        {'j': 6},
        {'j': 9},
        {'j': 10},
        {'j': 20},
        ]
    query = 'WHERE 5 < j AND j < 10'
    qp = QueryPlan(query)
    result = qp.build(db)
    qp.run(db)
    assert sorted(result) == [{'j': 6}, {'j': 9}]


def test_select_with_two_clauses_two_columns():
    db = Storage()
    db.contents = [
        {'j': 4, 'k': 3},
        {'j': 6, 'k': 3},
        {'j': 9, 'k': 10},
        {'j': 10, 'k': 3},
        {'j': 20, 'k': 20},
        ]
    query = 'WHERE 5 < j AND k < 10'
    qp = QueryPlan(query)
    results = qp.build(db)
    qp.run(db)
    assert sorted(results) == [
        {'k': 3, 'j': 6},
        {'k': 3, 'j': 10},
        ]


#def test_sum_with_where_clause():
#    db = Storage()
#    db.contents = [
#        {'j': 'a', 'k': 3},
#        {'j': 'a', 'k': 3},
#        {'j': 'b', 'k': 10},
#        {'j': 'a', 'k': 3},
#        {'j': 'c', 'k': 20},
#        {'j': 'b', 'k': 20},
#        ]
#    query = 'SELECT sum(k) WHERE 5 < k'
#    qp = QueryPlan(query)
#    results = qp.build(db)
#    qp.run(db)
#    assert results == []

#def test_groupby_multiple():
##    db = Storage()
#    db.contents = [
#        {'number': 1, 'letter': 'a', 'k': 3},
#        {'number': 2, 'letter': 'a', 'k': 3},
#        {'number': 1, 'letter': 'b', 'k': 10},
#        {'number': 1, 'letter': 'b', 'k': 1},
#        {'number': 2, 'letter': 'a', 'k': 3},
#        {'number': 1, 'letter': 'c', 'k': 20},
#        {'number': 2, 'letter': 'b', 'k': 20},
#        ]
#    query = '{"select": [["sum", "k"], ["count", "k"]], '\
#            ' "groupby": ["letter", "number"]}'
#    print(query)
#    qp = QueryPlan(query)
#    qp.build(db)
#    qp.run(db)
#    assert 1 == 0

if __name__ == '__main__':
    test_simple_select()
    test_select_with_two_clauses()
    test_select_with_two_clauses_two_columns()
