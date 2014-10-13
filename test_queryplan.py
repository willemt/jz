

from queryplan import QueryPlan


def test_ands():
    query = '{"where": [["gt", "j", 5], ["lt", "j", 10]]}'
    qp = QueryPlan(query)

    assert 1 == len(qp.ops())
