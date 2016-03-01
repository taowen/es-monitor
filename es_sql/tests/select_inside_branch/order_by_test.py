import unittest

from es_sql import es_query


class SelectInsideBranchOrderByTest(unittest.TestCase):
    def test_order_by_can_reference_child_buckets(self):
        executor = es_query.create_executor(
            ["WITH per_year AS (SELECT ipo_year, COUNT(*) AS ipo_count FROM symbol \n"
             "GROUP BY ipo_year ORDER BY max_in_finance LIMIT 2)",
             "SELECT MAX(market_cap) AS max_in_finance FROM per_year WHERE sector='Finance'"])
        self.assertEqual(
            {'aggs': {
            u'ipo_year': {'terms': {'field': u'ipo_year', 'order': {u'level2.max_in_finance': 'asc'}, 'size': 2},
                          'aggs': {'level2': {'filter': {'term': {u'sector': 'Finance'}},
                                              'aggs': {u'max_in_finance': {u'max': {'field': u'market_cap'}}}}}}},
             'size': 0},
            executor.request)
