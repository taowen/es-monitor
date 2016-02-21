import unittest
import es_query


class SelectInsideProjectionTest(unittest.TestCase):
    def test_one_level(self):
        executor = es_query.create_executor([
            "WITH SELECT MAX(sum_this_year) AS max_all_times FROM symbol AS all_symbols",
            "SELECT ipo_year, SUM(market_cap) AS sum_this_year FROM all_symbols GROUP BY ipo_year LIMIT 5"])
        self.assertEqual(
            {'aggs': {u'max_all_times': {u'max_bucket': {'buckets_path': u'ipo_year.sum_this_year'}},
                      u'ipo_year': {'terms': {'field': u'ipo_year', 'size': 5},
                                    'aggs': {u'sum_this_year': {u'sum': {'field': u'market_cap'}}}}}, 'size': 0},
            executor.request)
