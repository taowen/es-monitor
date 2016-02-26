import unittest

from es_sql import es_query


class SelectInsideLeafOrderByTest(unittest.TestCase):
    def test_order_by_term(self):
        executor = es_query.create_executor(
                "SELECT ipo_year, COUNT(*) FROM symbol GROUP BY ipo_year ORDER BY ipo_year")
        self.assertEqual(
                {'aggs': {
                    'ipo_year': {'terms': {'field': 'ipo_year', 'order': {'_term': 'asc'}, 'size': 0}, 'aggs': {}}},
                    'size': 0},
                executor.request)

    def test_order_by_count(self):
        executor = es_query.create_executor(
                "SELECT ipo_year, COUNT(*) AS c FROM symbol GROUP BY ipo_year ORDER BY c")
        self.assertEqual(
                {'aggs': {
                    'ipo_year': {'terms': {'field': 'ipo_year', 'order': {'_count': 'asc'}, 'size': 0}, 'aggs': {}}},
                    'size': 0},
                executor.request)

    def test_order_by_metric(self):
        executor = es_query.create_executor(
                "SELECT ipo_year, MAX(market_cap) AS c FROM symbol GROUP BY ipo_year ORDER BY c")
        self.assertEqual(
                {'aggs': {'ipo_year': {'terms': {'field': 'ipo_year', 'order': {'c': 'asc'}, 'size': 0},
                                       'aggs': {'c': {'max': {'field': 'market_cap'}}}}}, 'size': 0},
                executor.request)

    def test_order_by_histogram(self):
        executor = es_query.create_executor(
                "SELECT ipo_year_range, MAX(market_cap) AS max_market_cap FROM symbol "
                "GROUP BY histogram(ipo_year, 3) AS ipo_year_range ORDER BY ipo_year_range LIMIT 2")
        self.assertEqual(
                {'aggs': {'ipo_year_range': {'aggs': {'max_market_cap': {'max': {'field': 'market_cap'}}},
                                             'histogram': {'field': 'ipo_year', 'interval': 3, 'order': {'_key': 'asc'},
                                                           'size': 2}}}, 'size': 0},
                executor.request)

    def test_order_by_extended_stats(self):
        executor = es_query.create_executor(
                "SELECT ipo_year, STD_DEVIATION(market_cap) AS s FROM symbol GROUP BY ipo_year ORDER BY s")
        self.assertEqual(
                {'aggs': {u'ipo_year': {
                    'terms': {'field': u'ipo_year', 'order': {'market_cap_extended_stats.std_deviation': 'asc'},
                              'size': 0}, 'aggs': {
                        u'market_cap_extended_stats': {'extended_stats': {'field': u'market_cap'}}}}}, 'size': 0},
                executor.request)
