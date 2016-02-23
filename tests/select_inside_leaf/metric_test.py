import unittest
import es_query


class SelectInsideLeafMetricTest(unittest.TestCase):
    def test_count_star(self):
        executor = es_query.create_executor("SELECT COUNT(*) FROM symbol")
        self.assertEqual({'aggs': {}, 'size': 0}, executor.request)

    def test_count_field(self):
        executor = es_query.create_executor("SELECT COUNT(ipo_year) FROM symbol")
        self.assertEqual(
            {'aggs': {'COUNT(ipo_year)': {'value_count': {'field': 'ipo_year'}}}, 'size': 0},
            executor.request)

    def test_count_distinct(self):
        executor = es_query.create_executor("SELECT COUNT(DISTINCT ipo_year) FROM symbol")
        self.assertEqual(
            {'aggs': {'COUNT(DISTINCT ipo_year)': {'cardinality': {'field': 'ipo_year'}}}, 'size': 0},
            executor.request)

    def test_max(self):
        executor = es_query.create_executor("SELECT MAX(ipo_year) FROM symbol")
        self.assertEqual(
            {'aggs': {'MAX(ipo_year)': {'max': {'field': 'ipo_year'}}}, 'size': 0},
            executor.request)

    def test_min(self):
        executor = es_query.create_executor("SELECT MIN(ipo_year) FROM symbol")
        self.assertEqual(
            {'aggs': {'MIN(ipo_year)': {'min': {'field': 'ipo_year'}}}, 'size': 0},
            executor.request)

    def test_avg(self):
        executor = es_query.create_executor("SELECT AVG(ipo_year) FROM symbol")
        self.assertEqual(
            {'aggs': {'AVG(ipo_year)': {'avg': {'field': 'ipo_year'}}}, 'size': 0},
            executor.request)

    def test_sum(self):
        executor = es_query.create_executor("SELECT SUM(market_cap) FROM symbol")
        self.assertEqual(
            {'aggs': {'SUM(market_cap)': {'sum': {'field': 'market_cap'}}}, 'size': 0},
            executor.request)

    def test_count_dot(self):
        executor = es_query.create_executor("SELECT COUNT(a.b) FROM symbol")
        self.assertEqual(
            {'aggs': {'COUNT(a.b)': {'value_count': {'field': u'a.b'}}}, 'size': 0},
            executor.request)

    def test_count_distinct_dot(self):
        executor = es_query.create_executor("SELECT COUNT(DISTINCT a.b) FROM symbol")
        self.assertEqual(
            {'aggs': {'COUNT(DISTINCT a.b)': {'cardinality': {'field': u'a.b'}}}, 'size': 0},
            executor.request)
