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
