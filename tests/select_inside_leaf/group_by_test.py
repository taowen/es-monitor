import unittest
import es_query


class SelectInsideLeafGroupByTest(unittest.TestCase):
    def test_group_by_one(self):
        executor = es_query.create_executor("SELECT ipo_year, COUNT(*) FROM symbol GROUP BY ipo_year")
        self.assertEqual(
            {'aggs': {'ipo_year': {'terms': {'field': 'ipo_year', 'size': 0}, 'aggs': {}}}, 'size': 0},
            executor.request)

    def test_group_by_two(self):
        executor = es_query.create_executor("SELECT ipo_year, COUNT(*) FROM symbol GROUP BY ipo_year, abc")
        self.assertEqual(
            {'aggs': {
                'ipo_year': {'terms': {'field': 'ipo_year', 'size': 0}, 'aggs': {
                    'abc': {'terms': {'field': 'abc', 'size': 0}, 'aggs': {}}}}},
                'size': 0},
            executor.request)
