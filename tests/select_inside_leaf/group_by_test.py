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

    def test_group_by_date_trunc(self):
        executor = es_query.create_executor(
            "SELECT year, MAX(adj_close) FROM quote WHERE symbol='AAPL' "
            "GROUP BY date_trunc('year',\"date\") AS year")
        self.assertEqual(
            {'query': {'term': {'symbol': 'AAPL'}}, 'aggs': {
                'year': {'date_histogram': {'field': 'date', 'interval': 'year', 'time_zone': '+08:00'},
                         'aggs': {'MAX(adj_close)': {'max': {'field': 'adj_close'}}}}}, 'size': 0},
            executor.request)

    def test_group_by_date_trunc(self):
        executor = es_query.create_executor(
            "SELECT year, MAX(adj_close) FROM quote WHERE symbol='AAPL' "
            "GROUP BY TO_CHAR(date_trunc('year',\"date\"), 'yyyy-MM-dd') AS year")
        self.assertEqual(
            {'query': {'term': {'symbol': 'AAPL'}}, 'aggs': {
                'year': {'date_histogram': {'field': 'date', 'interval': 'year',
                                            'time_zone': '+08:00', 'format': 'yyyy-MM-dd'},
                         'aggs': {'MAX(adj_close)': {'max': {'field': 'adj_close'}}}}}, 'size': 0},
            executor.request)

    def test_group_by_histogram(self):
        executor = es_query.create_executor(
            "select ipo_year_range, count(*) from symbol group by histogram(ipo_year, 5) as ipo_year_range")
        self.assertEqual(
            {'aggs': {'ipo_year_range': {'aggs': {}, 'histogram': {'field': 'ipo_year', 'interval': 5}}}, 'size': 0},
            executor.request)
