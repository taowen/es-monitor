import unittest

from es_sql import es_query


class SelectInsideLeafGroupByTest(unittest.TestCase):
    def test_group_by_one(self):
        executor = es_query.create_executor("SELECT ipo_year, COUNT(*) FROM symbol GROUP BY ipo_year")
        self.assertEqual(
            {'aggs': {'ipo_year': {'terms': {'field': 'ipo_year', 'size': 0}, 'aggs': {}}}, 'size': 0},
            executor.request)

    def test_group_by_field_as_alias(self):
        executor = es_query.create_executor("SELECT year, COUNT(*) FROM symbol GROUP BY ipo_year AS year")
        self.assertEqual(
            {'aggs': {'year': {'terms': {'field': 'ipo_year', 'size': 0}, 'aggs': {}}}, 'size': 0},
            executor.request)

    def test_group_by_can_be_put_in_select(self):
        executor = es_query.create_executor("SELECT ipo_year AS year, COUNT(*) FROM symbol GROUP BY year")
        self.assertEqual(
            {'aggs': {'year': {'terms': {'field': 'ipo_year', 'size': 0}, 'aggs': {}}}, 'size': 0},
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
            "GROUP BY TO_CHAR(date_trunc('year',\"date\"), '%Y-%m-%d') AS year")
        self.assertEqual(
            {'query': {'term': {'symbol': 'AAPL'}}, 'aggs': {
                'year': {'date_histogram': {'field': 'date', 'interval': 'year',
                                            'time_zone': '+08:00', 'format': 'yyyy-MM-dd'},
                         'aggs': {'MAX(adj_close)': {'max': {'field': 'adj_close'}}}}}, 'size': 0},
            executor.request)

    def test_group_by_histogram(self):
        executor = es_query.create_executor(
            "SELECT ipo_year_range, COUNT(*) FROM symbol "
            "GROUP BY histogram(ipo_year, 5) AS ipo_year_range")
        self.assertEqual(
            {'aggs': {'ipo_year_range': {'aggs': {}, 'histogram': {'field': 'ipo_year', 'interval': 5}}}, 'size': 0},
            executor.request)

    def test_group_by_numeric_range(self):
        executor = es_query.create_executor(
            "SELECT ipo_year_range, COUNT(*) FROM symbol "
            "GROUP BY CASE "
            "  WHEN ipo_year_range >= 2000 THEN 'post_2000' "
            "  WHEN ipo_year_range < 2000 THEN 'pre_2000' END AS ipo_year_range")
        self.assertEqual(
            {'aggs': {'ipo_year_range': {
                'range': {'ranges': [{'from': 2000.0, 'key': 'post_2000'}, {'to': 2000.0, 'key': 'pre_2000'}],
                          'field': 'ipo_year_range'}, 'aggs': {}}}, 'size': 0},
            executor.request)

    def test_group_by_filters(self):
        executor = es_query.create_executor(
            "SELECT ipo_year_range, COUNT(*) FROM symbol "
            "GROUP BY CASE "
            "  WHEN ipo_year_range > 2000 THEN 'post_2000' "
            "  WHEN ipo_year_range < 2000 THEN 'pre_2000'"
            "  ELSE '2000' END AS ipo_year_range")
        self.assertEqual(
            {'aggs': {'ipo_year_range': {'filters': {
                'filters': {'pre_2000': {'range': {'ipo_year_range': {'lt': 2000}}},
                            'post_2000': {'range': {'ipo_year_range': {'gt': 2000}}}},
                'other_bucket_key': '2000'},
                'aggs': {}}}, 'size': 0},
            executor.request)

    def test_group_by_single_value_script(self):
        executor = es_query.create_executor(
            "SELECT ipo_year_range, COUNT(*) FROM symbol GROUP BY ipo_year / 6 AS ipo_year_range")
        self.assertEqual(
            {'aggs': {'ipo_year_range': {
                'terms': {'field': 'ipo_year', 'size': 0, 'script': {'lang': 'expression', 'inline': '_value / 6'}},
                'aggs': {}}}, 'size': 0},
            executor.request)

    def test_group_by_multiple_values_script(self):
        executor = es_query.create_executor(
            "SELECT shares_count, COUNT(*) FROM symbol GROUP BY market_cap / last_sale AS shares_count")
        self.assertEqual(
            {'aggs': {'shares_count': {
                'terms': {'size': 0, 'script': {
                    'lang': 'expression',
                    'inline': "doc['market_cap'].value / doc['last_sale'].value"}},
                'aggs': {}}}, 'size': 0},
            executor.request)

    def test_group_by_function(self):
        executor = es_query.create_executor(
            "SELECT shares_count, COUNT(*) FROM symbol GROUP BY floor(market_cap / last_sale) AS shares_count")
        self.assertEqual(
            {'aggs': {'shares_count': {
                'terms': {'size': 0, 'script': {
                    'lang': 'expression',
                    'inline': "floor(doc['market_cap'].value / doc['last_sale'].value)"}},
                'aggs': {}}}, 'size': 0},
            executor.request)
