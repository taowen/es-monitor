import unittest

from es_sql import es_query


class SelectInsideProjectionTest(unittest.TestCase):
    def test_one_level(self):
        executor = es_query.create_executor([
            "WITH all_symbols AS (SELECT MAX(sum_this_year) AS max_all_times FROM symbol)",
            "SELECT ipo_year, SUM(market_cap) AS sum_this_year FROM all_symbols GROUP BY ipo_year LIMIT 5"])
        self.assertEqual(
            {'aggs': {u'max_all_times': {u'max_bucket': {'buckets_path': u'ipo_year.sum_this_year'}},
                      u'ipo_year': {'terms': {'field': u'ipo_year', 'size': 5},
                                    'aggs': {u'sum_this_year': {u'sum': {'field': u'market_cap'}}}}}, 'size': 0},
            executor.request)

    def test_two_level(self):
        executor = es_query.create_executor([
            "WITH all_symbols AS (SELECT MAX(sum_this_year) AS max_all_times FROM symbol)",
            "WITH finance_symbols AS (SELECT * FROM all_symbols WHERE sector='Finance')",
            "SELECT ipo_year, SUM(market_cap) AS sum_this_year FROM finance_symbols GROUP BY ipo_year LIMIT 5"])
        self.assertEqual(
            {'aggs': {u'max_all_times': {u'max_bucket': {'buckets_path': u'finance_symbols>ipo_year.sum_this_year'}},
                      'finance_symbols': {'filter': {'term': {u'sector': 'Finance'}}, 'aggs': {
                          u'ipo_year': {'terms': {'field': u'ipo_year', 'size': 5},
                                        'aggs': {u'sum_this_year': {u'sum': {'field': u'market_cap'}}}}}}}, 'size': 0},
            executor.request)

    def test_csum(self):
        executor = es_query.create_executor([
            "SELECT year, MAX(adj_close) AS max_adj_close, CSUM(max_adj_close) FROM quote "
            "WHERE symbol='AAPL' GROUP BY date_trunc('year', \"date\") AS year"])
        self.assertEqual(
            {'query': {'term': {u'symbol': 'AAPL'}}, 'aggs': {
                u'year': {'date_histogram': {'field': u'date', 'interval': 'year', 'time_zone': '+08:00'},
                          'aggs': {u'max_adj_close': {u'max': {'field': u'adj_close'}},
                                   'CSUM(max_adj_close)': {'cumulative_sum': {'buckets_path': u'max_adj_close'}}}}},
             'size': 0},
            executor.request)

    def test_moving_average(self):
        executor = es_query.create_executor([
            "SELECT year, MAX(adj_close) AS max_adj_close, MOVING_Avg(max_adj_close) FROM quote "
            "WHERE symbol='AAPL' GROUP BY date_trunc('year', \"date\") AS year"])
        self.assertEqual(
            {'query': {'term': {u'symbol': 'AAPL'}}, 'aggs': {
                u'year': {'date_histogram': {'field': u'date', 'interval': 'year', 'time_zone': '+08:00'},
                          'aggs': {u'max_adj_close': {u'max': {'field': u'adj_close'}},
                                   'MOVING_Avg(max_adj_close)': {'moving_avg': {'buckets_path': u'max_adj_close'}}}}},
             'size': 0},
            executor.request)

    def test_moving_average_with_params(self):
        executor = es_query.create_executor([
            "SELECT year, MAX(adj_close) AS max_adj_close, MOVING_Avg(max_adj_close, '{\"window\":5}') AS ma FROM quote "
            "WHERE symbol='AAPL' GROUP BY date_trunc('year', \"date\") AS year"])
        self.assertEqual(
            {'query': {'term': {u'symbol': 'AAPL'}}, 'aggs': {
                u'year': {'date_histogram': {'field': u'date', 'interval': 'year', 'time_zone': '+08:00'},
                          'aggs': {u'max_adj_close': {u'max': {'field': u'adj_close'}},
                                   'ma': {
                                       'moving_avg': {'buckets_path': u'max_adj_close', 'window': 5}}}}}, 'size': 0},
            executor.request)

    def test_moving_average_with_named_params(self):
        executor = es_query.create_executor([
            "SELECT year, MAX(adj_close) AS max_adj_close, MOVING_Avg(max_adj_close, window=5, settings='{\"alpha\":0.8}') AS ma FROM quote "
            "WHERE symbol='AAPL' GROUP BY date_trunc('year', \"date\") AS year"])
        self.assertEqual(
            {'query': {'term': {u'symbol': 'AAPL'}}, 'aggs': {
                u'year': {'date_histogram': {'field': u'date', 'interval': 'year', 'time_zone': '+08:00'},
                          'aggs': {u'max_adj_close': {u'max': {'field': u'adj_close'}},
                                   'ma': {
                                       'moving_avg': {'buckets_path': u'max_adj_close', 'window': 5,
                                                      'settings': {'alpha': 0.8}}}}}}, 'size': 0},
            executor.request)

    def test_serial_diff(self):
        executor = es_query.create_executor([
            "SELECT year, MAX(adj_close) AS max_adj_close, SERIAL_DIFF(max_adj_close, lag=7) AS ma FROM quote "
            "WHERE symbol='AAPL' GROUP BY date_trunc('year', \"date\") AS year"])
        self.assertEqual(
            {'query': {'term': {u'symbol': 'AAPL'}}, 'aggs': {
                u'year': {'date_histogram': {'field': u'date', 'interval': 'year', 'time_zone': '+08:00'},
                          'aggs': {u'max_adj_close': {u'max': {'field': u'adj_close'}},
                                   u'ma': {u'serial_diff': {'buckets_path': u'max_adj_close', u'lag': 7}}}}},
             'size': 0},
            executor.request)

    def test_drivative(self):
        executor = es_query.create_executor([
            "SELECT year, MAX(adj_close) AS max_adj_close, DERIVATIVE(max_adj_close) FROM quote "
            "WHERE symbol='AAPL' GROUP BY date_trunc('year', \"date\") AS year"])
        self.assertEqual(
            {'query': {'term': {u'symbol': 'AAPL'}}, 'aggs': {
                u'year': {'date_histogram': {'field': u'date', 'interval': 'year', 'time_zone': '+08:00'},
                          'aggs': {u'max_adj_close': {u'max': {'field': u'adj_close'}},
                                   'DERIVATIVE(max_adj_close)': {'derivative': {'buckets_path': u'max_adj_close'}}}}},
             'size': 0},
            executor.request)