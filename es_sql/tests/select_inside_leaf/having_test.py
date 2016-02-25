import unittest

from es_sql import es_query


class SelectInsideLeafHavingTest(unittest.TestCase):
    def test_having_by_count(self):
        executor = es_query.create_executor(
            "SELECT ipo_year, COUNT(*) AS ipo_count FROM symbol GROUP BY ipo_year HAVING ipo_count > 100")
        self.assertEqual(
            {'aggs': {u'ipo_year': {'terms': {'field': u'ipo_year', 'size': 0}, 'aggs': {'having': {
                'bucket_selector': {'buckets_path': {u'ipo_count': '_count'},
                                    'script': {'lang': 'expression', 'inline': u' ipo_count > 100'}}}}}}, 'size': 0},
            executor.request)

    def test_having_by_key(self):
        executor = es_query.create_executor(
            "SELECT ipo_year, COUNT(*) AS ipo_count FROM symbol GROUP BY ipo_year HAVING ipo_year > 100")
        {'aggs': {u'ipo_year': {'terms': {'field': u'ipo_year', 'size': 0}, 'aggs': {'having': {
        'bucket_selector': {'buckets_path': {u'ipo_year': '_key'},
                            'script': {'lang': 'expression', 'inline': u' _key > 100'}}}}}}, 'size': 0}

        self.assertEqual(
            {'aggs': {'ipo_year': {'terms': {'field': 'ipo_year', 'size': 0}, 'aggs': {'having': {
                'bucket_selector': {'buckets_path': {'ipo_year': '_key'},
                                    'script': {'lang': 'expression', 'inline': ' ipo_year > 100'}}}}}}, 'size': 0},
            executor.request)

    def test_having_by_metric(self):
        executor = es_query.create_executor(
            "SELECT ipo_year, MAX(market_cap) AS max_market_cap FROM symbol GROUP BY ipo_year HAVING max_market_cap > 100")
        self.assertEqual(
            {'aggs': {'ipo_year': {'terms': {'field': 'ipo_year', 'size': 0}, 'aggs': {'having': {
                'bucket_selector': {
                    'buckets_path': {'max_market_cap': 'max_market_cap'},
                    'script': {
                        'lang': 'expression', 'inline': ' max_market_cap > 100'}}},
                'max_market_cap': {'max': {
                    'field': 'market_cap'}}}}},
             'size': 0},
            executor.request)
