import unittest

from es_sql import es_query


class SelectInsideBranchWhereTest(unittest.TestCase):
    def test_filter_without_group_by(self):
        executor = es_query.create_executor([
            "WITH all_symbols AS (SELECT MAX(market_cap) AS max_all_times FROM symbol)",
            "SELECT MAX(market_cap) AS max_at_2000 FROM all_symbols WHERE ipo_year=2000"])
        self.assertEqual(
            {'aggs': {'level2': {'filter': {'term': {u'ipo_year': 2000}},
                                 'aggs': {u'max_at_2000': {u'max': {'field': u'market_cap'}}}},
                      u'max_all_times': {u'max': {'field': u'market_cap'}}}, 'size': 0},
            executor.request)

    def test_two_filters(self):
        executor = es_query.create_executor([
            "WITH all_symbols AS (SELECT MAX(market_cap) AS max_all_times FROM symbol)",
            "SELECT MAX(market_cap) AS max_at_2000 FROM all_symbols WHERE ipo_year=2000",
            "SELECT MAX(market_cap) AS max_at_2001 FROM all_symbols WHERE ipo_year=2001"])
        self.assertEqual(
            {'aggs': {'level2': {'filter': {'term': {u'ipo_year': 2000}},
                                 'aggs': {u'max_at_2000': {u'max': {'field': u'market_cap'}}}},
                      'level3': {'filter': {'term': {u'ipo_year': 2001}},
                                 'aggs': {u'max_at_2001': {u'max': {'field': u'market_cap'}}}},
                      u'max_all_times': {u'max': {'field': u'market_cap'}}}, 'size': 0},
            executor.request)

    def test_filter_upon_filter(self):
        executor = es_query.create_executor([
            "WITH all_symbols AS (SELECT MAX(market_cap) AS max_all_times FROM symbol)",
            "WITH year_2001 AS (SELECT MAX(market_cap) AS max_at_2000 FROM all_symbols WHERE ipo_year=2000)",
            "SELECT MAX(market_cap) AS max_at_2001_finance FROM year_2001 WHERE sector='Finance'"])
        self.assertEqual(
            {'aggs': {'year_2001': {'filter': {'term': {u'ipo_year': 2000}},
                                    'aggs': {u'max_at_2000': {u'max': {'field': u'market_cap'}},
                                             'level3': {'filter': {'term': {u'sector': 'Finance'}}, 'aggs': {
                                                 u'max_at_2001_finance': {u'max': {'field': u'market_cap'}}}}}},
                      u'max_all_times': {u'max': {'field': u'market_cap'}}}, 'size': 0},
            executor.request)

    def test_filter_then_group_by(self):
        executor = es_query.create_executor([
            "WITH all_symbols AS (SELECT MAX(market_cap) AS max_all_times FROM symbol)",
            "WITH year_2000 AS (SELECT MAX(market_cap) AS max_at_2000 FROM all_symbols WHERE ipo_year=2000)",
            "SELECT sector, MAX(market_cap) AS max_per_sector FROM year_2000 GROUP BY sector LIMIT 2"])
        self.assertEqual(
            {
                "aggs": {
                    "year_2000": {
                        "filter": {
                            "term": {
                                "ipo_year": 2000
                            }
                        },
                        "aggs": {
                            "max_at_2000": {
                                "max": {
                                    "field": "market_cap"
                                }
                            },
                            "sector": {
                                "terms": {
                                    "field": "sector",
                                    "size": 2
                                },
                                "aggs": {
                                    "max_per_sector": {
                                        "max": {
                                            "field": "market_cap"
                                        }
                                    }
                                }
                            }
                        }
                    },
                    "max_all_times": {
                        "max": {
                            "field": "market_cap"
                        }
                    }
                },
                "size": 0
            },
            executor.request)
