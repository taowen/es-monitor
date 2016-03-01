import unittest

from es_sql import es_query


class SelectInsideBranchGroupByTest(unittest.TestCase):
    def test_drill_down_one_direction(self):
        executor = es_query.create_executor([
            "WITH all_symbols AS (SELECT MAX(market_cap) AS max_all_times FROM symbol)",
            "SELECT ipo_year, MAX(market_cap) AS max_this_year FROM all_symbols GROUP BY ipo_year LIMIT 5"])
        self.assertEqual(
            {'aggs': {'max_all_times': {'max': {'field': 'market_cap'}},
                      'ipo_year': {'terms': {'field': 'ipo_year', 'size': 5},
                                   'aggs': {'max_this_year': {'max': {'field': 'market_cap'}}}}}, 'size': 0},
            executor.request)

    def test_drill_down_two_directions(self):
        executor = es_query.create_executor([
            "WITH all_symbols AS (SELECT MAX(market_cap) AS max_all_times FROM symbol)",
            "SELECT ipo_year, MAX(market_cap) AS max_this_year FROM all_symbols GROUP BY ipo_year LIMIT 1",
            "SELECT sector, MAX(market_cap) AS max_this_sector FROM all_symbols GROUP BY sector LIMIT 1"])
        self.assertEqual(
            {'aggs': {u'sector': {'terms': {'field': u'sector', 'size': 1},
                                  'aggs': {u'max_this_sector': {u'max': {'field': u'market_cap'}}}},
                      u'max_all_times': {u'max': {'field': u'market_cap'}},
                      u'ipo_year': {'terms': {'field': u'ipo_year', 'size': 1},
                                    'aggs': {u'max_this_year': {u'max': {'field': u'market_cap'}}}}}, 'size': 0},
            executor.request)
