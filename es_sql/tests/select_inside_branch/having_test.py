import unittest

from es_sql import es_query


class SelectInsideBranchHavingTest(unittest.TestCase):
    def test_having_can_reference_child_buckets(self):
        executor = es_query.create_executor(
            ["WITH per_year AS (SELECT ipo_year, COUNT(*) AS ipo_count FROM symbol \n"
             "GROUP BY ipo_year HAVING max_in_finance > 200)",
             "SELECT MAX(market_cap) AS max_in_finance FROM per_year WHERE sector='Finance'"])
        self.assertEqual(
            {'aggs': {u'ipo_year': {'terms': {'field': u'ipo_year', 'size': 0}, 'aggs': {
            'level2': {'filter': {'term': {u'sector': 'Finance'}},
                       'aggs': {u'max_in_finance': {u'max': {'field': u'market_cap'}}}}, 'having': {
            'bucket_selector': {'buckets_path': {u'max_in_finance': u'level2.max_in_finance'},
                                'script': {'lang': 'expression', 'inline': u' max_in_finance > 200'}}}}}}, 'size': 0},
            executor.request)
