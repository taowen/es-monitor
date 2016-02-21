import unittest
import es_query


class SelectInsideProjectionTest(unittest.TestCase):
    def test_one_level(self):
        executor = es_query.create_executor([
            "WITH SELECT MAX(sum_this_year) AS max_all_times FROM symbol AS all_symbols",
            "SELECT ipo_year, SUM(market_cap) AS sum_this_year FROM all_symbols GROUP BY ipo_year LIMIT 5"])
        print(executor.request)

