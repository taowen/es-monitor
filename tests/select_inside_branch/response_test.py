import unittest
import es_query


class SelectInsideBranchResponseTest(unittest.TestCase):
    def test_one_child(self):
        executor = es_query.create_executor([
            "WITH SELECT MAX(market_cap) AS max_all_times FROM symbol AS all_symbols",
            "SELECT ipo_year, MAX(market_cap) AS max_this_year FROM all_symbols GROUP BY ipo_year LIMIT 5"])
        rows = executor.select_response({
            "hits": {
                "hits": [],
                "total": 6714,
                "max_score": 0.0
            },
            "_shards": {
                "successful": 1,
                "failed": 0,
                "total": 1
            },
            "took": 2,
            "aggregations": {
                "max_all_times": {
                    "value": 522690000000.0
                },
                "ipo_year": {
                    "buckets": [
                        {
                            "max_this_year": {
                                "value": 54171930444.0
                            },
                            "key": 2014,
                            "doc_count": 390
                        },
                        {
                            "max_this_year": {
                                "value": 5416144671.0
                            },
                            "key": 2015,
                            "doc_count": 334
                        },
                        {
                            "max_this_year": {
                                "value": 10264219758.0
                            },
                            "key": 2013,
                            "doc_count": 253
                        },
                        {
                            "max_this_year": {
                                "value": 287470000000.0
                            },
                            "key": 2012,
                            "doc_count": 147
                        },
                        {
                            "max_this_year": {
                                "value": 7436036210.0
                            },
                            "key": 2011,
                            "doc_count": 144
                        }
                    ],
                    "sum_other_doc_count": 1630,
                    "doc_count_error_upper_bound": 0
                }
            },
            "timed_out": False
        })
        self.assertEqual(
            [{'max_this_year': 54171930444.0, 'max_all_times': 522690000000.0, u'ipo_year': 2014},
             {'max_this_year': 5416144671.0, 'max_all_times': 522690000000.0, u'ipo_year': 2015},
             {'max_this_year': 10264219758.0, 'max_all_times': 522690000000.0, u'ipo_year': 2013},
             {'max_this_year': 287470000000.0, 'max_all_times': 522690000000.0, u'ipo_year': 2012},
             {'max_this_year': 7436036210.0, 'max_all_times': 522690000000.0, u'ipo_year': 2011}],
            rows)

    def test_two_children(self):
        executor = es_query.create_executor([
            "WITH SELECT MAX(market_cap) AS max_all_times FROM symbol AS all_symbols",
            "SELECT ipo_year, MAX(market_cap) AS max_this_year FROM all_symbols GROUP BY ipo_year LIMIT 1",
            "SELECT sector, MAX(market_cap) AS max_this_sector FROM all_symbols GROUP BY sector LIMIT 1"])
        rows = executor.select_response({
            "hits": {
                "hits": [],
                "total": 6714,
                "max_score": 0.0
            },
            "_shards": {
                "successful": 1,
                "failed": 0,
                "total": 1
            },
            "took": 2,
            "aggregations": {
                "sector": {
                    "buckets": [
                        {
                            "max_this_sector": {
                                "value": 34620000000.0
                            },
                            "key": "n/a",
                            "doc_count": 1373
                        }
                    ],
                    "sum_other_doc_count": 5341,
                    "doc_count_error_upper_bound": 0
                },
                "max_all_times": {
                    "value": 522690000000.0
                },
                "ipo_year": {
                    "buckets": [
                        {
                            "max_this_year": {
                                "value": 54171930444.0
                            },
                            "key": 2014,
                            "doc_count": 390
                        }
                    ],
                    "sum_other_doc_count": 2508,
                    "doc_count_error_upper_bound": 0
                }
            },
            "timed_out": False
        })
        self.assertEqual(
            [{'max_this_year': 54171930444.0, u'ipo_year': 2014, 'max_all_times': 522690000000.0},
             {u'sector': 'n/a', 'max_all_times': 522690000000.0, 'max_this_sector': 34620000000.0}],
            rows)
