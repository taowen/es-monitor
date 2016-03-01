import unittest

from es_sql import es_query


class SelectInsideBranchResponseTest(unittest.TestCase):
    def test_one_child(self):
        executor = es_query.create_executor([
            "WITH all_symbols AS (SELECT MAX(market_cap) AS max_all_times FROM symbol)",
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
            [{'max_this_year': 54171930444.0, u'ipo_year': 2014, 'max_all_times': 522690000000.0, '_bucket_path': ['level2']},
             {'max_this_year': 5416144671.0, u'ipo_year': 2015, 'max_all_times': 522690000000.0, '_bucket_path': ['level2']},
             {'max_this_year': 10264219758.0, u'ipo_year': 2013, 'max_all_times': 522690000000.0, '_bucket_path': ['level2']},
             {'max_this_year': 287470000000.0, u'ipo_year': 2012, 'max_all_times': 522690000000.0, '_bucket_path': ['level2']},
             {'max_this_year': 7436036210.0, u'ipo_year': 2011, 'max_all_times': 522690000000.0, '_bucket_path': ['level2']}],
            rows)

    def test_two_children(self):
        executor = es_query.create_executor([
            "WITH all_symbols AS (SELECT MAX(market_cap) AS max_all_times FROM symbol)",
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
            [{'max_this_year': 54171930444.0, 'ipo_year': 2014, 'max_all_times': 522690000000.0, '_bucket_path': ['level2']},
             {'sector': 'n/a', 'max_all_times': 522690000000.0, 'max_this_sector': 34620000000.0, '_bucket_path': ['level3']}],
            rows)

    def test_filter_only_will_not_create_new_row(self):
        executor = es_query.create_executor([
            "WITH all_symbols AS (SELECT MAX(market_cap) AS max_all_times FROM symbol)",
            "SELECT MAX(market_cap) AS max_at_2000 FROM all_symbols WHERE ipo_year=2000",
            "SELECT MAX(market_cap) AS max_at_2001 FROM all_symbols WHERE ipo_year=2001"])
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
            "took": 4,
            "aggregations": {
                "level2": {
                    "max_at_2000": {
                        "value": 20310000000.0
                    },
                    "doc_count": 58
                },
                "level3": {
                    "max_at_2001": {
                        "value": 8762940000.0
                    },
                    "doc_count": 38
                },
                "max_all_times": {
                    "value": 522690000000.0
                }
            },
            "timed_out": False
        })
        self.assertEqual(
            [{'max_at_2000': 20310000000.0, 'max_at_2001': 8762940000.0, 'max_all_times': 522690000000.0}],
            rows)

    def test_filter_upon_filter(self):
        executor = es_query.create_executor([
            "WITH all_symbols AS (SELECT MAX(market_cap) AS max_all_times FROM symbol)",
            "WITH year_2001 AS (SELECT MAX(market_cap) AS max_at_2000 FROM all_symbols WHERE ipo_year=2000)",
            "SELECT MAX(market_cap) AS max_at_2001_finance FROM year_2001 WHERE sector='Finance'"])
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
            "took": 3,
            "aggregations": {
                "year_2001": {
                    "max_at_2000": {
                        "value": 20310000000.0
                    },
                    "level3": {
                        "max_at_2001_finance": {
                            "value": 985668354.0
                        },
                        "doc_count": 2
                    },
                    "doc_count": 58
                },
                "max_all_times": {
                    "value": 522690000000.0
                }
            },
            "timed_out": False
        })
        self.assertEqual(
            [{'max_at_2000': 20310000000.0, 'max_all_times': 522690000000.0, 'max_at_2001_finance': 985668354.0}],
            rows)

    def test_filter_then_group_by(self):
        executor = es_query.create_executor([
            "WITH all_symbols AS (SELECT MAX(market_cap) AS max_all_times FROM symbol)",
            "WITH year_2000 AS (SELECT MAX(market_cap) AS max_at_2000 FROM all_symbols WHERE ipo_year=2000)",
            "SELECT sector, MAX(market_cap) AS max_per_sector FROM year_2000 GROUP BY sector LIMIT 2"])
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
            "took": 5,
            "aggregations": {
                "year_2000": {
                    "max_at_2000": {
                        "value": 20310000000.0
                    },
                    "sector": {
                        "buckets": [
                            {
                                "max_per_sector": {
                                    "value": 19600000000.0
                                },
                                "key": "Health Care",
                                "doc_count": 18
                            },
                            {
                                "max_per_sector": {
                                    "value": 4440000000.0
                                },
                                "key": "Technology",
                                "doc_count": 16
                            }
                        ],
                        "sum_other_doc_count": 24,
                        "doc_count_error_upper_bound": 0
                    },
                    "doc_count": 58
                },
                "max_all_times": {
                    "value": 522690000000.0
                }
            },
            "timed_out": False
        })
        self.assertEqual(
            [{"sector": "Health Care", "max_all_times": 522690000000.0, "max_at_2000": 20310000000.0,
              "max_per_sector": 19600000000.0, "_bucket_path": ["year_2000", "level3"]},
             {"sector": "Technology", "max_all_times": 522690000000.0, "max_at_2000": 20310000000.0,
              "max_per_sector": 4440000000.0, "_bucket_path": ["year_2000", "level3"]}],
            rows)
