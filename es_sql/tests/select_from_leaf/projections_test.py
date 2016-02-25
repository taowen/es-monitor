import unittest

from es_sql import es_query


class TestSelectFromLeafProjections(unittest.TestCase):
    def test_select_all(self):
        executor = es_query.create_executor('SELECT * FROM symbol')
        self.assertIsNotNone(executor)
        self.assertEqual({}, executor.request)
        rows = executor.select_response({
            "hits": {
                "hits": [
                    {
                        "_score": 1.0,
                        "_type": "symbol",
                        "_id": "AVLgXwu88_EnCX8dV9PN",
                        "_source": {
                            "exchange": "nasdaq"
                        },
                        "_index": "symbol"
                    }
                ],
                "total": 6714,
                "max_score": 1.0
            },
            "_shards": {
                "successful": 3,
                "failed": 0,
                "total": 3
            },
            "took": 32,
            "timed_out": False
        })
        self.assertEqual([{
            '_id': 'AVLgXwu88_EnCX8dV9PN', '_type': 'symbol',
            '_index': 'symbol', 'exchange': 'nasdaq'}],
            rows)

    def test_select_one_field(self):
        executor = es_query.create_executor('SELECT exchange FROM symbol')
        self.assertEqual({}, executor.request)
        rows = executor.select_response({
            "hits": {
                "hits": [
                    {
                        "_score": 1.0,
                        "_type": "symbol",
                        "_id": "AVLgXwu88_EnCX8dV9PN",
                        "_source": {
                            "exchange": "nasdaq"
                        },
                        "_index": "symbol"
                    }
                ],
                "total": 6714,
                "max_score": 1.0
            },
            "_shards": {
                "successful": 3,
                "failed": 0,
                "total": 3
            },
            "took": 32,
            "timed_out": False
        })
        self.assertEqual([{'exchange': 'nasdaq'}], rows)

    def test_select_system_field(self):
        executor = es_query.create_executor('SELECT _id, _type, _index FROM symbol')
        self.assertEqual({}, executor.request)
        rows = executor.select_response({
            "hits": {
                "hits": [
                    {
                        "_score": 1.0,
                        "_type": "symbol",
                        "_id": "AVLgXwu88_EnCX8dV9PN",
                        "_source": {
                            "exchange": "nasdaq"
                        },
                        "_index": "symbol"
                    }
                ],
                "total": 6714,
                "max_score": 1.0
            },
            "_shards": {
                "successful": 3,
                "failed": 0,
                "total": 3
            },
            "took": 32,
            "timed_out": False
        })
        self.assertEqual([{
            '_id': 'AVLgXwu88_EnCX8dV9PN', '_type': 'symbol',
            '_index': 'symbol'}],
            rows)

    def test_select_nested_field(self):
        executor = es_query.create_executor('SELECT "a.exchange" FROM symbol')
        self.assertEqual({}, executor.request)
        rows = executor.select_response({
            "hits": {
                "hits": [
                    {
                        "_score": 1.0,
                        "_type": "symbol",
                        "_id": "AVLgXwu88_EnCX8dV9PN",
                        "_source": {
                            "a": {
                                "exchange": "nasdaq"
                            }
                        },
                        "_index": "symbol"
                    }
                ],
                "total": 6714,
                "max_score": 1.0
            },
            "_shards": {
                "successful": 3,
                "failed": 0,
                "total": 3
            },
            "took": 32,
            "timed_out": False
        })
        self.assertEqual([{
            'a.exchange': 'nasdaq'}],
            rows)

    def test_select_nested_field_via_dot(self):
        executor = es_query.create_executor('SELECT a.exchange FROM symbol')
        self.assertEqual({}, executor.request)
        rows = executor.select_response({
            "hits": {
                "hits": [
                    {
                        "_score": 1.0,
                        "_type": "symbol",
                        "_id": "AVLgXwu88_EnCX8dV9PN",
                        "_source": {
                            "a": {
                                "exchange": "nasdaq"
                            }
                        },
                        "_index": "symbol"
                    }
                ],
                "total": 6714,
                "max_score": 1.0
            },
            "_shards": {
                "successful": 3,
                "failed": 0,
                "total": 3
            },
            "took": 32,
            "timed_out": False
        })
        self.assertEqual([{
            'a.exchange': 'nasdaq'}],
            rows)


    def test_select_expression(self):
        executor = es_query.create_executor('SELECT "a.price"/2 FROM symbol')
        self.assertEqual({}, executor.request)
        rows = executor.select_response({
            "hits": {
                "hits": [
                    {
                        "_score": 1.0,
                        "_type": "symbol",
                        "_id": "AVLgXwu88_EnCX8dV9PN",
                        "_source": {
                            "a": {
                                "price": 100
                            }
                        },
                        "_index": "symbol"
                    }
                ],
                "total": 6714,
                "max_score": 1.0
            },
            "_shards": {
                "successful": 3,
                "failed": 0,
                "total": 3
            },
            "took": 32,
            "timed_out": False
        })
        self.assertEqual([{
            '"a.price"/2': 50}],
            rows)
