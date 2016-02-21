import unittest

import es_monitor
import es_query
import functools


class TestQueryDatapoints(unittest.TestCase):
    def setUp(self):
        super(TestQueryDatapoints, self).setUp()
        self.old_create_executor = es_query.create_executor
        es_query.create_executor = self.create_executor

    def tearDown(self):
        es_query.create_executor = self.old_create_executor
        super(TestQueryDatapoints, self).tearDown()

    def create_executor(self, sql_selects, joinable_results):
        return self

    def execute(self):
        return [{'some_key': 'some_value'}]

    def test(self):
        datapoints = es_monitor.query_datapoints("""http://127.0.0.1:9200
SELECT * FROM abc;
SAVE RESULT AS metric1
        """)
        self.assertEqual(1, len(datapoints))
        self.assertEqual('metric1', datapoints[0]['name'])
