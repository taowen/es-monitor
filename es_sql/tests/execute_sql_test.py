import unittest

from es_sql import es_query


class TestExecuteSQL(unittest.TestCase):
    def setUp(self):
        super(TestExecuteSQL, self).setUp()
        self.old_create_executor = es_query.create_executor
        es_query.create_executor = self.create_executor

    def tearDown(self):
        es_query.create_executor = self.old_create_executor
        super(TestExecuteSQL, self).tearDown()

    def create_executor(self, sql_selects, joinable_results):
        return self

    def execute(self, es_url, arguments):
        return [{'some_key': 'some_value'}]

    def test_no_save_as(self):
        result_map = es_query.execute_sql(None, """
            SELECT * FROM abc
        """)
        self.assertEqual(['result'], result_map.keys())

    def test_save_as(self):
        result_map = es_query.execute_sql(None, """
            SELECT * FROM abc;
            SAVE RESULT AS result1;
        """)
        self.assertEqual(['result1'], result_map.keys())

    def test_remove(self):
        result_map = es_query.execute_sql(None, """
            SELECT * FROM abc;
            SAVE RESULT AS result1;
            REMOVE RESULT result1;
        """)
        self.assertEqual([], result_map.keys())

    def test_python_code(self):
        result_map = es_query.execute_sql(None, """
            result_map['result1'] = []
        """)
        self.assertEqual(['result1'], result_map.keys())

    def test_complex_python_code(self):
        result_map = es_query.execute_sql(None, """
result_map['result1'] = []
for i in range(100):
    result_map['result1'].append(i)
        """)
        self.assertEqual(['result1'], result_map.keys())
