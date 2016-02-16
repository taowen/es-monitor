import unittest
import es_query


class TestSelectFromLeafProjections(unittest.TestCase):
    def test_field_eq_string(self):
        executor = es_query.create_executor("SELECT * FROM symbol WHERE exchange='nyse'")
        self.assertEqual({'query': {'term': {'exchange': 'nyse'}}}, executor.request)

    def test_and(self):
        executor = es_query.create_executor("SELECT * FROM symbol WHERE exchange='nyse' AND sector='Technology'")
        self.assertEqual(
            {'query': {'bool': {'filter': [{'term': {'exchange': 'nyse'}}, {'term': {'sector': 'Technology'}}]}}},
            executor.request)

    def test_field_gt_numeric(self):
        executor = es_query.create_executor("SELECT * FROM symbol WHERE last_sale > 1000")
        self.assertEqual(
            {'query': {'range': {'last_sale': {'gt': 1000.0}}}},
            executor.request)

    def test_field_gte_numeric(self):
        executor = es_query.create_executor("SELECT * FROM symbol WHERE last_sale >= 1000")
        self.assertEqual(
            {'query': {'range': {'last_sale': {'gte': 1000.0}}}},
            executor.request)

    def test_field_lt_numeric(self):
        executor = es_query.create_executor("SELECT * FROM symbol WHERE last_sale < 1000")
        self.assertEqual(
            {'query': {'range': {'last_sale': {'lt': 1000.0}}}},
            executor.request)

    def test_field_lte_numeric(self):
        executor = es_query.create_executor("SELECT * FROM symbol WHERE last_sale <= 1000")
        self.assertEqual(
            {'query': {'range': {'last_sale': {'lte': 1000.0}}}},
            executor.request)

    def test_field_not_eq_numeric(self):
        executor = es_query.create_executor("SELECT * FROM symbol WHERE last_sale != 1000")
        self.assertEqual(
            {'query': {'not': {'term': {'last_sale': 1000}}}},
            executor.request)
