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

    def test_and_not(self):
        executor = es_query.create_executor("SELECT * FROM symbol WHERE exchange='nyse' AND NOT sector='Technology'")
        self.assertEqual(
            {'query': {'bool': {
                'filter': [{'term': {'exchange': 'nyse'}}],
                'must_not': [{'term': {'sector': 'Technology'}}]}}},
            executor.request)

    def test_not_and_not(self):
        executor = es_query.create_executor(
            "SELECT * FROM symbol WHERE NOT exchange='nyse' AND NOT sector='Technology'")
        self.assertEqual(
            {'query': {'bool': {
                'must_not': [{'term': {'exchange': 'nyse'}}, {'term': {'sector': 'Technology'}}]}}},
            executor.request)

    def test_and_and(self):
        executor = es_query.create_executor(
            "SELECT * FROM symbol WHERE exchange='nyse' AND sector='Technology' AND ipo_year=1998")
        self.assertEqual(
            {'query': {'bool': {'filter': [
                {'term': {'exchange': 'nyse'}},
                {'term': {'sector': 'Technology'}},
                {'term': {'ipo_year': 1998}},
            ]}}},
            executor.request)

    def test_or(self):
        executor = es_query.create_executor("SELECT * FROM symbol WHERE exchange='nyse' OR sector='Technology'")
        self.assertEqual(
            {'query': {'bool': {'should': [{'term': {'exchange': 'nyse'}}, {'term': {'sector': 'Technology'}}]}}},
            executor.request)

    def test_or_not(self):
        executor = es_query.create_executor("SELECT * FROM symbol WHERE exchange='nyse' OR NOT sector='Technology'")
        self.assertEqual(
            {'query': {'bool': {'should': [
                {'term': {'exchange': 'nyse'}},
                {'bool': {'must_not': [{'term': {'sector': 'Technology'}}]}}]}}},
            executor.request)

    def test_and_or_must_use_parentheses(self):
        try:
            executor = es_query.create_executor(
                "SELECT * FROM symbol WHERE exchange='nyse' AND sector='Technology' OR ipo_year > 1998")
        except:
            return
        self.fail('should fail')

    def test_and_or_used_parentheses(self):
        executor = es_query.create_executor(
            "SELECT * FROM symbol WHERE exchange='nyse' AND (sector='Technology' OR ipo_year > 1998)")
        self.assertEqual(
            {'query': {'bool': {'filter': [
                {'term': {'exchange': 'nyse'}},
                {'bool': {'should': [
                    {'term': {'sector': 'Technology'}},
                    {'range': {'ipo_year': {'gt': 1998.0}}}]}}
            ]}}},
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
            {'query': {'bool': {'must_not': {'term': {'last_sale': 1000}}}}},
            executor.request)

    def test_field_in_range(self):
        executor = es_query.create_executor("SELECT * FROM symbol WHERE last_sale > 500 AND last_sale < 600")
        self.assertEqual(
            {'query': {'range': {'last_sale': {'lt': 600.0, 'gt': 500.0}}}},
            executor.request)

    def test_field_in_range_not_merged(self):
        executor = es_query.create_executor("SELECT * FROM symbol WHERE last_sale > 500 AND last_sale > 600")
        self.assertEqual(
            {'query': {'bool': {'filter': [
                {'range': {'last_sale': {'gt': 500.0}}},
                {'range': {'last_sale': {'gt': 600.0}}}]
            }}},
            executor.request)
