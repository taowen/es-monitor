import datetime
import unittest

from es_sql import es_query
from es_sql.sqlparse import datetime_evaluator


class TestSelectFromLeafWhere(unittest.TestCase):
    def test_field_eq_string(self):
        executor = es_query.create_executor("SELECT * FROM symbol WHERE exchange='nyse'")
        self.assertEqual({'query': {'term': {'exchange': 'nyse'}}}, executor.request)
        executor = es_query.create_executor("SELECT * FROM symbol WHERE exchange=%(exchange)s")
        self.assertEqual({
            'query': {'term': {u'exchange': '%(exchange)s'}},
            '_parameters_': {u'exchange': {
                'path': ['query', 'term', u'exchange'],
                'field_hint': 'exchange'
            }}}
            , executor.request)

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
        executor = es_query.create_executor("SELECT * FROM symbol WHERE last_sale > %(param1)s")
        self.assertEqual(
            {'query': {'range': {u'last_sale': {'gt': '%(param1)s'}}},
             '_parameters_': {u'param1': {
                 'path': ['query', 'range', u'last_sale', 'gt'],
                 'field_hint': 'last_sale'
             }}},
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
        executor = es_query.create_executor("SELECT * FROM symbol WHERE 1000 != last_sale")
        self.assertEqual(
            {'query': {'bool': {'must_not': {'term': {'last_sale': 1000}}}}},
            executor.request)
        executor = es_query.create_executor("SELECT * FROM symbol WHERE last_sale != %(param1)s")
        self.assertEqual(
            {'query': {'bool': {'must_not': {'term': {u'last_sale': '%(param1)s'}}}}, '_parameters_': {
                u'param1': {'path': ['query', 'bool', 'must_not', 'term', u'last_sale'], 'field_hint': u'last_sale'}}},
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

    def test_is_null(self):
        executor = es_query.create_executor("SELECT * FROM symbol WHERE last_sale IS NULL")
        self.assertEqual(
            {'query': {'bool': {'must_not': {'exists': {'field': 'last_sale'}}}}},
            executor.request)

    def test_is_not_null(self):
        executor = es_query.create_executor("SELECT * FROM symbol WHERE last_sale IS  NOT NULL")
        self.assertEqual(
            {'query': {'exists': {'field': 'last_sale'}}},
            executor.request)

    def test_field_can_be_right_operand(self):
        executor = es_query.create_executor("SELECT * FROM symbol WHERE 'nyse'=exchange")
        self.assertEqual({'query': {'term': {'exchange': 'nyse'}}}, executor.request)
        executor = es_query.create_executor("SELECT * FROM symbol WHERE 1998<ipo_year")
        self.assertEqual({'query': {'range': {'ipo_year': {'gte': 1998.0}}}}, executor.request)

    def test_dot_field(self):
        executor = es_query.create_executor("SELECT * FROM symbol WHERE 'nyse'=\"a.exchange\"")
        self.assertEqual({'query': {'term': {'a.exchange': 'nyse'}}}, executor.request)

    def test_now(self):
        datetime_evaluator.NOW = datetime.datetime(2016, 8, 8)
        executor = es_query.create_executor("SELECT * FROM symbol WHERE ts > now()")
        self.assertEqual({'query': {'range': {'ts': {'gt': 1470585600000L}}}}, executor.request)

    def test_now_expression(self):
        datetime_evaluator.NOW = datetime.datetime(2016, 8, 8)
        executor = es_query.create_executor("SELECT * FROM symbol WHERE ts > now() - INTERVAL '1 DAY'")
        self.assertEqual({'query': {'range': {'ts': {'gt': 1470585600000L - 24 * 60 * 60 * 1000}}}}, executor.request)

    def test_today_expression(self):
        datetime_evaluator.NOW = datetime.datetime(2016, 8, 8)
        executor = es_query.create_executor("SELECT * FROM symbol WHERE ts > today() - interval('1 day')")
        self.assertEqual({'query': {'range': {'ts': {'gt': 1470585600000L - 24 * 60 * 60 * 1000}}}}, executor.request)

    def test_timestamp(self):
        executor = es_query.create_executor("SELECT * FROM symbol WHERE ts > TIMESTAMP '2016-08-08 00:00:00'")
        self.assertEqual({'query': {'range': {'ts': {'gt': 1470585600000L}}}}, executor.request)

    def test_in(self):
        executor = es_query.create_executor("SELECT * FROM symbol WHERE symbol IN ('AAPL', 'GOOG')")
        self.assertEqual({'query': {'terms': {u'symbol': ['AAPL', 'GOOG']}}}, executor.request)
        executor = es_query.create_executor("SELECT * FROM symbol WHERE symbol IN %(param1)s")
        self.assertEqual(
            {'query': {'terms': {u'symbol': '%(param1)s'}},
             '_parameters_': {u'param1': {'path': ['query', 'terms', u'symbol'], 'field_hint': u'symbol'}}},
            executor.request)

    def test_type_eq(self):
        executor = es_query.create_executor("SELECT * FROM symbol WHERE _type = 'symbol'")
        self.assertEqual({'query': {'type': {'value': 'symbol'}}}, executor.request)

    def test_id_eq(self):
        executor = es_query.create_executor("SELECT * FROM symbol WHERE _id = '1'")
        self.assertEqual({'query': {'ids': {'value': ['1']}}}, executor.request)

    def test_id_in(self):
        executor = es_query.create_executor("SELECT * FROM symbol WHERE _id IN ('1', '2')")
        self.assertEqual({'query': {'ids': {'value': ['1', '2']}}}, executor.request)

    def test_type_merged_with_ids(self):
        executor = es_query.create_executor("SELECT * FROM symbol WHERE _type = 'symbol' AND _id = '1'")
        self.assertEqual({'query': {'ids': {'value': ['1'], 'type': 'symbol'}}}, executor.request)
        executor = es_query.create_executor("SELECT * FROM symbol WHERE _type = 'symbol' AND _id IN ('1', '2')")
        self.assertEqual({'query': {'ids': {'value': ['1', '2'], 'type': 'symbol'}}}, executor.request)
        executor = es_query.create_executor("SELECT * FROM symbol WHERE _id IN ('1', '2') AND _type = 'symbol'")
        self.assertEqual({'query': {'ids': {'value': ['1', '2'], 'type': 'symbol'}}}, executor.request)
        executor = es_query.create_executor(
            "SELECT * FROM symbol WHERE _id IN ('1', '2') AND _type = 'symbol' AND _type='abc'")
        self.assertEqual({'query': {
            'bool': {'filter': [{'ids': {'type': 'symbol', 'value': ['1', '2']}}, {'type': {'value': 'abc'}}]}}},
            executor.request)

    def test_like(self):
        executor = es_query.create_executor("SELECT * FROM symbol WHERE symbol LIKE 'AAP%'")
        self.assertEqual({'query': {'wildcard': {u'symbol': 'AAP*'}}}, executor.request)
        executor = es_query.create_executor("SELECT * FROM symbol WHERE symbol LIKE %(param1)s")
        self.assertEqual(
            {'query': {'wildcard': {u'symbol': '%(param1)s'}}, '_parameters_': {
                u'param1': {'path': ['query', 'wildcard', u'symbol'], 'field_hint': u'symbol'}}},
            executor.request)
