import unittest
from es_sql import es_query
from es_sql.sqlparse.sql_select import SqlSelect


class TestClientSideJoin(unittest.TestCase):
    def test_join_on_one_field(self):
        executor = es_query.create_executor(
            'SELECT * FROM quote JOIN matched_symbols ON quote.symbol = matched_symbols.symbol', {
                'matched_symbols': [
                    {'symbol': '1'},
                    {'symbol': '2'}
                ]
            })
        self.assertEqual(
            {'query': {'bool': {'filter': [{}, {'terms': {u'symbol': ['1', '2']}}]}}},
            executor.request)

    def test_join_on_two_fields(self):
        executor = es_query.create_executor(
            'SELECT * FROM quote JOIN '
            'matched_symbols ON quote.symbol = matched_symbols.symbol '
            'AND quote.date = matched_symbols.date', {
                'matched_symbols': [
                    {'symbol': '1', 'date': '1998'},
                    {'symbol': '2', 'date': '1998'}
                ]
            }
        )
        self.assertEqual(
            {'query': {'bool': {'filter': {}, 'should': [
                {'bool': {'filter': [{'term': {u'symbol': '1'}}, {'term': {u'date': '1998'}}]}},
                {'bool': {'filter': [{'term': {u'symbol': '2'}}, {'term': {u'date': '1998'}}]}}]}}},
            executor.request)

    def test_select_inside_join(self):
        executor = es_query.create_executor(
            'SELECT COUNT(*) FROM quote JOIN '
            'matched_symbols ON quote.symbol = matched_symbols.symbol '
            'AND quote.date = matched_symbols.date', {
                'matched_symbols': [
                    {'symbol': '1', 'date': '1998'},
                    {'symbol': '2', 'date': '1998'}
                ]
            }
        )
        self.assertEqual(
            {'query': {'bool': {'filter': {}, 'should': [
                {'bool': {'filter': [{'term': {u'symbol': '1'}}, {'term': {u'date': '1998'}}]}},
                {'bool': {'filter': [{'term': {u'symbol': '2'}}, {'term': {u'date': '1998'}}]}}]}}, 'aggs': {},
             'size': 0},
            executor.request)
