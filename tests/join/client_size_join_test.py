import unittest
import es_query
from sqlparse.sql_select import SqlSelect


class TestClientSideJoin(unittest.TestCase):
    def test_join_on_one_field(self):
        sql_select = SqlSelect.parse(
            'SELECT * FROM quote JOIN matched_symbols ON quote.symbol = matched_symbols.symbol')
        sql_select.joinable_results['matched_symbols'] = [
            {'symbol': '1'},
            {'symbol': '2'}
        ]
        executor = es_query.create_executor(sql_select)
        self.assertEqual(
            {'query': {'bool': {'filter': {}, 'should': [{'term': {u'symbol': '1'}}, {'term': {u'symbol': '2'}}]}}},
            executor.request)

    def test_join_on_two_fields(self):
        sql_select = SqlSelect.parse(
            'SELECT * FROM quote JOIN '
            'matched_symbols ON quote.symbol = matched_symbols.symbol '
            'AND quote.date = matched_symbols.date')
        sql_select.joinable_results['matched_symbols'] = [
            {'symbol': '1', 'date': '1998'},
            {'symbol': '2', 'date': '1998'}
        ]
        executor = es_query.create_executor(sql_select)
        self.assertEqual(
            {'query': {'bool': {'filter': {}, 'should': [
                {'bool': {'filter': [{'term': {u'symbol': '1'}}, {'term': {u'date': '1998'}}]}},
                {'bool': {'filter': [{'term': {u'symbol': '2'}}, {'term': {u'date': '1998'}}]}}]}}},
            executor.request)

    def test_select_inside_join(self):
        sql_select = SqlSelect.parse(
            'SELECT COUNT(*) FROM quote JOIN '
            'matched_symbols ON quote.symbol = matched_symbols.symbol '
            'AND quote.date = matched_symbols.date')
        sql_select.joinable_results['matched_symbols'] = [
            {'symbol': '1', 'date': '1998'},
            {'symbol': '2', 'date': '1998'}
        ]
        executor = es_query.create_executor(sql_select)
        self.assertEqual(
            {'query': {'bool': {'filter': {}, 'should': [
                {'bool': {'filter': [{'term': {u'symbol': '1'}}, {'term': {u'date': '1998'}}]}},
                {'bool': {'filter': [{'term': {u'symbol': '2'}}, {'term': {u'date': '1998'}}]}}]}}, 'aggs': {},
             'size': 0},
            executor.request)
