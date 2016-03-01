import unittest
from es_sql import es_query
from es_sql.sqlparse.sql_select import SqlSelect


class TestClientSideJoin(unittest.TestCase):
    def test_join_on_one_field(self):
        executor = es_query.create_executor([
            "WITH finance_symbols AS (SELECT * FROM symbol WHERE sector='Finance')",
            'SELECT * FROM quote JOIN finance_symbols ON quote.symbol = finance_symbols.symbol'
        ])
        self.assertEqual(
            {'query': {'bool': {'filter': [
                {}, {'filterjoin': {
                    u'symbol': {'indices': u'symbol*', 'path': u'symbol',
                                'query': {'term': {u'sector': 'Finance'}}}}}]
            }}},
            executor.request)
