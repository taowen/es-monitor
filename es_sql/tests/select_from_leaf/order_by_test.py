import unittest

from es_sql import es_query


class TestSelectFromLeafOrderBy(unittest.TestCase):
    def test_order_by(self):
        executor = es_query.create_executor("SELECT * FROM symbol ORDER BY name")
        self.assertEqual({'sort': {'name': 'asc'}}, executor.request)

    def test_order_by_asc(self):
        executor = es_query.create_executor("SELECT * FROM symbol ORDER BY name  asc")
        self.assertEqual({'sort': {'name': 'asc'}}, executor.request)

    def test_order_by_desc(self):
        executor = es_query.create_executor("SELECT * FROM symbol ORDER BY name DESC")
        self.assertEqual({'sort': {'name': 'desc'}}, executor.request)
