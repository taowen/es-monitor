import unittest

import datetime

from es_sql.sqlparse import datetime_evaluator
from es_sql.sqlparse import sql as stypes
from es_sql.sqlparse import tokens as ttypes
from es_sql.sqlparse.sql_select import SqlSelect


class TestSqlSelectProjections(unittest.TestCase):
    def test_projection_is_wildcard(self):
        sql_select = SqlSelect.parse('SELECT * FROM symbol')
        self.assertEqual(['*'], sql_select.projections.keys())
        self.assertEqual(ttypes.Wildcard, sql_select.projections['*'].ttype)
        self.assertIsNone(sql_select.where)
        self.assertEqual(dict(), sql_select.group_by)
        self.assertEqual([], sql_select.order_by)
        self.assertEqual([], sql_select.having)
        self.assertIsNone(sql_select.limit)

    def test_projection_is_function(self):
        sql_select = SqlSelect.parse('SELECT COUNT(*) FROM symbol')
        self.assertEqual(['COUNT(*)'], sql_select.projections.keys())
        self.assertEqual(stypes.Function, type(sql_select.projections['COUNT(*)']))
        self.assertEqual('COUNT', sql_select.projections['COUNT(*)'].get_name())
        self.assertEqual(ttypes.Wildcard, sql_select.projections['COUNT(*)'].get_parameters()[0].ttype)
        self.assertIsNone(sql_select.where)
        self.assertEqual(dict(), sql_select.group_by)
        self.assertEqual([], sql_select.order_by)
        self.assertEqual([], sql_select.having)
        self.assertIsNone(sql_select.limit)

    def test_projection_is_named(self):
        sql_select = SqlSelect.parse('SELECT COUNT(*) AS abc FROM symbol')
        self.assertEqual(['abc'], sql_select.projections.keys())
        self.assertEqual(stypes.Function, type(sql_select.projections['abc']))
        self.assertEqual('COUNT', sql_select.projections['abc'].get_name())
        self.assertEqual(ttypes.Wildcard, sql_select.projections['abc'].get_parameters()[0].ttype)
        self.assertIsNone(sql_select.where)
        self.assertEqual(dict(), sql_select.group_by)
        self.assertEqual([], sql_select.order_by)
        self.assertEqual([], sql_select.having)
        self.assertIsNone(sql_select.limit)

    def test_projection_is_expression_without_function(self):
        sql_select = SqlSelect.parse('SELECT a/2 AS abc FROM symbol')
        self.assertEqual(['abc'], sql_select.projections.keys())
        self.assertEqual(stypes.Expression, type(sql_select.projections['abc']))
        self.assertEqual('a/2', str(sql_select.projections['abc']))
        self.assertIsNone(sql_select.where)
        self.assertEqual(dict(), sql_select.group_by)
        self.assertEqual([], sql_select.order_by)
        self.assertEqual([], sql_select.having)
        self.assertIsNone(sql_select.limit)

    def test_projection_is_expression_with_function(self):
        sql_select = SqlSelect.parse('SELECT COUNT(*)/2 AS abc FROM symbol')
        self.assertEqual(['abc'], sql_select.projections.keys())
        self.assertEqual(stypes.Expression, type(sql_select.projections['abc']))
        self.assertEqual('COUNT(*)/2', str(sql_select.projections['abc']))
        self.assertEqual('/', sql_select.projections['abc'].operator)
        self.assertEqual(stypes.Function, type(sql_select.projections['abc'].left))
        self.assertIsNone(sql_select.where)
        self.assertEqual(dict(), sql_select.group_by)
        self.assertEqual([], sql_select.order_by)
        self.assertEqual([], sql_select.having)
        self.assertIsNone(sql_select.limit)

    def test_projections(self):
        sql_select = SqlSelect.parse('SELECT a,b FROM symbol')
        self.assertEqual(['a', 'b'], sql_select.projections.keys())

    def test_projections_mixed_with_symbol(self):
        sql_select = SqlSelect.parse('SELECT "a",b FROM symbol')
        self.assertEqual(['a', 'b'], sql_select.projections.keys())

    def test_dot(self):
        sql_select = SqlSelect.parse('SELECT a.b FROM symbol')
        self.assertEqual(['a.b'], sql_select.projections.keys())

    def test_count_distinct_dot(self):
        sql_select = SqlSelect.parse('SELECT COUNT(DISTINCT a.b) AS c FROM symbol')
        self.assertEqual('a.b', str(sql_select.projections['c'].get_parameters()[-1]))


class TestSqlSelectWhere(unittest.TestCase):
    def test_bigger_than(self):
        sql_select = SqlSelect.parse('SELECT * FROM symbol WHERE a > 0')
        comparison = sql_select.where.tokens[-1]
        self.assertEqual(stypes.Comparison, type(comparison))
        self.assertEqual('>', comparison.operator)
        self.assertEqual('a', str(comparison.left))
        self.assertEqual('0', str(comparison.right))

    def test_is(self):
        sql_select = SqlSelect.parse('SELECT * FROM symbol WHERE a IS NULL')
        comparison = sql_select.where.tokens[-1]
        self.assertEqual(stypes.Comparison, type(comparison))
        self.assertEqual('IS', comparison.operator)
        self.assertEqual('a', str(comparison.left))
        self.assertEqual('NULL', str(comparison.right))

    def test_is_not(self):
        sql_select = SqlSelect.parse('SELECT * FROM symbol WHERE a IS  NOT NULL')
        comparison = sql_select.where.tokens[-1]
        self.assertEqual(stypes.Comparison, type(comparison))
        self.assertEqual('IS  NOT', comparison.operator)
        self.assertEqual('a', str(comparison.left))
        self.assertEqual('NULL', str(comparison.right))

    def test_in(self):
        sql_select = SqlSelect.parse('SELECT * FROM symbol WHERE a IN (1,2,3)')
        comparison = sql_select.where.tokens[-1]
        self.assertEqual(stypes.Comparison, type(comparison))
        self.assertEqual('IN', comparison.operator)
        self.assertEqual('a', str(comparison.left))
        self.assertEqual('(1,2,3)', str(comparison.right))

    def test_not_in(self):
        sql_select = SqlSelect.parse('SELECT * FROM symbol WHERE a NOT IN (1,2,3)')
        comparison = sql_select.where.tokens[-1]
        self.assertEqual(stypes.Comparison, type(comparison))
        self.assertEqual('NOT IN', comparison.operator)
        self.assertEqual('a', str(comparison.left))
        self.assertEqual('(1,2,3)', str(comparison.right))

    def test_like(self):
        sql_select = SqlSelect.parse("SELECT * FROM symbol WHERE a LIKE 'abc%'")
        comparison = sql_select.where.tokens[-1]
        self.assertEqual(stypes.Comparison, type(comparison))
        self.assertEqual('LIKE', comparison.operator)
        self.assertEqual('a', str(comparison.left))
        self.assertEqual("'abc%'", str(comparison.right))

    def test_not_like(self):
        sql_select = SqlSelect.parse("SELECT * FROM symbol WHERE a NOT LIKE 'abc%'")
        comparison = sql_select.where.tokens[-1]
        self.assertEqual(stypes.Comparison, type(comparison))
        self.assertEqual('NOT LIKE', comparison.operator)
        self.assertEqual('a', str(comparison.left))
        self.assertEqual("'abc%'", str(comparison.right))

    def test_bigger_than_function_expression(self):
        sql_select = SqlSelect.parse("SELECT * FROM symbol WHERE a > now() - INTERVAL '5 DAYS'")
        comparison = sql_select.where.tokens[-1]
        self.assertEqual(stypes.Comparison, type(comparison))
        self.assertEqual('>', comparison.operator)
        self.assertEqual('a', str(comparison.left))
        self.assertEqual("now() - eval_datetime('INTERVAL', '5 DAYS')", str(comparison.right))

    def test_in(self):
        sql_select = SqlSelect.parse("SELECT * FROM symbol WHERE symbol IN ('AAPL', 'GOOG')")
        comparison = sql_select.where.tokens[-1]
        self.assertEqual(stypes.Comparison, type(comparison))
        self.assertEqual('IN', comparison.operator)
        self.assertEqual('symbol', str(comparison.left))
        self.assertEqual("('AAPL', 'GOOG')", str(comparison.right))

    def test_parameter(self):
        sql_select = SqlSelect.parse("SELECT * FROM symbol WHERE a = %(param)s")
        comparison = sql_select.where.tokens[-1]
        self.assertEqual(stypes.Comparison, type(comparison))
        self.assertEqual('=', comparison.operator)
        self.assertEqual('a', str(comparison.left))
        self.assertEqual('%(param)s', str(comparison.right))
        self.assertEqual(ttypes.Name.Placeholder, comparison.right.ttype)


class TestSqlSelectFrom(unittest.TestCase):
    def test_from_simple_index(self):
        sql_select = SqlSelect.parse("SELECT * FROM symbol GROUP BY name")
        self.assertEqual('symbol', sql_select.from_table)
        self.assertEqual('symbol*', sql_select.from_indices)

    def test_from_sinle_index(self):
        sql_select = SqlSelect.parse("SELECT * FROM index('symbol') GROUP BY name")
        self.assertEqual("index('symbol')", sql_select.from_table)
        self.assertEqual('symbol', sql_select.from_indices)

    def test_from_single_index_as_alias(self):
        sql_select = SqlSelect.parse("SELECT * FROM index('symbol') AS my_table GROUP BY name")
        self.assertEqual('my_table', sql_select.from_table)
        self.assertEqual('symbol', sql_select.from_indices)

    def test_single_year(self):
        sql_select = SqlSelect.parse("SELECT * FROM index('symbol-%Y', '2015') AS my_table GROUP BY name")
        self.assertEqual('symbol-2015', sql_select.from_indices)

    def test_single_year_month(self):
        sql_select = SqlSelect.parse("SELECT * FROM index('symbol-%Y-%m', '2015-06') AS my_table GROUP BY name")
        self.assertEqual('symbol-2015-06', sql_select.from_indices)

    def test_single_year_range(self):
        sql_select = SqlSelect.parse("SELECT * FROM index('symbol-%Y-%m-%d', '2015-01-01', '2015-01-03') AS my_table GROUP BY name")
        self.assertEqual('symbol-2015-01-01,symbol-2015-01-02,symbol-2015-01-03', sql_select.from_indices)

    def test_support_now_and_interval(self):
        datetime_evaluator.NOW = datetime.datetime(2015, 1, 2)
        sql_select = SqlSelect.parse(
            "SELECT * FROM index('symbol-%Y-%m-%d', now()-interval('1 days'), timestamp('2015-01-03 00:00:00')) "
            "AS my_table GROUP BY name")
        self.assertEqual('symbol-2015-01-01,symbol-2015-01-02,symbol-2015-01-03', sql_select.from_indices)

    def test_multiple_index(self):
        sql_select = SqlSelect.parse("SELECT * FROM (index('symbol') UNION index('quote')) AS my_table GROUP BY name")
        self.assertEqual('symbol,quote', sql_select.from_indices)

    def test_except_index(self):
        sql_select = SqlSelect.parse("SELECT * FROM (index('symbol') EXCEPT index('quote')) AS my_table GROUP BY name")
        self.assertEqual('symbol,-quote', sql_select.from_indices)


class TestSqlSelectGroupBy(unittest.TestCase):
    def test_group_by_one_field(self):
        sql_select = SqlSelect.parse("SELECT * FROM symbol GROUP BY name")
        self.assertEqual(['name'], sql_select.group_by.keys())
        self.assertEqual(ttypes.Name, sql_select.group_by['name'].ttype)

    def test_group_by_multiple_fields(self):
        sql_select = SqlSelect.parse("SELECT * FROM symbol GROUP BY name1, name2")
        self.assertEqual(['name1', 'name2'], sql_select.group_by.keys())
        self.assertEqual(ttypes.Name, sql_select.group_by['name1'].ttype)
        self.assertEqual(ttypes.Name, sql_select.group_by['name2'].ttype)

    def test_group_by_function(self):
        sql_select = SqlSelect.parse("SELECT * FROM symbol GROUP BY date_trunc('minute', ts) AS m")
        self.assertEqual(['m'], sql_select.group_by.keys())
        self.assertEqual(stypes.Function, type(sql_select.group_by['m']))

    def test_group_by_case_when(self):
        sql_select = SqlSelect.parse(
            "SELECT * FROM symbol GROUP BY CASE WHEN price > 10 THEN 'high' ELSE 'low' END AS hl")
        self.assertEqual(['hl'], sql_select.group_by.keys())
        self.assertEqual(stypes.Case, type(sql_select.group_by['hl']))
        sql_select = SqlSelect.parse(
            "SELECT * FROM symbol GROUP BY ( CASE WHEN price > 10 THEN 'high' ELSE 'low' END) AS hl")
        self.assertEqual(['hl'], sql_select.group_by.keys())
        self.assertEqual(stypes.Case, type(sql_select.group_by['hl']))


class TestSqlSelectOrderBy(unittest.TestCase):
    def test_order_by_one_field(self):
        sql_select = SqlSelect.parse("SELECT * FROM symbol ORDER BY name")
        self.assertEqual(1, len(sql_select.order_by))
        self.assertEqual('name', str(sql_select.order_by[0]))

    def test_order_by_multiple_fields(self):
        sql_select = SqlSelect.parse("SELECT * FROM symbol ORDER BY name1, name2")
        self.assertEqual(2, len(sql_select.order_by))
        self.assertEqual('name1', str(sql_select.order_by[0]))
        self.assertEqual('name2', str(sql_select.order_by[1]))

    def test_order_by_asc(self):
        sql_select = SqlSelect.parse("SELECT * FROM symbol ORDER BY name ASC")
        self.assertEqual(1, len(sql_select.order_by))
        self.assertEqual('name ASC', str(sql_select.order_by[0]))

    def test_order_by_desc(self):
        sql_select = SqlSelect.parse("SELECT * FROM symbol ORDER BY name   DESC")
        self.assertEqual(1, len(sql_select.order_by))
        self.assertEqual('name   DESC', str(sql_select.order_by[0]))


class TestSqlSelectHaving(unittest.TestCase):
    def test_bigger_than(self):
        sql_select = SqlSelect.parse('SELECT * FROM symbol HAVING a > 0 ORDER BY name')
        comparison = sql_select.having[-2]
        self.assertEqual(stypes.Comparison, type(comparison))
        self.assertEqual('>', comparison.operator)
        self.assertEqual('a', str(comparison.left))
        self.assertEqual('0', str(comparison.right))


class TestSqlSelectLimit(unittest.TestCase):
    def test_limit(self):
        sql_select = SqlSelect.parse('SELECT * FROM symbol LIMIT 1')
        self.assertEqual(1, sql_select.limit)


class TestSqlSelectJoin(unittest.TestCase):
    def test_join_one_field(self):
        sql_select = SqlSelect.parse(
            'SELECT * FROM quote JOIN matched_symbols ON quote.symbol = matched_symbols.symbol')
        self.assertEqual('matched_symbols', sql_select.join_table)
        self.assertTrue(len(sql_select.join_conditions) > 0)

    def test_where_should_not_be_part_of_join_condition(self):
        sql_select = SqlSelect.parse(
            """select  phone from cn_tag_data_info_es
join base_info on  cn_tag_data_info_es.phone = base_info.phone
where cn_tag_data_info_es.date_str='2016-07-07'
group by phone
            """)
        self.assertIsNotNone(sql_select.where)

    def test_group_by_and_having(self):
        sql_select = SqlSelect.parse(
            """SELECT phone, sum(cr_car_xiangqing_count) as total_count
  FROM cn_tag_data_info_es
      JOIN base_info ON cn_tag_data_info_es.phone = base_info.phone
 WHERE date_str >= "2016-05-01" and date_str <= "2016-05-03"
 GROUP BY phone
 HAVING total_count > 9""")
        self.assertIsNotNone(sql_select.where)
        self.assertIsNotNone(sql_select.having)
