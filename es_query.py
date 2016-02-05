#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Query elasticsearch using SQL
"""
import sys
import urllib2
import json

import sqlparse
from sqlparse.sql_select import SqlSelect
import in_mem_computation
import translators

DEBUG = False


def execute_sql(es_hosts, sql):
    global ES_HOSTS

    ES_HOSTS = es_hosts
    statement = sqlparse.parse(sql.strip())[0]
    return SqlExecutor.create(statement.tokens).execute()


class SqlExecutor(object):
    def __init__(self, sql_select):
        # internal state
        self.sql_select = sql_select
        self.include_bucket_in_row = False

    @classmethod
    def create(cls, tokens):
        sql_select = SqlSelect()
        sql_select.on_SELECT(tokens)
        return SqlExecutor(sql_select)

    def execute(self, inner_aggs=None):
        rows = self._execute(inner_aggs)
        if not self.include_bucket_in_row:
            for row in rows:
                row.pop('_bucket_')
        return rows

    def _execute(self, inner_aggs=None):
        request, select_response = translators.translate_select(self.sql_select)
        inner_aggs = inner_aggs or {}
        if inner_aggs:
            outter_aggs = request['aggs']['_global_']['aggs']
            if self.sql_select.group_by:
                for group_by_name in self.sql_select.group_by.keys():
                    outter_aggs = outter_aggs[group_by_name]['aggs']
            outter_aggs.update(inner_aggs)
        if DEBUG:
            print('=====')
            print(json.dumps(request, indent=2))
        if isinstance(self.sql_select.select_from, basestring):
            url = ES_HOSTS + '/%s*/_search' % self.sql_select.select_from
            try:
                resp = urllib2.urlopen(url, json.dumps(request)).read()
            except urllib2.HTTPError as e:
                sys.stderr.write(e.read())
                return
            except:
                import traceback

                sys.stderr.write(traceback.format_exc())
                return
            response = json.loads(resp)
            if DEBUG:
                print('=====')
                print(json.dumps(response, indent=2))
            return select_response(response)
        else:
            if in_mem_computation.is_in_mem_computation(self.sql_select):
                response = SqlExecutor.create(self.sql_select.select_from.tokens).execute()
                return select_response(response)
            else:
                if self.sql_select.is_inside_query:
                    inner_executor = SqlExecutor.create(self.sql_select.select_from.tokens)
                    inner_executor.include_bucket_in_row = True
                    if 'aggs' not in request:
                        raise Exception('SELECT ... INSIDE ... can only nest aggregation query')
                    inner_rows = inner_executor.execute(self.request['aggs'])
                    response = inner_rows
                    return select_response(response)
                else:
                    print(request)
                    raise Exception('not implemented')


if __name__ == "__main__":
    DEBUG = True
    sql = sys.stdin.read()
    rows = execute_sql(sys.argv[1], sql)
    print('=====')
    for row in rows:
        print json.dumps(row)
    sys.exit(0 if rows else 1)
