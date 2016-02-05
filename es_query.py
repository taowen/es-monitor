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
import select_inside_translator

DEBUG = False


def execute_sql(es_hosts, sql):
    statement = sqlparse.parse(sql.strip())[0]
    sql_select = SqlSelect()
    sql_select.on_SELECT(statement.tokens)
    rows = execute_sql_select(es_hosts, sql_select)
    for row in rows:
        row.pop('_bucket_')
    return rows


def execute_sql_select(es_hosts, sql_select, inner_aggs=None):
    request, select_response = select_inside_translator.translate_select(sql_select)
    inner_aggs = inner_aggs or {}
    if inner_aggs:
        outter_aggs = request['aggs']['_global_']['aggs']
        if sql_select.group_by:
            for group_by_name in sql_select.group_by.keys():
                outter_aggs = outter_aggs[group_by_name]['aggs']
        outter_aggs.update(inner_aggs)
    if DEBUG:
        print('=====')
        print(json.dumps(request, indent=2))
    if isinstance(sql_select.select_from, basestring):
        url = es_hosts + '/%s*/_search' % sql_select.select_from
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
        if in_mem_computation.is_in_mem_computation(sql_select):
            response = execute_sql_select(es_hosts, sql_select.select_from)
            return in_mem_computation.do_in_mem_computation(sql_select, response)
        else:
            if sql_select.is_select_inside:
                if 'aggs' not in request:
                    raise Exception('SELECT ... INSIDE ... can only nest aggregation query')
                response = execute_sql_select(es_hosts, sql_select.select_from, request['aggs'])
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
