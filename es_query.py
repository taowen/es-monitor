#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Query elasticsearch using SQL
"""
import json
import sys
import urllib2
import re

import sqlparse
from executors import SelectFromAllBucketsExecutor
from executors import SelectFromPerBucketExecutor
from executors import SelectFromInMemExecutor
from executors import SelectFromLeafExecutor
from executors import SelectInsideBranchExecutor
from executors import SelectInsideLeafExecutor
from sqlparse.sql_select import SqlSelect

DEBUG = False
ES_HOSTS = None


def execute_sql(es_hosts, sql):
    global ES_HOSTS

    ES_HOSTS = es_hosts
    return create_executor(sql.split(';')).execute()


def create_executor(sql_selects):
    executor_map = {}
    if not isinstance(sql_selects, list):
        sql_selects = [sql_selects]
    root_executor = None
    level = 0
    for sql_select in sql_selects:
        level += 1
        sql_select = sql_select.strip()
        if not sql_select:
            continue
        match = re.match(r'^WITH\s+(.*)\s+AS\s+(.*)\s*$', sql_select, re.IGNORECASE | re.DOTALL)
        executor_name = None
        if match:
            sql_select = match.group(1)
            executor_name = match.group(2)
        else:
            executor_name = 'level%s' % level
        sql_select = SqlSelect.parse(sql_select)
        if not isinstance(sql_select.source, basestring):
            raise Exception('nested SELECT is not supported')
        if sql_select.source in executor_map:
            parent_executor = executor_map[sql_select.source]
            executor = SelectInsideBranchExecutor(sql_select, executor_name)
            parent_executor.add_child(executor)
        else:
            if sql_select.is_select_inside:
                executor = SelectInsideLeafExecutor(sql_select, search_es)
            else:
                executor = SelectFromLeafExecutor(sql_select, search_es)
            if root_executor:
                raise Exception('multiple root executor is not supported')
            root_executor = executor
        executor_map[executor_name] = executor
    root_executor.build_request()
    return root_executor


def search_es(index, request):
    if DEBUG:
        print('=====')
        print(json.dumps(request, indent=2))
    url = ES_HOSTS + '/%s*/_search' % index
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
    return response


if __name__ == "__main__":
    DEBUG = True
    sql = sys.stdin.read()
    rows = execute_sql(sys.argv[1], sql)
    print('=====')
    for row in rows:
        print json.dumps(row)
    sys.exit(0 if rows else 1)
