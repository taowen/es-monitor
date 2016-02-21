#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Query elasticsearch using SQL
"""
import json
import sys
import urllib2
import re
import functools

from executors import SelectFromLeafExecutor
from executors import SelectInsideBranchExecutor
from executors import SelectInsideLeafExecutor
from sqlparse.sql_select import SqlSelect

DEBUG = False
ES_HOSTS = None


def execute_sql(es_hosts, sql):
    global ES_HOSTS

    ES_HOSTS = es_hosts
    current_sql_selects = []
    result_map = {}
    for sql_select in sql.split(';'):
        sql_select = sql_select.strip()
        if not sql_select:
            continue
        is_sql = re.match(r'^(WITH|SELECT)\s+', sql_select, re.IGNORECASE)
        if is_sql:
            current_sql_selects.append(sql_select)
        else:
            is_save = re.match(r'^SAVE\s+RESULT\s+AS\s+(.*)$', sql_select, re.IGNORECASE | re.DOTALL)
            is_remove = re.match(r'^REMOVE\s+RESULT\s+(.*)$', sql_select, re.IGNORECASE | re.DOTALL)
            if is_save:
                result_name = is_save.group(1)
                result_map[result_name] = create_executor(current_sql_selects, result_map).execute()
                current_sql_selects = []
            elif is_remove:
                if current_sql_selects:
                    result_map['result'] = create_executor(current_sql_selects, result_map).execute()
                    current_sql_selects = []
                result_map.pop(is_remove.group(1))
            else:
                exec sql_select in {'result_map': result_map}, {}
    if current_sql_selects:
        result_map['result'] = create_executor(current_sql_selects, result_map).execute()
    return result_map


def create_executor(sql_selects, joinable_results=None):
    executor_map = {}
    if not isinstance(sql_selects, list):
        sql_selects = [sql_selects]
    root_executor = None
    level = 0
    for sql_select in sql_selects:
        level += 1
        executor_name = 'level%s' % level
        if not isinstance(sql_select, SqlSelect):
            sql_select = sql_select.strip()
            if not sql_select:
                continue
            match = re.match(r'^WITH\s+(.*)\s+AS\s+(.*)\s*$', sql_select, re.IGNORECASE | re.DOTALL)
            if match:
                sql_select = match.group(1)
                executor_name = match.group(2)
            sql_select = SqlSelect.parse(sql_select)
        if joinable_results:
            sql_select.joinable_results = joinable_results
        sql_select.joinable_queries = executor_map
        if not isinstance(sql_select.from_table, basestring):
            raise Exception('nested SELECT is not supported')
        if sql_select.from_table in executor_map:
            parent_executor = executor_map[sql_select.from_table]
            executor = SelectInsideBranchExecutor(sql_select, executor_name)
            parent_executor.add_child(executor)
        else:
            _search_es = search_es
            if sql_select.join_table in executor_map:
                _search_es = functools.partial(search_es, search_url='_coordinate_search')
            if sql_select.is_select_inside:
                executor = SelectInsideLeafExecutor(sql_select, _search_es)
            else:
                executor = SelectFromLeafExecutor(sql_select, _search_es)
            if root_executor:
                if executor.sql_select.join_table != root_executor[0]:
                    raise Exception('multiple root executor is not supported')
            root_executor = (executor_name, executor)
        executor_map[executor_name] = executor
    root_executor[1].build_request()
    return root_executor[1]


def search_es(index, request, search_url='_search'):
    if DEBUG:
        print('===== %s' % search_url)
        print(json.dumps(request, indent=2))
    url = ES_HOSTS + '/%s*/%s' % (index, search_url)
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
    result_map = execute_sql(sys.argv[1], sql)
    print('=====')
    for result_name, rows in result_map.iteritems():
        for row in rows:
            print json.dumps(row)
