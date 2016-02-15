#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Query elasticsearch using SQL
"""
import json
import sys
import urllib2

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
    rows = create_executor(sql).execute()
    for row in rows:
        row.pop('_bucket_', None)
    return rows


def create_executor(sql_select):
    if isinstance(sql_select, basestring):
        sql_select = SqlSelect.parse(sql_select.strip())
    if isinstance(sql_select.source, basestring):
        if sql_select.is_select_inside:
            return SelectInsideLeafExecutor(sql_select, search_es)
        else:
            return SelectFromLeafExecutor(sql_select, search_es)
    elif sql_select.is_select_inside:
        return SelectInsideBranchExecutor(sql_select, create_executor(sql_select.source))
    else:
        if SelectFromInMemExecutor.is_in_mem_computation(sql_select):
            return SelectFromInMemExecutor(sql_select, create_executor(sql_select.source))
        elif SelectFromAllBucketsExecutor.is_select_from_all_buckets(sql_select):
            return SelectFromAllBucketsExecutor(sql_select, create_executor(sql_select.source))
        else:
            return SelectFromPerBucketExecutor(sql_select, create_executor(sql_select.source))


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
