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
import itertools

DEBUG = False
ES_HOSTS = None


def execute_sql(es_hosts, sql):
    global ES_HOSTS

    ES_HOSTS = es_hosts
    statement = sqlparse.parse(sql.strip())[0]
    sql_select = SqlSelect()
    sql_select.on_SELECT(statement.tokens)
    rows = create_executor(sql_select).execute()
    for row in rows:
        row.pop('_bucket_')
    return rows


def create_executor(sql_select):
    if isinstance(sql_select.select_from, basestring):
        return LeafExecutor(sql_select)
    elif in_mem_computation.is_in_mem_computation(sql_select):
        return InMemExecutor(sql_select)
    else:
        if sql_select.is_select_inside:
            return SelectInsideExecutor(sql_select)
        else:
            return SelectFromExecutor(sql_select)


class InMemExecutor(object):
    def __init__(self, sql_select):
        self.sql_select = sql_select
        self.inner_executor = create_executor(sql_select.select_from)
        self.request = self.inner_executor.request

    def execute(self):
        response = self.inner_executor.execute(self.request)
        return in_mem_computation.do_in_mem_computation(self.sql_select, response)


class LeafExecutor(object):
    def __init__(self, sql_select):
        self.sql_select = sql_select
        self.request, self.select_response = translators.translate_select_inside(sql_select)

    def execute(self):
        if DEBUG:
            print('=====')
            print(json.dumps(self.request, indent=2))
        url = ES_HOSTS + '/%s*/_search' % self.sql_select.select_from
        try:
            resp = urllib2.urlopen(url, json.dumps(self.request)).read()
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
        return self.select_response(response)


class SelectInsideExecutor(object):
    def __init__(self, sql_select):
        self.sql_select = sql_select
        my_request, self.select_response = translators.translate_select_inside(sql_select)
        my_aggs = my_request['aggs']
        self.inner_executor = create_executor(sql_select.select_from)
        self.request = self.inner_executor.request
        inner_aggs = self.get_inner_aggs(self.request['aggs'], sql_select)
        for k in my_aggs.keys():
            if k in inner_aggs:
                if 'having' == k:
                    raise Exception('having and nested can only have one')
                else:
                    raise Exception('aggregation %s conflicted' % k)
        inner_aggs.update(my_aggs)

    def get_inner_aggs(self, aggs, sql_select):
        if isinstance(sql_select.select_from, basestring):
            return aggs
        aggs = aggs['_global_']['aggs']
        bucket_keys = list(itertools.chain(*sql_select.select_from.get_bucket_keys()))
        for bucket_key in bucket_keys:
            aggs = aggs[bucket_key]['aggs']
        return aggs

    def execute(self):
        response = self.inner_executor.execute()
        return self.select_response(response)


class SelectFromExecutor(object):
    def __init__(self, sql_select):
        self.sql_select = sql_select
        bucket_selector_agg, self.select_response = translators.translate_select_from(sql_select)
        self.inner_executor = create_executor(sql_select.select_from)
        self.request = self.inner_executor.request
        inner_aggs = self.get_inner_aggs(self.request['aggs'], sql_select)
        inner_aggs.update(bucket_selector_agg)

    def get_inner_aggs(self, aggs, sql_select):
        aggs = aggs['_global_']['aggs']
        top_most_bucket_key = sql_select.get_bucket_keys()[-1][-1]
        aggs = aggs[top_most_bucket_key]['aggs']
        return aggs

    def execute(self):
        response = self.inner_executor.execute()
        return self.select_response(response)


if __name__ == "__main__":
    DEBUG = True
    sql = sys.stdin.read()
    rows = execute_sql(sys.argv[1], sql)
    print('=====')
    for row in rows:
        print json.dumps(row)
    sys.exit(0 if rows else 1)
