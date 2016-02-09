#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Query elasticsearch using SQL
"""
import json
import sys
import urllib2

import sqlparse
from executors import SelectFromLeafExecutor
from executors import SelectInsideLeafExecutor
from executors import SelectFromInMemExecutor
from executors import SelectInsideBranchExecutor
from executors import SelectFromAllBucketsExecutor
from executors import translators, in_mem_computation
from sqlparse.sql_select import SqlSelect

DEBUG = False
ES_HOSTS = None


def execute_sql(es_hosts, sql):
    global ES_HOSTS

    ES_HOSTS = es_hosts
    statement = sqlparse.parse(sql.strip())[0]
    sql_select = SqlSelect(statement.tokens)
    rows = create_executor(sql_select).execute()
    for row in rows:
        row.pop('_bucket_', None)
    return rows


def create_executor(sql_select):
    if isinstance(sql_select.source, basestring):
        if sql_select.is_select_inside:
            return SelectInsideLeafExecutor(sql_select, search_es)
        else:
            return SelectFromLeafExecutor(sql_select, search_es)
    elif sql_select.is_select_inside:
        return SelectInsideBranchExecutor(sql_select, create_executor(sql_select.source))
    else:
        if in_mem_computation.is_in_mem_computation(sql_select):
            return SelectFromInMemExecutor(sql_select, create_executor(sql_select.source))
        elif SelectFromAllBucketsExecutor.is_select_from_all_buckets(sql_select):
            return SelectFromAllBucketsExecutor(sql_select, create_executor(sql_select.source))
        else:
            return SelectFromExecutor(sql_select)


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


class SelectInsideExecutor(object):
    def __init__(self, sql_select):
        self.sql_select = sql_select
        my_request, self.select_response = translators.translate_select_inside(sql_select)
        my_aggs = my_request['aggs']
        self.inner_executor = create_executor(sql_select.source)
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
        if isinstance(sql_select.source, basestring):
            return aggs
        bucket_keys = []
        for bucket_key in sql_select.source.get_bucket_keys():
            if '_global_' in aggs:
                aggs = aggs['_global_']['aggs']
            aggs = aggs[bucket_key]['aggs']
        return aggs

    def execute(self):
        response = self.inner_executor.execute()
        return self.select_response(response)


class SelectFromExecutor(object):
    def __init__(self, sql_select):
        self.sql_select = sql_select
        parent_pipeline_aggs, sibling_pipeline_aggs = translators.translate_select_from(sql_select)
        self.inner_executor = create_executor(sql_select.source)
        self.request = self.inner_executor.request
        self.add_sibling_pipeline_aggs(sibling_pipeline_aggs)
        self.add_parent_pipeline_aggs(parent_pipeline_aggs)

    def add_parent_pipeline_aggs(self, parent_pipeline_aggs):
        if not parent_pipeline_aggs:
            return
        aggs = self.request['aggs']
        for bucket_key in self.sql_select.source.group_by.keys():
            if '_global_' in aggs:
                aggs = aggs['_global_']['aggs']
            aggs = aggs[bucket_key]['aggs']
        aggs.update(parent_pipeline_aggs or {})

    def add_sibling_pipeline_aggs(self, sibling_pipeline_aggs):
        if not sibling_pipeline_aggs:
            return
        if '_global_' in self.request['aggs']:
            if self.request['aggs']['_global_']['filter']:
                raise Exception('sibling pipeline does not support filter aggregation yet')
            self.request['aggs'] = self.request['aggs']['_global_']['aggs']
        self.request['aggs'].update(sibling_pipeline_aggs)

    def execute(self):
        response = self.inner_executor.execute()
        return response


if __name__ == "__main__":
    DEBUG = True
    sql = sys.stdin.read()
    rows = execute_sql(sys.argv[1], sql)
    print('=====')
    for row in rows:
        print json.dumps(row)
    sys.exit(0 if rows else 1)
