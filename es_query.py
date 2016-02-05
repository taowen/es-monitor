#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Query elasticsearch using SQL
"""
import sys
import urllib2
import json

import sqlparse
from sqlparse import tokens as ttypes
from sqlparse import sql as stypes
from sql_select import SqlSelect
import in_mem_computation
import filter_translator
import case_when_translator

DEBUG = False


def execute_sql(es_hosts, sql):
    global ES_HOSTS

    ES_HOSTS = es_hosts
    statement = sqlparse.parse(sql.strip())[0]
    return SqlExecutor.create(statement.tokens).execute()


class SqlExecutor(object):
    def __init__(self, sql_select):
        # output of request stage
        self.request = {}
        # input of response stage
        self.response = None
        # output of response stage
        self.rows = None

        # internal state
        self.sql_select = sql_select
        self.include_bucket_in_row = False

    @classmethod
    def create(cls, tokens):
        sql_select = SqlSelect()
        sql_select.on_SELECT(tokens)
        return SqlExecutor(sql_select)

    def execute(self, inner_aggs=None):
        inner_aggs = inner_aggs or {}
        self.on_SELECT()
        if inner_aggs:
            outter_aggs = self.request['aggs']
            if self.sql_select.group_by:
                for group_by_name in self.sql_select.group_by.keys():
                    outter_aggs = outter_aggs[group_by_name]['aggs']
            else:
                if '_global_' in outter_aggs:
                    outter_aggs = outter_aggs['_global_']['aggs']
            outter_aggs.update(inner_aggs)
        if DEBUG:
            print('=====')
            print(json.dumps(self.request, indent=2))
        if isinstance(self.sql_select.select_from, basestring):
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
            self.response = json.loads(resp)
            if DEBUG:
                print('=====')
                print(json.dumps(self.response, indent=2))
            self.on_SELECT()
            return self.rows
        else:
            if in_mem_computation.is_in_mem_computation(self.sql_select):
                self.response = SqlExecutor.create(self.sql_select.select_from.tokens).execute()
                self.on_SELECT()
                return self.rows
            else:
                if self.sql_select.is_inside_query:
                    inner_executor = SqlExecutor.create(self.sql_select.select_from.tokens)
                    inner_executor.include_bucket_in_row = True
                    if 'aggs' not in self.request:
                        raise Exception('SELECT ... INSIDE ... can only nest aggregation query')
                    inner_rows = inner_executor.execute(self.request['aggs'])
                    self.response = inner_rows
                    self.on_SELECT()
                    return self.rows
                else:
                    print(self.request)
                    raise Exception('not implemented')


    def on_SELECT(self):
        if self.sql_select.where and not self.response:
            self.request['query'] = filter_translator.create_compound_filter(self.sql_select.where.tokens[1:])
        if in_mem_computation.is_in_mem_computation(self.sql_select):
            if self.response:
                self.rows = in_mem_computation.do_in_mem_computation(self.sql_select, self.response)
        elif self.sql_select.group_by or self.has_function_projection():
            self.request['size'] = 0
            self.analyze_projections_and_group_by()
        else:
            self.analyze_non_aggregation()

    def has_function_projection(self):
        for projection in self.sql_select.projections.values():
            if isinstance(projection, stypes.Function):
                return True
        return False

    def analyze_projections_and_group_by(self):
        metrics = {}
        for projection_name, projection in self.sql_select.projections.iteritems():
            if projection.ttype in (ttypes.Name, ttypes.String.Symbol):
                if not self.sql_select.group_by.get(projection_name):
                    raise Exception('selected field not in group by: %s' % projection_name)
            elif isinstance(projection, stypes.Function):
                self.create_metric_aggregation(metrics, projection, projection_name)
            else:
                raise Exception('unexpected: %s' % repr(projection))
        reversed_group_by_names = list(reversed(self.sql_select.group_by.keys())) if self.sql_select.group_by else []
        group_by_names = self.sql_select.group_by.keys() if self.sql_select.group_by else []
        if self.response:
            self.rows = []
            if isinstance(self.response, list):
                for inner_row in self.response:
                    bucket = inner_row.pop('_bucket_')
                    if not group_by_names:
                        bucket = bucket['_global_']
                    self.collect_records(bucket, group_by_names, metrics, inner_row)
            else:
                agg_response = self.response['aggregations']
                if not group_by_names:
                    agg_response = agg_response['_global_']
                self.collect_records(agg_response, group_by_names, metrics, {})
        else:
            self.add_aggs_to_request(reversed_group_by_names, metrics)
        if self.sql_select.order_by or self.sql_select.limit:
            if len(self.sql_select.group_by or {}) != 1:
                raise Exception('order by can only be applied on single group by')
            aggs = self.request['aggs'][reversed_group_by_names[0]]
            agg_names = set(aggs.keys()) - set(['aggs'])
            if len(agg_names) != 1:
                raise Exception('order by can only be applied on single group by')
            agg_type = list(agg_names)[0]
            agg = aggs[agg_type]
            if self.sql_select.order_by:
                agg['order'] = self.create_sort((reversed_group_by_names[0], agg_type))
            if self.sql_select.limit:
                agg['size'] = self.sql_select.limit

    def add_aggs_to_request(self, group_by_names, metrics):
        current_aggs = {'aggs': {}}
        if metrics:
            current_aggs = {'aggs': metrics}
        if self.sql_select.having:
            bucket_selector_agg = {'buckets_path': {}, 'script': {'lang': 'expression', 'inline': ''}}
            self.process_having_agg(bucket_selector_agg, self.sql_select.having)
            current_aggs['aggs']['having'] = {'bucket_selector': bucket_selector_agg}
        if group_by_names:
            for group_by_name in group_by_names:
                group_by = self.sql_select.group_by.get(group_by_name)
                if group_by.tokens[0].ttype in (ttypes.Name, ttypes.String.Symbol):
                    current_aggs = self.append_terms_agg(current_aggs, group_by_name)
                else:
                    if isinstance(group_by.tokens[0], stypes.Parenthesis):
                        current_aggs = self.append_range_agg(current_aggs, group_by, group_by_name)
                    elif isinstance(group_by.tokens[0], stypes.Function):
                        current_aggs = self.append_date_histogram_agg(current_aggs, group_by, group_by_name)
                    else:
                        raise Exception('unexpected: %s' % repr(group_by.tokens[0]))
        else:
            current_aggs = self.append_global_agg(current_aggs)
        self.request.update(current_aggs)

    def process_having_agg(self, bucket_selector_agg, tokens):
        for token in tokens:
            if '@now' in token.value:
                bucket_selector_agg['script']['inline'] = '%s%s' % (
                    bucket_selector_agg['script']['inline'], filter_translator.eval_numeric_value(token.value))
            elif token.ttype == ttypes.Keyword and 'AND' == token.value.upper():
                bucket_selector_agg['script']['inline'] = '%s%s' % (
                    bucket_selector_agg['script']['inline'], '&&')
            elif token.ttype == ttypes.Keyword and 'OR' == token.value.upper():
                bucket_selector_agg['script']['inline'] = '%s%s' % (
                    bucket_selector_agg['script']['inline'], '||')
            elif token.is_group():
                self.process_having_agg(bucket_selector_agg, token.tokens)
            else:
                if ttypes.Name == token.ttype:
                    variable_name = token.value
                    projection = self.sql_select.projections.get(variable_name)
                    if not projection:
                        raise Exception(
                            'having clause referenced variable must exist in select clause: %s' % variable_name)
                    if is_count_star(projection):
                        bucket_selector_agg['buckets_path'][variable_name] = '_count'
                    else:
                        bucket_selector_agg['buckets_path'][variable_name] = variable_name
                bucket_selector_agg['script']['inline'] = '%s%s' % (
                    bucket_selector_agg['script']['inline'], token.value)

    def append_terms_agg(self, current_aggs, group_by_name):
        current_aggs = {
            'aggs': {group_by_name: dict(current_aggs, **{
                'terms': {'field': group_by_name, 'size': 0}
            })}
        }
        return current_aggs

    def append_date_histogram_agg(self, current_aggs, group_by, group_by_name):
        date_format = None
        if 'to_char' == group_by.tokens[0].get_name():
            to_char_params = list(group_by.tokens[0].get_parameters())
            sql_function = to_char_params[0]
            date_format = eval(to_char_params[1].value)
        else:
            sql_function = group_by.tokens[0]
        if 'date_trunc' == sql_function.get_name():
            parameters = tuple(sql_function.get_parameters())
            interval, field = parameters
            current_aggs = {
                'aggs': {group_by_name: dict(current_aggs, **{
                    'date_histogram': {
                        'field': field.get_name(),
                        'time_zone': '+08:00',
                        'interval': eval(interval.value)
                    }
                })}
            }
            if date_format:
                current_aggs['aggs'][group_by_name]['date_histogram']['format'] = date_format
        else:
            raise Exception('unexpected: %s' % repr(sql_function))
        return current_aggs

    def append_global_agg(self, current_aggs):
        current_aggs = {
            'aggs': {'_global_': dict(current_aggs, **{
                'filter': self.request.get('query') or {}
            })}
        }
        return current_aggs

    def append_range_agg(self, current_aggs, group_by, group_by_name):
        tokens = group_by.tokens[0].tokens[1:-1]
        if len(tokens) == 1 and isinstance(tokens[0], stypes.Case):
            case_when = tokens[0]
            case_when_aggs = case_when_translator.translate_case_when(case_when)
            current_aggs = {
                'aggs': {group_by_name: dict(current_aggs, **case_when_aggs)}
            }
        else:
            raise Exception('unexpected: %s' % repr(tokens[0]))
        return current_aggs

    def collect_records(self, parent_bucket, group_by_names, metrics, props):
        if group_by_names:
            current_response = parent_bucket[group_by_names[0]]
            child_buckets = current_response['buckets']
            if isinstance(child_buckets, dict):
                for child_bucket_key, child_bucket in child_buckets.iteritems():
                    child_props = dict(props, **{group_by_names[0]: child_bucket_key})
                    self.collect_records(child_bucket, group_by_names[1:], metrics, child_props)
            else:
                for child_bucket in child_buckets:
                    child_bucket_key = child_bucket['key_as_string'] if 'key_as_string' in child_bucket else \
                        child_bucket['key']
                    child_props = dict(props, **{group_by_names[0]: child_bucket_key})
                    self.collect_records(child_bucket, group_by_names[1:], metrics, child_props)
        else:
            record = props
            for metric_name, get_metric in metrics.iteritems():
                record[metric_name] = get_metric(parent_bucket)
            if self.include_bucket_in_row:
                record['_bucket_'] = parent_bucket
            self.rows.append(record)

    def create_metric_aggregation(self, metrics, sql_function, metric_name):
        if not isinstance(sql_function, stypes.Function):
            raise Exception('unexpected: %s' % repr(sql_function))
        sql_function_name = sql_function.tokens[0].get_name().upper()
        if 'COUNT' == sql_function_name:
            params = list(sql_function.get_parameters())
            if params:
                count_keyword = sql_function.tokens[1].token_next_by_type(0, ttypes.Keyword)
                if self.response:
                    metrics[metric_name] = lambda bucket: bucket[metric_name]['value']
                else:
                    if count_keyword:
                        if 'DISTINCT' == count_keyword.value.upper():
                            metrics.update({metric_name: {
                                'cardinality': {
                                    'field': params[0].get_name()
                                }}})
                        else:
                            raise Exception('unexpected: %s' % repr(count_keyword))
                    else:
                        metrics.update({metric_name: {
                            'value_count': {'field': params[0].get_name()}}})
            else:
                if self.response:
                    metrics[metric_name] = lambda bucket: bucket['doc_count']
        elif sql_function_name in ('MAX', 'MIN', 'AVG', 'SUM'):
            if len(sql_function.get_parameters()) != 1:
                raise Exception('unexpected: %s' % repr(sql_function))
            if self.response:
                metrics[metric_name] = lambda bucket: bucket[metric_name]['value']
            else:
                metrics.update({metric_name: {
                    sql_function_name.lower(): {'field': sql_function.get_parameters()[0].get_name()}}})
        else:
            raise Exception('unsupported function: %s' % repr(sql_function))

    def analyze_non_aggregation(self):
        if self.response:
            self.rows = []
            for hit in self.response['hits']['hits']:
                record = {}
                for projection_name, projection in self.sql_select.projections.iteritems():
                    if projection.ttype == ttypes.Wildcard:
                        record = hit['_source']
                    elif projection.ttype in (ttypes.String.Symbol, ttypes.Name):
                        path = eval(projection.value) if projection.value.startswith('"') else projection.value
                        if path in hit.keys():
                            record[projection_name] = hit[path]
                        else:
                            record[projection_name] = self.get_object_member(
                                hit['_source'], path.split('.'))
                    else:
                        raise Exception('unexpected: %s' % repr(projection))
                self.rows.append(record)
        else:
            if self.sql_select.order_by:
                self.request['sort'] = self.create_sort()
            if self.sql_select.limit:
                self.request['size'] = self.sql_select.limit

    def create_sort(self, agg=None):
        sort = []
        for id in self.sql_select.order_by or []:
            asc_or_desc = 'asc'
            if 'DESC' == id.tokens[-1].value.upper():
                asc_or_desc = 'desc'
            projection = self.sql_select.projections.get(id.get_name())
            if not projection:
                raise Exception('can only sort on selected field: %s' % id.get_name())
            if is_count_star(projection):
                sort.append({'_count': asc_or_desc})
            elif agg and id.get_name() == agg[0]:
                if 'terms' == agg[1]:
                    sort.append({'_term': asc_or_desc})
                else:
                    sort.append({'_key': asc_or_desc})
            else:
                sort.append({id.get_name(): asc_or_desc})
        return sort

    def get_object_member(self, obj, paths):
        if obj is None:
            return None
        if len(paths) == 1:
            return obj.get(paths[0])
        else:
            return self.get_object_member(obj.get(paths[0]), paths[1:])


def is_count_star(projection):
    return isinstance(projection, stypes.Function) \
           and 'COUNT' == projection.tokens[0].get_name().upper() \
           and not projection.get_parameters()


if __name__ == "__main__":
    DEBUG = True
    sql = sys.stdin.read()
    rows = execute_sql(sys.argv[1], sql)
    print('=====')
    for row in rows:
        print json.dumps(row)
    sys.exit(0 if rows else 1)
