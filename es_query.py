#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Query elasticsearch using SQL
"""
import json
import sys
import urllib2
import sqlparse
import time
import json
from sqlparse import tokens as ttypes
from sqlparse import sql as stypes
from sqlparse.ordereddict import OrderedDict
import json
from sql_select import SqlSelect
import in_mem_computation
import filter_translator

DEBUG = False


def execute_sql(es_hosts, sql):
    global ES_HOSTS

    ES_HOSTS = es_hosts
    statement = sqlparse.parse(sql.strip())[0]
    return SqlExecutor().execute(statement)


class SqlExecutor(object):
    def __init__(self):
        # output of request stage
        self.request = {}
        # input of response stage
        self.response = None
        # output of response stage
        self.rows = None

        # internal state
        self.sql_select = None

    @property
    def projections(self):
        return self.sql_select.projections

    @property
    def group_by(self):
        return self.sql_select.group_by

    @property
    def order_by(self):
        return self.sql_select.order_by

    @property
    def limit(self):
        return self.sql_select.limit

    @property
    def having(self):
        return self.sql_select.having

    @property
    def where(self):
        return self.sql_select.where

    @property
    def select_from(self):
        return self.sql_select.select_from

    def execute(self, statement):
        self.on_SELECT(statement.tokens)
        if DEBUG:
            print('=====')
            print(json.dumps(self.request, indent=2))
        if isinstance(self.select_from, stypes.Statement):
            self.response = SqlExecutor().execute(self.select_from)
            self.on_SELECT(statement.tokens)
            return self.rows
        else:
            url = ES_HOSTS + '/%s*/_search' % self.select_from
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
            self.on_SELECT(statement.tokens)
            return self.rows

    def on_SELECT(self, tokens):
        if not self.sql_select:
            self.sql_select = SqlSelect()
            self.sql_select.on_SELECT(tokens)
        if self.where and not self.response:
            self.request['query'] = filter_translator.create_compound_filter(self.where.tokens[1:])
        if in_mem_computation.is_in_mem_computation(self.sql_select):
            if self.response:
                self.rows = in_mem_computation.do_in_mem_computation(self.sql_select, self.response)
        elif self.group_by or self.has_function_projection():
            self.request['size'] = 0
            self.analyze_projections_and_group_by()
        else:
            self.analyze_non_aggregation()

    def has_function_projection(self):
        for projection in self.projections.values():
            if isinstance(projection, stypes.Function):
                return True
        return False

    def analyze_projections_and_group_by(self):
        metrics = {}
        for projection_name, projection in self.projections.iteritems():
            if projection.ttype in (ttypes.Name, ttypes.String.Symbol):
                if not self.group_by.get(projection_name):
                    raise Exception('selected field not in group by: %s' % projection_name)
            elif isinstance(projection, stypes.Function):
                self.create_metric_aggregation(metrics, projection, projection_name)
            else:
                raise Exception('unexpected: %s' % repr(projection))
        group_by_names = list(reversed(self.group_by.keys())) if self.group_by else []
        if self.response:
            self.rows = []
            agg_response = self.response['aggregations']
            if not group_by_names:
                agg_response = agg_response['_global_']
            self.collect_records(agg_response, list(reversed(group_by_names)), metrics, {})
        else:
            self.add_aggs_to_request(group_by_names, metrics)
        if self.order_by or self.limit:
            if len(self.group_by) != 1:
                raise Exception('order by can only be applied on single group by')
            aggs = self.request['aggs'][group_by_names[0]]
            agg_names = set(aggs.keys()) - set(['aggs'])
            if len(agg_names) != 1:
                raise Exception('order by can only be applied on single group by')
            agg_type = list(agg_names)[0]
            agg = aggs[agg_type]
            if self.order_by:
                agg['order'] = self.create_sort((group_by_names[0], agg_type))
            if self.limit:
                agg['size'] = self.limit

    def add_aggs_to_request(self, group_by_names, metrics):
        current_aggs = {'aggs': {}}
        if metrics:
            current_aggs = {'aggs': metrics}
        if self.having:
            bucket_selector_agg = {'buckets_path': {}, 'script': {'lang': 'expression', 'inline': ''}}
            self.process_having_agg(bucket_selector_agg, self.having)
            current_aggs['aggs']['having'] = {'bucket_selector': bucket_selector_agg}
        if group_by_names:
            for group_by_name in group_by_names:
                group_by = self.group_by.get(group_by_name)
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
                    projection = self.projections.get(variable_name)
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
            case_when_translator = CaseWhenNumericRangeTranslator()
            try:
                case_when_aggs = case_when_translator.on_CASE(case_when.tokens[1:])
            except:
                if DEBUG:
                    print('not numeric: %s' % case_when)
                case_when_translator = CaseWhenFiltersTranslator()
                case_when_aggs = case_when_translator.on_CASE(case_when.tokens[1:])
            current_aggs = {
                'aggs': {group_by_name: dict(current_aggs, **case_when_aggs)}
            }
        else:
            raise Exception('unexpected: %s' % repr(tokens[0]))
        return current_aggs

    def collect_records(self, parent_bucket, terms_bucket_fields, metrics, props):
        if terms_bucket_fields:
            current_response = parent_bucket[terms_bucket_fields[0]]
            child_buckets = current_response['buckets']
            if isinstance(child_buckets, dict):
                for child_bucket_key, child_bucket in child_buckets.iteritems():
                    child_props = dict(props, **{terms_bucket_fields[0]: child_bucket_key})
                    self.collect_records(child_bucket, terms_bucket_fields[1:], metrics, child_props)
            else:
                for child_bucket in child_buckets:
                    child_bucket_key = child_bucket['key_as_string'] if 'key_as_string' in child_bucket else \
                        child_bucket['key']
                    child_props = dict(props, **{terms_bucket_fields[0]: child_bucket_key})
                    self.collect_records(child_bucket, terms_bucket_fields[1:], metrics, child_props)
        else:
            record = props
            for metric_name, get_metric in metrics.iteritems():
                record[metric_name] = get_metric(parent_bucket)
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
                for projection_name, projection in self.projections.iteritems():
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
            if self.order_by:
                self.request['sort'] = self.create_sort()
            if token.value:
                self.request['size'] = self.limit

    def create_sort(self, agg=None):
        sort = []
        for id in self.order_by or []:
            asc_or_desc = 'asc'
            if 'DESC' == id.tokens[-1].value.upper():
                asc_or_desc = 'desc'
            projection = self.projections.get(id.get_name())
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


class CaseWhenNumericRangeTranslator(object):
    def __init__(self):
        self.ranges = []
        self.field = None

    def on_CASE(self, tokens):
        idx = 0
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.ttype in (ttypes.Whitespace, ttypes.Comment):
                continue
            if 'WHEN' == token.value.upper():
                idx = self.on_WHEN(tokens, idx)
            elif 'ELSE' == token.value.upper():
                idx = self.on_ELSE(tokens, idx)
            elif 'END' == token.value.upper():
                break
            else:
                raise Exception('unexpected: %s' % repr(token))
        return self.build()

    def on_WHEN(self, tokens, idx):
        current_range = {}
        idx = skip_whitespace(tokens, idx)
        token = tokens[idx]
        self.parse_comparison(current_range, token)
        idx = skip_whitespace(tokens, idx + 1)
        token = tokens[idx]
        if 'AND' == token.value.upper():
            idx = skip_whitespace(tokens, idx + 1)
            token = tokens[idx]
            self.parse_comparison(current_range, token)
            idx = skip_whitespace(tokens, idx + 1)
            token = tokens[idx]
        if 'THEN' != token.value.upper():
            raise Exception('unexpected: %s' % repr(token))
        idx = skip_whitespace(tokens, idx + 1)
        token = tokens[idx]
        idx += 1
        current_range['key'] = eval(token.value)
        self.ranges.append(current_range)
        return idx

    def parse_comparison(self, current_range, token):
        if isinstance(token, stypes.Comparison):
            operator = str(token.token_next_by_type(0, ttypes.Comparison))
            if '>=' == operator:
                current_range['from'] = filter_translator.eval_numeric_value(str(token.right))
            elif '<' == operator:
                current_range['to'] = filter_translator.eval_numeric_value(str(token.right))
            else:
                raise Exception('unexpected: %s' % repr(token))
            self.set_field(token.left.get_name())
        else:
            raise Exception('unexpected: %s' % repr(token))

    def on_ELSE(self, tokens, idx):
        raise Exception('else is not supported')

    def set_field(self, field):
        if self.field is None:
            self.field = field
        elif self.field != field:
            raise Exception('can only case when on single field: %s %s' % (self.field, field))
        else:
            self.field = field

    def build(self):
        if not self.field or not self.ranges:
            raise Exception('internal error')
        return {
            'range': {
                'field': self.field,
                'ranges': self.ranges
            }
        }


class CaseWhenFiltersTranslator(object):
    def __init__(self):
        self.filters = {}
        self.other_bucket_key = None

    def on_CASE(self, tokens):
        idx = 0
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.ttype in (ttypes.Whitespace, ttypes.Comment):
                continue
            if 'WHEN' == token.value.upper():
                idx = self.on_WHEN(tokens, idx)
            elif 'ELSE' == token.value.upper():
                idx = self.on_ELSE(tokens, idx)
            elif 'END' == token.value.upper():
                break
            else:
                raise Exception('unexpected: %s' % repr(token))
        return self.build()

    def on_WHEN(self, tokens, idx):
        filter_tokens = []
        bucket_key = None
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.ttype in (ttypes.Whitespace, ttypes.Comment):
                continue
            if ttypes.Keyword == token.ttype and 'THEN' == token.value.upper():
                idx = skip_whitespace(tokens, idx + 1)
                bucket_key = eval(tokens[idx].value)
                idx += 1
                break
            filter_tokens.append(token)
        if not filter_tokens:
            raise Exception('case when can not have empty filter')
        self.filters[bucket_key] = filter_translator.create_compound_filter(filter_tokens)
        return idx

    def on_ELSE(self, tokens, idx):
        idx = skip_whitespace(tokens, idx + 1)
        self.other_bucket_key = eval(tokens[idx].value)
        idx += 1
        return idx

    def set_field(self, field):
        if self.field is None:
            self.field = field
        elif self.field != field:
            raise Exception('can only case when on single field: %s %s' % (self.field, field))
        else:
            self.field = field

    def build(self):
        if not self.filters:
            raise Exception('internal error')
        agg = {'filters': {'filters': self.filters}}
        if self.other_bucket_key:
            agg['filters']['other_bucket_key'] = self.other_bucket_key
        return agg





def is_count_star(projection):
    return isinstance(projection, stypes.Function) \
           and 'COUNT' == projection.tokens[0].get_name().upper() \
           and not projection.get_parameters()


def skip_whitespace(tokens, idx):
    while idx < len(tokens):
        token = tokens[idx]
        if token.ttype in (ttypes.Whitespace, ttypes.Comment):
            idx += 1
            continue
        else:
            break
    return idx

if __name__ == "__main__":
    DEBUG = True
    sql = sys.stdin.read()
    rows = execute_sql(sys.argv[1], sql)
    print('=====')
    for row in rows:
        print json.dumps(row)
    sys.exit(0 if rows else 1)
