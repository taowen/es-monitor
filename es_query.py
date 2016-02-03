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
import pprint

ES_HOSTS = 'http://10.121.89.8/gsapi'
DEBUG = False


def execute_sql(sql):
    statement = sqlparse.parse(sql.strip().replace('》', '>').replace('《', '<').replace('；', ';'))[0]
    return SqlExecutor().execute(statement)


class SqlExecutor(object):
    def __init__(self):
        # output of request stage
        self.select_from = None
        self.request = {}
        # input of response stage
        self.response = None
        # output of response stage
        self.rows = None

        # internal state
        self.projections = None
        self.group_by = None
        self.order_by = None
        self.having = None

    def execute(self, statement):
        self.on_SELECT(statement.tokens)
        if DEBUG:
            print('=====')
            pprint.pprint(self.request)
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
                pprint.pprint(self.response)
            self.on_SELECT(statement.tokens)
            return self.rows


    def on_SELECT(self, tokens):
        if not(ttypes.DML == tokens[0].ttype and 'SELECT' == tokens[0].value.upper()):
            raise Exception('it is not SELECT: %s' % tokens[0])
        idx = 1
        from_found = False
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.ttype in (ttypes.Whitespace, ttypes.Comment):
                continue
            if ttypes.Keyword == token.ttype:
                if 'FROM' == token.value.upper():
                    from_found = True
                    idx = self.on_FROM(tokens, idx)
                    continue
                elif 'GROUP' == token.value.upper():
                    idx = self.on_GROUP(tokens, idx)
                    continue
                elif 'ORDER' == token.value.upper():
                    idx = self.on_ORDER(tokens, idx)
                    continue
                elif 'LIMIT' == token.value.upper():
                    idx = self.on_LIMIT(tokens, idx)
                    continue
                elif 'HAVING' == token.value.upper():
                    idx = self.on_HAVING(tokens, idx)
                    continue
                else:
                    raise Exception('unexpected: %s' % repr(token))
            elif isinstance(token, stypes.Where):
                self.on_WHERE(token)
            elif not from_found:
                self.set_projections(token)
                continue
            else:
                raise Exception('unexpected: %s' % repr(token))
        if self.is_eval():
            if self.response:
                self.execute_eval()
        elif self.group_by or self.has_function_projection():
            self.request['size'] = 0
            self.analyze_projections_and_group_by()
        else:
            self.analyze_non_aggregation()

    def set_projections(self, token):
        if isinstance(token, stypes.IdentifierList):
            ids = list(token.get_identifiers())
        else:
            ids = [token]
        self.projections = {}
        for id in ids:
            if isinstance(id, stypes.TokenList):
                if isinstance(id, stypes.Identifier):
                    self.projections[id.get_name()] = id.tokens[0]
                else:
                    self.projections[id.get_name()] = id
            else:
                self.projections[id.value] = id

    def on_FROM(self, tokens, idx):
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.ttype in (ttypes.Whitespace, ttypes.Comment):
                continue
            if isinstance(token, stypes.Identifier):
                self.select_from = token.get_name()
                break
            elif isinstance(token, stypes.Parenthesis):
                self.select_from = sqlparse.parse(token.value[1:-1].strip())[0]
            else:
                raise Exception('unexpected: %s' % repr(token))
        return idx

    def on_LIMIT(self, tokens, idx):
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.ttype in (ttypes.Whitespace, ttypes.Comment):
                continue
            self.request['size'] = int(token.value)
            break
        return idx

    def on_HAVING(self, tokens, idx):
        self.having = []
        while idx < len(tokens):
            token = tokens[idx]
            if ttypes.Keyword == token.ttype and token.value.upper() in ('ORDER', 'LIMIT'):
                break
            else:
                idx += 1
                self.having.append(token)
        return idx

    def on_GROUP(self, tokens, idx):
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.ttype in (ttypes.Whitespace, ttypes.Comment):
                continue
            if ttypes.Keyword == token.ttype:
                if 'BY' == token.value.upper():
                    return self.on_GROUP_BY(tokens, idx)
            else:
                raise Exception('unexpected: %s' % repr(token))

    def on_GROUP_BY(self, tokens, idx):
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.ttype in (ttypes.Whitespace, ttypes.Comment):
                continue
            self.group_by = OrderedDict()
            if isinstance(token, stypes.IdentifierList):
                for id in token.get_identifiers():
                    if ttypes.Keyword == id.ttype:
                        raise Exception('%s is keyword' % id.value)
                    elif isinstance(id, stypes.Identifier):
                        self.group_by[id.get_name()] = id
                    else:
                        raise Exception('unexpected: %s' % repr(id))
            elif isinstance(token, stypes.Identifier):
                self.group_by[token.get_name()] = token
            else:
                raise Exception('unexpected: %s' % repr(token))
            return idx

    def on_ORDER(self, tokens, idx):
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.ttype in (ttypes.Whitespace, ttypes.Comment):
                continue
            if ttypes.Keyword == token.ttype:
                if 'BY' == token.value.upper():
                    return self.on_ORDER_BY(tokens, idx)
            else:
                raise Exception('unexpected: %s' % repr(token))

    def on_ORDER_BY(self, tokens, idx):
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.ttype in (ttypes.Whitespace, ttypes.Comment):
                continue
            if isinstance(token, stypes.IdentifierList):
                self.order_by = token.get_identifiers()
            else:
                self.order_by = [token]
            return idx

    def on_WHERE(self, where):
        if not self.response:
            self.request['query'] = self.create_compound_filter(where.tokens[1:])

    def create_compound_filter(self, tokens):
        idx = 0
        current_filter = None
        logic_op = None
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if token.ttype in (ttypes.Whitespace, ttypes.Comment):
                continue
            if isinstance(token, stypes.Comparison) or isinstance(token, stypes.Parenthesis):
                if isinstance(token, stypes.Comparison):
                    new_filter = self.create_comparision_filter(token)
                elif isinstance(token, stypes.Parenthesis):
                    new_filter = self.create_compound_filter(token.tokens[1:-1])
                else:
                    raise Exception('unexpected: %s' % repr(token))
                if not logic_op and not current_filter:
                    current_filter = new_filter
                elif 'OR' == logic_op:
                    current_filter = {'bool': {'should': [current_filter, new_filter]}}
                elif 'AND' == logic_op:
                    current_filter = {'bool': {'filter': [current_filter, new_filter]}}
                elif 'NOT' == logic_op:
                    current_filter = {'bool': {'must_not': new_filter}}
                else:
                    raise Exception('unexpected: %s' % repr(token))
            elif ttypes.Keyword == token.ttype:
                if 'OR' == token.value.upper():
                    logic_op = 'OR'
                elif 'AND' == token.value.upper():
                    logic_op = 'AND'
                elif 'NOT' == token.value.upper():
                    logic_op = 'NOT'
                else:
                    raise Exception('unexpected: %s' % repr(token))
            else:
                raise Exception('unexpected: %s' % repr(token))
        return current_filter

    def create_comparision_filter(self, token):
        if not isinstance(token, stypes.Comparison):
            raise Exception('unexpected: %s' % repr(token))
        operator = token.token_next_by_type(0, ttypes.Comparison)
        if '>' == operator.value:
            return {'range': {token.left.get_name(): {'from': eval_numeric_value(str(token.right))}}}
        elif '<' == operator.value:
            return {'range': {token.left.get_name(): {'to': eval_numeric_value(str(token.right))}}}
        elif '=' == operator.value:
            right_operand = eval(token.right.value)
            return {'term': {token.left.get_name(): right_operand}}
        elif operator.value.upper() in ('LIKE', 'ILIKE'):
            right_operand = eval(token.right.value)
            return {'wildcard': {token.left.get_name(): right_operand.replace('%', '*').replace('_', '?')}}
        elif operator.value in ('!=', '<>'):
            right_operand = eval(token.right.value)
            return {'not': {'term': {token.left.get_name(): right_operand}}}
        elif 'IN' == operator.value.upper():
            values = eval(token.right.value)
            if not isinstance(values, tuple):
                values = (values,)
            return {'terms': {token.left.get_name(): values}}
        else:
            raise Exception('unexpected operator: %s' % operator.value)

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
            agg_response = dict(self.response.get('aggregations') or self.response)
            agg_response.update(self.response)
            self.collect_records(agg_response, list(reversed(group_by_names)), metrics, {})
        else:
            self.add_aggs_to_request(group_by_names, metrics)

    def add_aggs_to_request(self, group_by_names, metrics):
        current_aggs = {'aggs': {}}
        if metrics:
            current_aggs = {'aggs': metrics}
        if self.having:
            bucket_selector_agg = {'buckets_path': {}, 'script': {'lang': 'expression', 'inline': ''}}
            self.process_having_agg(bucket_selector_agg, self.having)
            current_aggs['aggs']['having'] = {'bucket_selector': bucket_selector_agg}
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
        self.request.update(current_aggs)

    def process_having_agg(self, bucket_selector_agg, tokens):
        for token in tokens:
            if '@now' in token.value:
                bucket_selector_agg['script']['inline'] = '%s%s' % (
                    bucket_selector_agg['script']['inline'], eval_numeric_value(token.value))
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
                        raise Exception('having clause referenced variable must exist in select clause: %s' % variable_name)
                    if isinstance(projection, stypes.Function) \
                            and 'COUNT' == projection.tokens[0].get_name().upper() \
                            and not projection.get_parameters():
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

    def append_range_agg(self, current_aggs, group_by, group_by_name):
        tokens = group_by.tokens[0].tokens[1:-1]
        if len(tokens) == 1 and isinstance(tokens[0], stypes.Case):
            case_when_translator = CaseWhenTranslator()
            case_when_translator.on_CASE(tokens[0].tokens[1:])
            current_aggs = {
                'aggs': {group_by_name: dict(current_aggs, **{
                    'range': {
                        'field': case_when_translator.field,
                        'ranges': case_when_translator.ranges}
                })}
            }
        else:
            raise Exception('unexpected: %s' % repr(tokens[0]))
        return current_aggs

    def collect_records(self, parent_bucket, terms_bucket_fields, metrics, props):
        if terms_bucket_fields:
            current_response = parent_bucket[terms_bucket_fields[0]]
            for child_bucket in current_response['buckets']:
                child_bucket_key = child_bucket['key_as_string'] if 'key_as_string' in child_bucket else child_bucket[
                    'key']
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
                    metrics[metric_name] = lambda bucket: bucket['hits']['total'] if 'hits' in bucket else bucket[
                        'doc_count']
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
            self.request['sort'] = []
            for id in self.order_by or []:
                asc_or_desc = 'asc'
                if 'DESC' == id.tokens[-1].value.upper():
                    asc_or_desc = 'desc'
                self.request['sort'].append({id.get_name(): asc_or_desc})

    def get_object_member(self, obj, paths):
        if obj is None:
            return None
        if len(paths) == 1:
            return obj.get(paths[0])
        else:
            return self.get_object_member(obj.get(paths[0]), paths[1:])

    def is_eval(self):
        return len(self.projections) == 1 \
                and isinstance(self.projections.values()[0], stypes.Function) \
                and 'EVAL' == self.projections.values()[0].tokens[0].value.upper()

    def execute_eval(self):
        eval_func = self.projections.values()[0]
        source = eval(eval_func.get_parameters()[0].value)
        compiled_source = compile(source, '', 'exec')
        self.rows = []
        for row in self.response:
            context = {'input': row, 'output': {}}
            exec(compiled_source, {}, context)
            self.rows.append(context['output'])


class CaseWhenTranslator(object):
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
                current_range['from'] = eval_numeric_value(str(token.right))
            elif '<' == operator:
                current_range['to'] = eval_numeric_value(str(token.right))
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


def skip_whitespace(tokens, idx):
    while idx < len(tokens):
        token = tokens[idx]
        if token.ttype in (ttypes.Whitespace, ttypes.Comment):
            idx += 1
            continue
        else:
            break
    return idx


def eval_numeric_value(token):
    token_str = str(token).strip()
    if token_str.startswith('('):
        token_str = token_str[1:-1]
    if token_str.startswith('@now'):
        token_str = token_str[4:].strip()
        if not token_str:
            return long(time.time() * long(1000))
        if '+' == token_str[0]:
            return long(time.time() * long(1000)) + eval_timedelta(token_str[1:])
        elif '-' == token_str[0]:
            return long(time.time() * long(1000)) - eval_timedelta(token_str[1:])
        else:
            raise Exception('unexpected: %s' % repr(token))
    else:
        return float(token)


def eval_timedelta(str):
    if str.endswith('m'):
        return long(str[:-1]) * long(60 * 1000)
    elif str.endswith('s'):
        return long(str[:-1]) * long(1000)
    elif str.endswith('h'):
        return long(str[:-1]) * long(60 * 60 * 1000)
    elif str.endswith('d'):
        return long(str[:-1]) * long(24 * 60 * 60 * 1000)
    else:
        return long(str)


if __name__ == "__main__":
    DEBUG = True
    sql = sys.stdin.read()
    rows = execute_sql(sql)
    print('=====')
    for row in rows:
        print json.dumps(row)
    sys.exit(0 if rows else 1)
