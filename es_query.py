#!/usr/bin/python

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
import pprint

ES_HOSTS = 'http://10.121.89.8/gsapi'
DEBUG = False


def execute_sql(sql):
    statement = sqlparse.parse(sql.strip())[0]
    translator = Translator()
    translator.on(statement)
    if DEBUG:
        print('=====')
        pprint.pprint(translator.request)
    url = ES_HOSTS + '/%s*/_search' % translator.index
    try:
        resp = urllib2.urlopen(url, json.dumps(translator.request)).read()
    except urllib2.HTTPError as e:
        sys.stderr.write(e.read())
        return
    except:
        import traceback

        sys.stderr.write(traceback.format_exc())
        return
    translator.response = json.loads(resp)
    if DEBUG:
        print('=====')
        pprint.pprint(translator.response)
    translator.on(statement)
    return translator.records


class Translator(object):
    def __init__(self):
        # output of request stage
        self.index = None
        self.request = {}
        # input of response stage
        self.response = None
        # output of response stage
        self.records = None
        # internal state
        self.projections = None
        self.group_by = None
        self.order_by = None

    def on(self, statement):
        getattr(self, 'on_%s' % statement.get_type())(statement)

    def on_SELECT(self, statement):
        idx = 1
        from_found = False
        while idx < len(statement.tokens):
            token = statement.tokens[idx]
            idx += 1
            if token.ttype in (ttypes.Whitespace, ttypes.Comment):
                continue
            if ttypes.Keyword == token.ttype:
                if 'FROM' == token.value.upper():
                    from_found = True
                    idx = self.on_FROM(statement, idx)
                    continue
                elif 'GROUP' == token.value.upper():
                    idx = self.on_GROUP(statement, idx)
                    continue
                elif 'ORDER' == token.value.upper():
                    idx = self.on_ORDER(statement, idx)
                    continue
                elif 'LIMIT' == token.value.upper():
                    idx = self.on_LIMIT(statement, idx)
                    continue
                else:
                    raise Exception('unexpected: %s' % repr(token))
            elif isinstance(token, stypes.Where):
                self.on_WHERE(token)
            elif not from_found:
                if isinstance(token, stypes.IdentifierList):
                    self.projections = list(token.get_identifiers())
                else:
                    self.projections = [token]
                continue
            else:
                raise Exception('unexpected: %s' % repr(token))
        if self.group_by or self.has_function_projection():
            self.request['size'] = 0
            self.analyze_projections_and_group_by()
        else:
            self.analyze_non_aggregation()

    def on_FROM(self, statement, idx):
        while idx < len(statement.tokens):
            token = statement.tokens[idx]
            idx += 1
            if token.ttype in (ttypes.Whitespace, ttypes.Comment):
                continue
            if isinstance(token, stypes.Identifier):
                self.index = token.get_name()
                break
            else:
                raise Exception('unexpected: %s' % repr(token))
        return idx

    def on_LIMIT(self, statement, idx):
        while idx < len(statement.tokens):
            token = statement.tokens[idx]
            idx += 1
            if token.ttype in (ttypes.Whitespace, ttypes.Comment):
                continue
            self.request['size'] = int(token.value)
            break
        return idx

    def on_GROUP(self, statement, idx):
        while idx < len(statement.tokens):
            token = statement.tokens[idx]
            idx += 1
            if token.ttype in (ttypes.Whitespace, ttypes.Comment):
                continue
            if ttypes.Keyword == token.ttype:
                if 'BY' == token.value.upper():
                    return self.on_GROUP_BY(statement, idx)
            else:
                raise Exception('unexpected: %s' % repr(token))

    def on_GROUP_BY(self, statement, idx):
        while idx < len(statement.tokens):
            token = statement.tokens[idx]
            idx += 1
            if token.ttype in (ttypes.Whitespace, ttypes.Comment):
                continue
            self.group_by = token
            return idx

    def on_ORDER(self, statement, idx):
        while idx < len(statement.tokens):
            token = statement.tokens[idx]
            idx += 1
            if token.ttype in (ttypes.Whitespace, ttypes.Comment):
                continue
            if ttypes.Keyword == token.ttype:
                if 'BY' == token.value.upper():
                    return self.on_ORDER_BY(statement, idx)
            else:
                raise Exception('unexpected: %s' % repr(token))

    def on_ORDER_BY(self, statement, idx):
        while idx < len(statement.tokens):
            token = statement.tokens[idx]
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
                else:
                    raise Exception('unexpected: %s' % repr(token))
            elif ttypes.Keyword == token.ttype:
                if 'OR' == token.value:
                    logic_op = 'OR'
                elif 'AND' == token.value:
                    logic_op = 'AND'
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
            return {'term': {token.left.get_name(): eval(token.right.value)}}
        else:
            raise Exception('unexpected: %s' % repr(token))

    def has_function_projection(self):
        for projection in self.projections:
            if isinstance(projection, stypes.Function):
                return True
        return False

    def analyze_projections_and_group_by(self):
        group_by_identifiers = {}
        if self.group_by:
            if isinstance(self.group_by, stypes.IdentifierList):
                for id in self.group_by.get_identifiers():
                    if ttypes.Keyword == id.ttype:
                        raise Exception('%s is keyword' % id.value)
                    elif isinstance(id, stypes.Identifier):
                        group_by_identifiers[id.get_name()] = id
                    else:
                        raise Exception('unexpected: %s' % repr(id))
            elif isinstance(self.group_by, stypes.Identifier):
                group_by_identifiers[self.group_by.get_name()] = self.group_by
            else:
                raise Exception('unexpected: %s' % repr(self.group_by))
        metrics = {}
        for projection in self.projections:
            if isinstance(projection, stypes.Identifier):
                if projection.tokens[0].ttype in (ttypes.Name, ttypes.String.Symbol):
                    if not group_by_identifiers.get(projection.get_name()):
                        raise Exception('unexpected: %s' % repr(projection))
                elif isinstance(projection.tokens[0], stypes.Function):
                    self.create_metric_aggregation(metrics, projection.tokens[0], projection.get_name())
                else:
                    raise Exception('unexpected: %s' % repr(projection))
            elif isinstance(projection, stypes.Function):
                self.create_metric_aggregation(metrics, projection, projection.get_name())
            else:
                raise Exception('unexpected: %s' % repr(projection))
        terms_bucket_fields = sorted(group_by_identifiers.keys())
        if self.response:
            self.records = []
            agg_response = dict(self.response.get('aggregations') or self.response)
            agg_response.update(self.response)
            self.collect_records(agg_response, list(reversed(terms_bucket_fields)), metrics, {})
        else:
            current_aggs = {}
            if metrics:
                current_aggs = {'aggs': metrics}
            for terms_bucket_field in terms_bucket_fields:
                group_by = group_by_identifiers.get(terms_bucket_field)
                if ttypes.Name == group_by.tokens[0].ttype:
                    current_aggs = {
                        'aggs': {terms_bucket_field: dict(current_aggs, **{
                            'terms': {'field': terms_bucket_field, 'size': 0}
                        })}
                    }
                else:
                    if isinstance(group_by.tokens[0], stypes.Parenthesis):
                        tokens = group_by.tokens[0].tokens[1:-1]
                        if len(tokens) ==1 and isinstance(tokens[0], stypes.Case):
                            case_when_translator = CaseWhenTranslator()
                            case_when_translator.on_CASE(tokens[0].tokens[1:])
                            current_aggs = {
                                'aggs': {terms_bucket_field: dict(current_aggs, **{
                                    'range': {
                                        'field': case_when_translator.field,
                                        'ranges': case_when_translator.ranges                                    }
                                })}
                            }
                        else:
                            raise Exception('unexpected: %s' % repr(tokens[0]))
                    elif isinstance(group_by.tokens[0], stypes.Function):
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
                                'aggs': {terms_bucket_field: dict(current_aggs, **{
                                    'date_histogram': {
                                        'field': field.get_name(),
                                        'time_zone': '+08:00',
                                        'interval': eval(interval.value)
                                    }
                                })}
                            }
                            if date_format:
                                current_aggs['aggs'][terms_bucket_field]['date_histogram']['format'] = date_format
                        else:
                            raise Exception('unexpected: %s' % repr(sql_function))
                    else:
                        raise Exception('unexpected: %s' % repr(group_by.tokens[0]))
            self.request.update(current_aggs)

    def collect_records(self, parent_bucket, terms_bucket_fields, metrics, props):
        if terms_bucket_fields:
            current_response = parent_bucket[terms_bucket_fields[0]]
            for child_bucket in current_response['buckets']:
                child_props = dict(props, **{terms_bucket_fields[0]: child_bucket['key_as_string']})
                self.collect_records(child_bucket, terms_bucket_fields[1:], metrics, child_props)
        else:
            record = props
            for metric_name, get_metric in metrics.iteritems():
                record[metric_name] = get_metric(parent_bucket)
            self.records.append(record)

    def create_metric_aggregation(self, metrics, sql_function, metric_name):
        if not isinstance(sql_function, stypes.Function):
            raise Exception('unexpected: %s' % repr(sql_function))
        sql_function_name = sql_function.tokens[0].get_name().upper()
        if 'COUNT' == sql_function_name:
            if sql_function.get_parameters():
                raise Exception('only COUNT(*) is supported')
            if self.response:
                metrics[metric_name] = lambda bucket: bucket['hits']['total'] if 'hits' in bucket else bucket['doc_count']
        elif sql_function_name in ('MAX', 'MIN', 'AVG'):
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
            self.records = []
            for hit in self.response['hits']['hits']:
                record = {}
                for projection in self.projections:
                    if projection.ttype == ttypes.Wildcard:
                        record = hit['_source']
                    else:
                        record[projection.get_name()] = self.get_object_member(
                            hit['_source'],
                            projection.get_real_name().split('.'))
                self.records.append(record)
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
            idx = skip_whitespace(tokens, idx+1)
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
    records = execute_sql(sql)
    print('=====')
    for record in records:
        print json.dumps(record)
    sys.exit(0 if records else 1)
