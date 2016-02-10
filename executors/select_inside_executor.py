from sqlparse import sql as stypes
from sqlparse import tokens as ttypes
from translators import case_when_translator
from translators import filter_translator
from translators import script_translator
from translators import sort_translator
from translators import metric_translator
from merge_aggs import merge_aggs


class SelectInsideExecutor(object):
    def __init__(self, sql_select):
        self.sql_select = sql_select
        self.request = {}
        self.metric_request, self.metric_selector = metric_translator.translate_metrics(sql_select)
        self.build_request()

    def build_request(self):
        self.request['size'] = 0  # do not need hits in response
        reversed_group_by_names = list(reversed(self.sql_select.group_by.keys())) if self.sql_select.group_by else []
        self.add_aggs_to_request(reversed_group_by_names)
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
                agg['order'] = sort_translator.translate_sort(reversed_group_by_names[0], agg_type)
            if self.sql_select.limit:
                agg['size'] = self.sql_select.limit

    def select_response(self, response):
        group_by_names = self.sql_select.group_by.keys() if self.sql_select.group_by else []
        buckets = self.select_buckets(response)
        all_rows = []
        for bucket, inner_row in buckets:
            rows = []
            sibling = {}
            sibling_keys = set(bucket.keys()) - set(group_by_names)
            for sibling_key in sibling_keys:
                if isinstance(bucket[sibling_key], dict) and 'value' in bucket[sibling_key]:
                    sibling[sibling_key] = bucket[sibling_key]['value']
            self.collect_records(rows, bucket, group_by_names, {})
            for row in rows:
                row.update(sibling)
                row.update(inner_row)
            all_rows.extend(rows)
        return all_rows

    def select_buckets(self, response):
        raise Exception('base class')

    def add_aggs_to_request(self, group_by_names):
        current_aggs = {'aggs': {}}
        if self.metric_request:
            current_aggs = {'aggs': self.metric_request}
        if self.sql_select.having:
            current_aggs['aggs']['having'] = {
                'bucket_selector': script_translator.translate_script(self.sql_select, self.sql_select.having)
            }
        if group_by_names:
            for group_by_name in group_by_names:
                group_by = self.sql_select.group_by.get(group_by_name)
                if group_by.ttype in (ttypes.Name, ttypes.String.Symbol):
                    current_aggs = self.append_terms_aggs(current_aggs, group_by_name)
                elif isinstance(group_by, stypes.Parenthesis):
                    current_aggs = self.append_range_aggs(current_aggs, group_by, group_by_name)
                elif isinstance(group_by, stypes.Function):
                    sql_function_name = group_by.tokens[0].get_name().upper()
                    if sql_function_name in ('DATE_TRUNC', 'TO_CHAR'):
                        current_aggs = self.append_date_histogram_aggs(current_aggs, group_by, group_by_name)
                    elif 'HISTOGRAM' == sql_function_name:
                        current_aggs = self.append_histogram_aggs(current_aggs, group_by, group_by_name)
                    else:
                        raise Exception('unsupported group by on %s' % sql_function_name)
                elif isinstance(group_by, stypes.Where):
                    current_aggs = self.append_filter_aggs(current_aggs, group_by)
                else:
                    raise Exception('unexpected: %s' % repr(group_by))
        self.request.update(current_aggs)

    def append_terms_aggs(self, current_aggs, group_by_name):
        current_aggs = {
            'aggs': {group_by_name: dict(current_aggs, **{
                'terms': {'field': group_by_name, 'size': 0}
            })}
        }
        return current_aggs

    def append_date_histogram_aggs(self, current_aggs, group_by, group_by_name):
        date_format = None
        if 'to_char' == group_by.tokens[0].get_name():
            to_char_params = list(group_by.tokens[0].get_parameters())
            sql_function = to_char_params[0]
            date_format = eval(to_char_params[1].value)
        else:
            sql_function = group_by
        if 'date_trunc' == sql_function.tokens[0].get_name():
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

    def append_histogram_aggs(self, current_aggs, group_by, group_by_name):
        parameters = tuple(group_by.get_parameters())
        historgram = {'field': parameters[0].get_name(), 'interval': eval(parameters[1].value)}
        if len(parameters) == 3:
            historgram.update(eval(eval(parameters[2].value)))
        current_aggs = {
            'aggs': {group_by_name: dict(current_aggs, **{
                'histogram': historgram
            })}
        }
        return current_aggs

    def append_filter_aggs(self, current_aggs, where):
        filter = filter_translator.create_compound_filter(where.tokens[1:])
        current_aggs = {
            'aggs': {self.sql_select.filter_bucket_key: dict(current_aggs, **{
                'filter': filter
            })}
        }
        return current_aggs

    def append_range_aggs(self, current_aggs, group_by, group_by_name):
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

    def collect_records(self, rows, parent_bucket, group_by_names, props):
        if group_by_names:
            current_response = parent_bucket[group_by_names[0]]
            if 'buckets' in current_response:
                child_buckets = current_response['buckets']
                if isinstance(child_buckets, dict):
                    for child_bucket_key, child_bucket in child_buckets.iteritems():
                        child_props = dict(props, **{group_by_names[0]: child_bucket_key})
                        self.collect_records(rows, child_bucket, group_by_names[1:], child_props)
                else:
                    for child_bucket in child_buckets:
                        child_bucket_key = child_bucket['key_as_string'] if 'key_as_string' in child_bucket else \
                            child_bucket['key']
                        child_props = dict(props, **{group_by_names[0]: child_bucket_key})
                        self.collect_records(rows, child_bucket, group_by_names[1:], child_props)
            else:
                self.collect_records(rows, current_response, group_by_names[1:], props)
        else:
            record = props
            for key, value in parent_bucket.iteritems():
                if isinstance(value, dict) and 'value' in value:
                    record[key] = value['value']
            for metric_name, get_metric in self.metric_selector.iteritems():
                record[metric_name] = get_metric(parent_bucket)
            record['_bucket_'] = parent_bucket
            rows.append(record)


class SelectInsideBranchExecutor(SelectInsideExecutor):
    def __init__(self, sql_select, inner_executor):
        super(SelectInsideBranchExecutor, self).__init__(sql_select)
        self.inner_executor = inner_executor

    def execute(self, inside_aggs=None, parent_pipeline_aggs=None, sibling_pipeline_aggs=None):
        next_level_sibling_pipeline_aggs = None
        next_level_parent_pipeline_aggs = None
        if self.sql_select.source.is_select_inside:
            next_level_sibling_pipeline_aggs = sibling_pipeline_aggs
            sibling_pipeline_aggs = None
            next_level_parent_pipeline_aggs = parent_pipeline_aggs
            parent_pipeline_aggs = None
        merge_aggs(
                self.sql_select, self.request['aggs'],
                inside_aggs=inside_aggs,
                parent_pipeline_aggs=parent_pipeline_aggs,
                sibling_pipeline_aggs=sibling_pipeline_aggs)
        response = self.inner_executor.execute(
                inside_aggs=self.request['aggs'],
                sibling_pipeline_aggs=next_level_sibling_pipeline_aggs,
                parent_pipeline_aggs=next_level_parent_pipeline_aggs)
        return self.select_response(response)

    def select_buckets(self, response):
        # response is selected from inner executor
        buckets = []
        for inner_row in response:
            bucket = inner_row.pop('_bucket_')
            buckets.append((bucket, inner_row))
        return buckets


class SelectInsideLeafExecutor(SelectInsideExecutor):
    def __init__(self, sql_select, search_es):
        super(SelectInsideLeafExecutor, self).__init__(sql_select)
        self.search_es = search_es

    def execute(self, inside_aggs=None, parent_pipeline_aggs=None, sibling_pipeline_aggs=None):
        merge_aggs(
                self.sql_select, self.request['aggs'],
                inside_aggs=inside_aggs,
                parent_pipeline_aggs=parent_pipeline_aggs,
                sibling_pipeline_aggs=sibling_pipeline_aggs)
        response = self.search_es(self.sql_select.source, self.request)
        return self.select_response(response)

    def build_request(self):
        super(SelectInsideLeafExecutor, self).build_request()
        if self.sql_select.where:
            self.request['query'] = filter_translator.create_compound_filter(self.sql_select.where.tokens[1:])

    def select_buckets(self, response):
        # response is returned from elasticsearch
        buckets = []
        bucket = response.get('aggregations', {})
        bucket['doc_count'] = response['hits']['total']
        buckets.append((bucket, {}))
        return buckets
