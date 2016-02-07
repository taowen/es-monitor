from sqlparse import tokens as ttypes
from sqlparse import sql as stypes
import filter_translator
import case_when_translator
import metric_translator
import having_translator


def translate_select_inside(sql_select):
    translator = Translator(sql_select)
    return translator.request, translator


class Translator(object):
    def __init__(self, sql_select):
        self.sql_select = sql_select
        self.request = {}
        if self.is_aggregation():
            self.metric_request, self.metric_selector = metric_translator.translate_metrics(sql_select)
            self.build_aggregation_request()
        else:
            self.build_non_aggregation_request()

    def __call__(self, response):  # select_response()
        if self.is_aggregation():
            return self.select_aggregation_response(response)
        else:
            return self.select_non_aggregation_response(response)

    def is_aggregation(self):
        return self.sql_select.group_by or self.has_function_projection()

    def has_function_projection(self):
        for projection in self.sql_select.projections.values():
            if isinstance(projection, stypes.Function):
                return True
        return False

    def build_aggregation_request(self):
        self.request['size'] = 0  # do not need hits in response
        reversed_group_by_names = list(reversed(self.sql_select.group_by.keys())) if self.sql_select.group_by else []
        self.add_aggs_to_request(reversed_group_by_names)
        if isinstance(self.sql_select.select_from, basestring) and self.sql_select.where:
            self.request['query'] = filter_translator.create_compound_filter(self.sql_select.where.tokens[1:])
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

    def select_aggregation_response(self, response):
        group_by_names = self.sql_select.group_by.keys() if self.sql_select.group_by else []
        rows = []
        if isinstance(response, list):
            for inner_row in response:
                bucket = inner_row.pop('_bucket_')
        else:
            bucket = response['aggregations']
        if '_global_' in bucket:
            bucket = bucket['_global_']
        else:
            sibling_keys = set(bucket.keys()) - set(group_by_names)
            sibling = {}
            for sibling_key in sibling_keys:
                sibling[sibling_key] = bucket[sibling_key]['value']
            self.collect_records(rows, bucket, group_by_names, {})
            for row in rows:
                row.update(sibling)
        return rows

    def add_aggs_to_request(self, group_by_names):
        current_aggs = {'aggs': {}}
        if self.metric_request:
            current_aggs = {'aggs': self.metric_request}
        if self.sql_select.having:
            current_aggs['aggs'].update(having_translator.translate_having(self.sql_select, self.sql_select.having))
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
        current_aggs = self.append_global_agg(current_aggs)
        self.request.update(current_aggs)

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
        if self.sql_select.where:
            if isinstance(self.sql_select.select_from, basestring):
                filter = {}
            else:
                filter = filter_translator.create_compound_filter(self.sql_select.where.tokens[1:])
        else:
            filter = {}
        current_aggs = {
            'aggs': {'_global_': dict(current_aggs, **{
                'filter': filter
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

    def collect_records(self, rows, parent_bucket, group_by_names, props):
        if group_by_names:
            current_response = parent_bucket[group_by_names[0]]
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
            record = props
            for metric_name, get_metric in self.metric_selector.iteritems():
                record[metric_name] = get_metric(parent_bucket)
            record['_bucket_'] = parent_bucket
            rows.append(record)

    def build_non_aggregation_request(self):
        if self.sql_select.order_by:
            self.request['sort'] = self.create_sort()
        if self.sql_select.limit:
            self.request['size'] = self.sql_select.limit
        if self.sql_select.where:
            self.request['query'] = filter_translator.create_compound_filter(self.sql_select.where.tokens[1:])

    def select_non_aggregation_response(self, response):
        rows = []
        for input in response['hits']['hits']:
            row = {}
            for projection_name, projection in self.sql_select.projections.iteritems():
                if projection.ttype == ttypes.Wildcard:
                    row = input['_source']
                else:
                    path = eval(projection.value) if projection.value.startswith('"') else projection.value
                    if path in input.keys():
                        row[projection_name] = input[path]
                    else:
                        row[projection_name] = eval(path, {}, input['_source'])
            rows.append(row)
        return rows

    def create_sort(self, agg=None):
        sort = []
        for id in self.sql_select.order_by or []:
            asc_or_desc = 'asc'
            if 'DESC' == id.tokens[-1].value.upper():
                asc_or_desc = 'desc'
            projection = self.sql_select.projections.get(id.get_name())
            if not projection:
                raise Exception('can only sort on selected field: %s' % id.get_name())
            if having_translator.is_count_star(projection):
                sort.append({'_count': asc_or_desc})
            elif agg and id.get_name() == agg[0]:
                if 'terms' == agg[1]:
                    sort.append({'_term': asc_or_desc})
                else:
                    sort.append({'_key': asc_or_desc})
            else:
                sort.append({id.get_name(): asc_or_desc})
        return sort


def get_object_member(obj, paths):
    if obj is None:
        return None
    if len(paths) == 1:
        return obj.get(paths[0])
    else:
        return get_object_member(obj.get(paths[0]), paths[1:])
