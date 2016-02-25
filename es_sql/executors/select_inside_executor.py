from es_sql.sqlparse.ordereddict import OrderedDict
from .translators import bucket_script_translator
from .translators import filter_translator
from .translators import group_by_translator
from .translators import join_translator
from .translators import metric_translator
from .translators import sort_translator
from .select_from_leaf_executor import search_es


class SelectInsideExecutor(object):
    def __init__(self, sql_select):
        self.sql_select = sql_select
        self.request = {}
        self.children = []

    def add_child(self, executor):
        self.children.append(executor)

    def build_request(self):
        buckets_names = self.list_buckets_names()
        for buckets_name, buckets_path in buckets_names.iteritems():
            buckets_names[buckets_name] = self.format_buckets_path(buckets_path)
        self.sql_select.buckets_names = buckets_names
        self.metric_request, self.metric_selector = metric_translator.translate_metrics(self.sql_select)
        self.request['size'] = 0  # do not need hits in response
        self.add_aggs_to_request()
        self.add_children_aggs()

    def list_buckets_names(self):
        buckets_names = {}
        for projection_name, projection in self.sql_select.projections.iteritems():
            if bucket_script_translator.is_count_star(projection):
                buckets_names[projection_name] = ['_count']
            elif projection_name in self.sql_select.group_by:
                buckets_names[projection_name] = ['_key']
            else:
                buckets_names[projection_name] = [projection_name]
        for child_executor in self.children:
            child_prefix = child_executor.sql_select.group_by.keys()
            child_buckets_names = child_executor.list_buckets_names()
            for buckets_name, buckets_path in child_buckets_names.iteritems():
                buckets_names[buckets_name] = child_prefix + buckets_path
        return buckets_names

    def format_buckets_path(self, buckets_path):
        prefix = '>'.join(buckets_path[:-1])
        if prefix:
            return '.'.join([prefix, buckets_path[-1]])
        else:
            return buckets_path[-1]

    def add_children_aggs(self):
        if not self.children:
            return
        aggs = self.request['aggs']
        for bucket_key in self.sql_select.group_by.keys():
            aggs = aggs[bucket_key]['aggs']
        for child_executor in self.children:
            child_executor.build_request()
            child_aggs = child_executor.request['aggs']
            aggs.update(child_aggs)

    def select_response(self, response):
        group_by_names = self.sql_select.group_by.keys() if self.sql_select.group_by else []
        buckets = self.select_buckets(response)
        all_rows = []
        for bucket, inner_row in buckets:
            rows = []
            self.collect_records(rows, bucket, group_by_names, inner_row)
            all_rows.extend(rows)
        all_rows = self.pass_response_to_children(all_rows)
        for row in all_rows:
            filtered = row.pop('_filtered_', {})
            filtered.pop('_bucket_', None)
            row.update(filtered)
        return all_rows

    def pass_response_to_children(self, all_rows):
        filter_children = []
        drill_down_children = []
        for child_executor in self.children:
            if child_executor.is_filter_only():
                filter_children.append(child_executor)
            else:
                drill_down_children.append(child_executor)
        for child_executor in filter_children:
            child_executor.select_response(all_rows)
        if drill_down_children:
            children_rows = []
            for child_executor in drill_down_children:
                children_rows.extend(child_executor.select_response(all_rows))
            return children_rows
        else:
            return all_rows

    def select_buckets(self, response):
        raise Exception('base class')

    def add_aggs_to_request(self):
        self.request['aggs'], tail_aggs = group_by_translator.translate_group_by(self.sql_select.group_by)
        if self.metric_request:
            tail_aggs.update(self.metric_request)
        if self.sql_select.having:
            tail_aggs['having'] = {
                'bucket_selector': bucket_script_translator.translate_script(self.sql_select, self.sql_select.having)
            }
        if self.sql_select.order_by or self.sql_select.limit:
            if len(self.sql_select.group_by or {}) != 1:
                raise Exception('order by can only be applied on single group by')
            group_by_name = self.sql_select.group_by.keys()[0]
            aggs = self.request['aggs'][group_by_name]
            agg_names = set(aggs.keys()) - set(['aggs'])
            if len(agg_names) != 1:
                raise Exception('order by can only be applied on single group by')
            agg_type = list(agg_names)[0]
            agg = aggs[agg_type]
            if self.sql_select.order_by:
                agg['order'] = sort_translator.translate_sort(self.sql_select)
            if self.sql_select.limit:
                agg['size'] = self.sql_select.limit

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
    def __init__(self, sql_select, executor_name):
        super(SelectInsideBranchExecutor, self).__init__(sql_select)
        self.executor_name = executor_name
        self._is_filter_only = len(self.sql_select.group_by) == 0
        if self.sql_select.where:
            old_group_by = self.sql_select.group_by
            self.sql_select.group_by = OrderedDict()
            self.sql_select.group_by[self.executor_name] = self.sql_select.where
            for key in old_group_by.keys():
                self.sql_select.group_by[key] = old_group_by[key]
            self.sql_select.where = None

    def is_filter_only(self):
        return self._is_filter_only and all(child_executor.is_filter_only() for child_executor in self.children)

    def select_buckets(self, response):
        # response is selected from inner executor
        if self.is_filter_only():
            buckets = []
            for parent_row in response:
                bucket = parent_row.get('_bucket_')
                parent_row['_filtered_'] = parent_row.get('_filtered_', {})
                buckets.append((bucket, parent_row['_filtered_']))
            return buckets
        else:
            buckets = []
            for parent_row in response:
                child_row = dict(parent_row)
                child_row['_bucket_path'] = list(child_row.get('_bucket_path', []))
                child_row['_bucket_path'].append(self.executor_name)
                bucket = child_row.get('_bucket_')
                buckets.append((bucket, child_row))
            return buckets


class SelectInsideLeafExecutor(SelectInsideExecutor):
    def __init__(self, sql_select):
        super(SelectInsideLeafExecutor, self).__init__(sql_select)

    def execute(self, es_url, arguments=None):
        url = self.sql_select.generate_url(es_url)
        response = search_es(url, self.request, arguments)
        return self.select_response(response)

    def select_response(self, response):
        rows = super(SelectInsideLeafExecutor, self).select_response(response)
        for row in rows:
            row.pop('_bucket_', None)
        return rows

    def build_request(self):
        super(SelectInsideLeafExecutor, self).build_request()
        if self.sql_select.where:
            self.request['query'] = filter_translator.create_compound_filter(self.sql_select.where.tokens[1:])
        if self.sql_select.join_table:
            join_filters = join_translator.translate_join(self.sql_select)
            if len(join_filters) == 1:
                self.request['query'] = {
                    'bool': {'filter': [self.request.get('query', {}), join_filters[0]]}}
            else:
                self.request['query'] = {
                    'bool': {'filter': self.request.get('query', {}),
                             'should': join_filters}}

    def select_buckets(self, response):
        # response is returned from elasticsearch
        buckets = []
        bucket = response.get('aggregations', {})
        bucket['doc_count'] = response['hits']['total']
        buckets.append((bucket, {}))
        return buckets
