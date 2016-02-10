from sqlparse import sql as stypes
from translators import having_translator

FUNC_NAMES = ('SUM', 'AVG', 'MAX', 'MIN', 'STATS', 'EXTENDED_STATS', 'PERCENTILES')


class SelectFromAllBucketsExecutor(object):
    def __init__(self, sql_select, inner_executor):
        self.sql_select = sql_select
        self.inner_executor = inner_executor
        self.sibling_pipeline_aggs = self.build_sibling_pipeline_aggs()
        self.parent_pipeline_aggs = self.build_parent_pipeline_aggs()

    def execute(self):
        if self.sql_select.group_by or self.sql_select.having \
                or self.sql_select.order_by or self.sql_select.limit:
            raise Exception('select from all buckets is select only')
        response = self.inner_executor.execute(
                sibling_pipeline_aggs=self.sibling_pipeline_aggs,
                parent_pipeline_aggs=self.parent_pipeline_aggs)
        return response

    @classmethod
    def is_select_from_all_buckets(cls, sql_select):
        for projection in sql_select.projections.values():
            if isinstance(projection, stypes.Function):
                sql_func_name = projection.get_name().upper()
                if sql_func_name in FUNC_NAMES:
                    return True
        return False

    def build_sibling_pipeline_aggs(self):
        sibling_pipeline_aggs = {}
        for projection_name, projection in self.sql_select.projections.iteritems():
            if not isinstance(projection, stypes.Function):
                raise Exception('unexpected: %s' % repr(projection))
            params = projection.get_parameters()
            sql_func_name = projection.get_name().upper()
            if sql_func_name in FUNC_NAMES:
                inner_most = self.sql_select.inner_most
                projection = inner_most.projections.get(params[0].get_name())
                bucket_key = '>'.join(inner_most.group_by.keys())
                metric_name = '_count' if having_translator.is_count_star(projection) else projection.get_name()
                if not bucket_key:
                    raise Exception('select from all buckets must nested with group by')
                buckets_path = '%s.%s' % (bucket_key, metric_name)
                sibling_pipeline_aggs[projection_name] = {
                    '%s_bucket' % sql_func_name.lower(): {'buckets_path': buckets_path}
                }
            else:
                raise Exception('unexpected: %s' % repr(projection))
        return sibling_pipeline_aggs

    def build_parent_pipeline_aggs(self):
        if not self.sql_select.where:
            return {}
        inner_most = self.sql_select.inner_most
        current_sql_select = self.sql_select.source
        variables = {}
        while current_sql_select != inner_most:
            bucket_path = '>'.join(current_sql_select.group_by.keys())
            for variable_name in variables.keys():
                variables[variable_name] = '%s>%s' % (bucket_path, variables[variable_name])
            for variable_name, projection in current_sql_select.projections.iteritems():
                if having_translator.is_count_star(projection):
                    variables[variable_name] = '%s._count' % bucket_path
                else:
                    variables[variable_name] = '%s.%s' % (bucket_path, variable_name)
            current_sql_select = current_sql_select.source
        return having_translator.translate_having(inner_most, self.sql_select.where.tokens[1:], variables)
