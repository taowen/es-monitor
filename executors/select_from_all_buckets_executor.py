from sqlparse import sql as stypes
from translators import having_translator

FUNC_NAMES = ('SUM', 'AVG', 'MAX', 'MIN', 'STATS', 'EXTENDED_STATS', 'PERCENTILES')


class SelectFromAllBucketsExecutor(object):
    def __init__(self, sql_select, inner_executor):
        self.sql_select = sql_select
        self.inner_executor = inner_executor
        self.sibling_pipeline_aggs = self.build_request()

    def execute(self):
        response = self.inner_executor.execute(sibling_pipeline_aggs=self.sibling_pipeline_aggs)
        return response

    @classmethod
    def is_select_from_all_buckets(cls, sql_select):
        for projection in sql_select.projections.values():
            if isinstance(projection, stypes.Function):
                sql_func_name = projection.get_name().upper()
                if sql_func_name in FUNC_NAMES:
                    return True
        return False

    def build_request(self):
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
                buckets_path = '%s.%s' % (bucket_key, metric_name)
                sibling_pipeline_aggs[projection_name] = {
                    '%s_bucket' % sql_func_name.lower(): {'buckets_path': buckets_path}
                }
            else:
                raise Exception('unexpected: %s' % repr(projection))
        return sibling_pipeline_aggs
