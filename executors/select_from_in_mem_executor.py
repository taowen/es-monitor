import in_mem_computation


class SelectFromInMemExecutor(object):
    is_in_mem_computation = in_mem_computation.is_in_mem_computation

    def __init__(self, sql_select, inner_executor):
        self.sql_select = sql_select
        self.inner_executor = inner_executor

    def execute(self, inside_aggs=None, parent_pipeline_aggs=None, sibling_pipeline_aggs=None):
        if inside_aggs or parent_pipeline_aggs or sibling_pipeline_aggs:
            raise Exception('in memory computation can not nest other aggregation')
        response = self.inner_executor.execute()
        return in_mem_computation.do_in_mem_computation(self.sql_select, response)
