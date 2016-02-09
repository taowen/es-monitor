from select_inside_executor import SelectInsideExecutor
from merge_aggs import merge_aggs

class SelectInsideBranchExecutor(SelectInsideExecutor):
    def __init__(self, sql_select, inner_executor):
        super(SelectInsideBranchExecutor, self).__init__(sql_select)
        self.inner_executor = inner_executor

    def execute(self, inside_aggs=None, parent_pipeline_aggs=None, sibling_pipeline_aggs=None):
        merge_aggs(
                self.sql_select, self.request['aggs'],
                inside_aggs=inside_aggs,
                parent_pipeline_aggs=parent_pipeline_aggs,
                sibling_pipeline_aggs=sibling_pipeline_aggs)
        response = self.inner_executor.execute(inside_aggs=self.request['aggs'])
        return self.select_response(response)
