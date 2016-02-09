from select_inside_executor import SelectInsideExecutor
from merge_aggs import merge_aggs

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
