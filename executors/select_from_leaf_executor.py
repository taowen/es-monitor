from sqlparse import tokens as ttypes
from translators import filter_translator
from translators import sort_translator


class SelectFromLeafExecutor(object):
    def __init__(self, sql_select, search_es):
        self.sql_select = sql_select
        self.request = self.build_request()
        self.search_es = search_es

    def execute(self, inside_aggs=None, parent_pipeline_aggs=None, sibling_pipeline_aggs=None):
        if inside_aggs or parent_pipeline_aggs or sibling_pipeline_aggs:
            raise Exception('leaf select from can not nest other aggregation')
        response = self.search_es(self.sql_select.source, self.request)
        return self.select_response(response)

    def build_request(self):
        request = {}
        if self.sql_select.order_by:
            request['sort'] = sort_translator.translate_sort(self.sql_select)
        if self.sql_select.limit:
            request['size'] = self.sql_select.limit
        if self.sql_select.where:
            request['query'] = filter_translator.create_compound_filter(self.sql_select.where.tokens[1:])
        return request

    def select_response(self, response):
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
