import functools

from elasticsearch_sql.sqlparse import sql as stypes
from elasticsearch_sql.sqlparse import tokens as ttypes
from .translators import filter_translator
from .translators import join_translator
from .translators import sort_translator


class SelectFromLeafExecutor(object):
    def __init__(self, sql_select, search_es):
        self.sql_select = sql_select
        self.request = self.build_request()
        self.search_es = search_es
        self.selectors = []
        for projection_name, projection in self.sql_select.projections.iteritems():
            if projection.ttype == ttypes.Wildcard:
                self.selectors.append(select_wildcard)
            elif projection.ttype == ttypes.Name:
                self.selectors.append(functools.partial(
                    select_name, projection_name=projection_name, projection=projection))
            else:
                python_script = translate_projection_to_python(projection)
                python_code = compile(python_script, '', 'eval')
                self.selectors.append(functools.partial(
                    select_by_python_code, projection_name=projection_name, python_code=python_code))

    def execute(self):
        response = self.search_es(self.sql_select.from_indices, self.request)
        return self.select_response(response)

    def build_request(self):
        request = {}
        if self.sql_select.order_by:
            request['sort'] = sort_translator.translate_sort(self.sql_select)
        if self.sql_select.limit:
            request['size'] = self.sql_select.limit
        if self.sql_select.where:
            request['query'] = filter_translator.create_compound_filter(self.sql_select.where.tokens[1:])
        if self.sql_select.join_table:
            join_filters = join_translator.translate_join(self.sql_select)
            if len(join_filters) == 1:
                request['query'] = {
                    'bool': {'filter': [request.get('query', {}), join_filters[0]]}}
            else:
                request['query'] = {
                    'bool': {'filter': request.get('query', {}),
                             'should': join_filters}}
        return request

    def select_response(self, response):
        rows = []
        for input in response['hits']['hits']:
            row = {}
            for selector in self.selectors:
                selector(input, row)
            rows.append(row)
        return rows


def select_wildcard(input, row):
    row.update(input['_source'])
    row['_id'] = input['_id']
    row['_type'] = input['_type']
    row['_index'] = input['_index']


def select_name(input, row, projection_name, projection):
    projection_as_str = str(projection)
    if projection_as_str in input:
        row[projection_name] = input[projection_as_str]
    elif projection_as_str in input['_source']:
        row[projection_name] = input['_source'][projection_as_str]
    else:
        row[projection_name] = None


def select_by_python_code(input, row, projection_name, python_code):
    row[projection_name] = eval(python_code, {}, input['_source'])


def translate_projection_to_python(projection):
    if isinstance(projection, stypes.DotName):
        return translate_symbol(str(projection))
    if isinstance(projection, stypes.TokenList):
        tokens = list(projection.flatten())
    else:
        tokens = [projection]
    translated = []
    for token in tokens:
        if token.ttype == ttypes.String.Symbol:
            translated.append(translate_symbol(token.value[1:-1]))
        else:
            translated.append(str(token))
    return ''.join(translated)


def translate_symbol(value):
    path = value.split('.')
    return ''.join([path[0], "['", "']['".join(path[1:]), "']"])
