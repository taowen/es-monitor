from translators import script_translator
from translators import filter_translator
from sqlparse import sql as stypes
from sqlparse import tokens as ttypes


class SelectFromPerBucketExecutor(object):
    def __init__(self, sql_select, inner_executor):
        self.sql_select = sql_select
        self.inner_executor = inner_executor
        self.parent_pipeline_aggs = {}
        if sql_select.having:
            bucket_selector_agg = script_translator.translate_script(
                    sql_select, sql_select.having,
                    include_sub_aggregation=True)
            self.parent_pipeline_aggs['having'] = {'bucket_selector': bucket_selector_agg}
        for projection_name, projection in sql_select.projections.iteritems():
            tokens = projection.tokens
            if isinstance(tokens[0], stypes.Parenthesis):
                tokens = tokens[0].tokens[1:-1]
            bucket_script_agg = script_translator.translate_script(
                    sql_select, tokens,
                    include_sub_aggregation=True)
            self.parent_pipeline_aggs[projection_name] = {'bucket_script': bucket_script_agg}

    def execute(self):
        if self.sql_select.group_by or self.sql_select.where \
                or self.sql_select.order_by or self.sql_select.limit:
            raise Exception('select from per buckets only support HAVING and SELECT')
        response = self.inner_executor.execute(
                parent_pipeline_aggs=self.parent_pipeline_aggs)
        return response


def _translate(variables, bucket_script_aggs, tokens):
    for token in tokens:
        if '@now' in token.value:
            bucket_script_aggs['script']['inline'] = '%s%s' % (
                bucket_script_aggs['script']['inline'], filter_translator.eval_numeric_value(token.value))
        elif token.ttype == ttypes.Keyword and 'AND' == token.value.upper():
            bucket_script_aggs['script']['inline'] = '%s%s' % (
                bucket_script_aggs['script']['inline'], '&&')
        elif token.ttype == ttypes.Keyword and 'OR' == token.value.upper():
            bucket_script_aggs['script']['inline'] = '%s%s' % (
                bucket_script_aggs['script']['inline'], '||')
        elif token.is_group():
            _translate(variables, bucket_script_aggs, token.tokens)
        else:
            if ttypes.Name == token.ttype:
                variable_name = token.value
                bucket_path = variables.get(variable_name)
                if not bucket_path:
                    raise Exception(
                            'having clause referenced variable must exist in select clause: %s' % variable_name)
                bucket_script_aggs['buckets_path'][variable_name] = bucket_path
            bucket_script_aggs['script']['inline'] = '%s%s' % (
                bucket_script_aggs['script']['inline'], token.value)
