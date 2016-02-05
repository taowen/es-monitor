from sqlparse import tokens as ttypes
from select_inside_translator import filter_translator
from select_inside_translator.select_translator import is_count_star


def translate_select(sql_select):
    tokens = sql_select.where.tokens
    bucket_selector_agg = {'buckets_path': {}, 'script': {'lang': 'expression', 'inline': ''}}
    Translator(sql_select).process_where(bucket_selector_agg, tokens[1:])
    return{'having': {'bucket_selector': bucket_selector_agg}}


class Translator(object):
    def __init__(self, sql_select):
        self.sql_select = sql_select

    def process_where(self, bucket_selector_agg, tokens):
        for token in tokens:
            if '@now' in token.value:
                bucket_selector_agg['script']['inline'] = '%s%s' % (
                    bucket_selector_agg['script']['inline'], filter_translator.eval_numeric_value(token.value))
            elif token.ttype == ttypes.Keyword and 'AND' == token.value.upper():
                bucket_selector_agg['script']['inline'] = '%s%s' % (
                    bucket_selector_agg['script']['inline'], '&&')
            elif token.ttype == ttypes.Keyword and 'OR' == token.value.upper():
                bucket_selector_agg['script']['inline'] = '%s%s' % (
                    bucket_selector_agg['script']['inline'], '||')
            elif token.is_group():
                self.process_where(bucket_selector_agg, token.tokens)
            else:
                if ttypes.Name == token.ttype:
                    variable_name = token.value
                    projection = self.sql_select.select_from.projections.get(variable_name)
                    if not projection:
                        raise Exception(
                            'having clause referenced variable must exist in select clause: %s' % variable_name)
                    if is_count_star(projection):
                        bucket_selector_agg['buckets_path'][variable_name] = '_count'
                    else:
                        bucket_selector_agg['buckets_path'][variable_name] = variable_name
                bucket_selector_agg['script']['inline'] = '%s%s' % (
                    bucket_selector_agg['script']['inline'], token.value)
