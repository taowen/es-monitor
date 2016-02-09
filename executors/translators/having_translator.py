import filter_translator
from sqlparse import sql as stypes
from sqlparse import tokens as ttypes


def translate_having(sql_select, tokens):
    bucket_selector_agg = {'buckets_path': {}, 'script': {'lang': 'expression', 'inline': ''}}
    _translate(sql_select, bucket_selector_agg, tokens)
    return {'having': {'bucket_selector': bucket_selector_agg}}


def _translate(sql_select, bucket_selector_agg, tokens):
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
            _translate(sql_select, bucket_selector_agg, token.tokens)
        else:
            if ttypes.Name == token.ttype:
                variable_name = token.value
                projection = sql_select.projections.get(variable_name)
                if not projection:
                    raise Exception(
                        'having clause referenced variable must exist in select clause: %s' % variable_name)
                if is_count_star(projection):
                    bucket_selector_agg['buckets_path'][variable_name] = '_count'
                else:
                    bucket_selector_agg['buckets_path'][variable_name] = variable_name
            bucket_selector_agg['script']['inline'] = '%s%s' % (
                bucket_selector_agg['script']['inline'], token.value)


def is_count_star(projection):
    return isinstance(projection, stypes.Function) \
           and 'COUNT' == projection.tokens[0].get_name().upper() \
           and not projection.get_parameters()
