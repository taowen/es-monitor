import filter_translator
from sqlparse import sql as stypes
from sqlparse import tokens as ttypes


def translate_script(sql_select, tokens, include_sub_aggregation=False):
    variables = {}
    if include_sub_aggregation:
        inner_most = sql_select.inner_most
        current_sql_select = sql_select.source
        while current_sql_select != inner_most:
            bucket_path = '>'.join(current_sql_select.group_by.keys())
            for variable_name in variables.keys():
                variables[variable_name] = '%s>%s' % (bucket_path, variables[variable_name])
            for variable_name, projection in current_sql_select.projections.iteritems():
                if is_count_star(projection):
                    variables[variable_name] = '%s._count' % bucket_path
                else:
                    variables[variable_name] = '%s.%s' % (bucket_path, variable_name)
            current_sql_select = current_sql_select.source
        include_sql_selects = [inner_most, sql_select]
    else:
        include_sql_selects = [sql_select]
    for include_sql_select in include_sql_selects:
        for variable_name, projection in include_sql_select.projections.iteritems():
            if is_count_star(projection):
                variables[variable_name] = '_count'
            else:
                variables[variable_name] = variable_name
    agg = {'buckets_path': {}, 'script': {'lang': 'expression', 'inline': ''}}
    _translate(variables, agg, tokens)
    return agg


def _translate(variables, agg, tokens):
    for token in tokens:
        if '@now' in token.value:
            agg['script']['inline'] = '%s%s' % (
                agg['script']['inline'], filter_translator.eval_numeric_value(token.value))
        elif token.ttype == ttypes.Keyword and 'AND' == token.value.upper():
            agg['script']['inline'] = '%s%s' % (
                agg['script']['inline'], '&&')
        elif token.ttype == ttypes.Keyword and 'OR' == token.value.upper():
            agg['script']['inline'] = '%s%s' % (
                agg['script']['inline'], '||')
        elif token.is_group():
            _translate(variables, agg, token.tokens)
        else:
            if ttypes.Name == token.ttype:
                variable_name = token.value
                bucket_path = variables.get(variable_name)
                if not bucket_path:
                    raise Exception(
                            'having clause referenced variable must exist in select clause: %s' % variable_name)
                agg['buckets_path'][variable_name] = bucket_path
            agg['script']['inline'] = '%s%s' % (
                agg['script']['inline'], token.value)


def is_count_star(projection):
    return isinstance(projection, stypes.Function) \
           and 'COUNT' == projection.tokens[0].as_field_name().upper() \
           and 1 == len(projection.get_parameters()) \
           and ttypes.Wildcard == projection.get_parameters()[0].ttype
