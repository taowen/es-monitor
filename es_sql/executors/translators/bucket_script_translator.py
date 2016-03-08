from es_sql.sqlparse import sql as stypes
from es_sql.sqlparse import tokens as ttypes


def translate_script(sql_select, tokens):
    agg = {'buckets_path': {}, 'script': {'lang': 'expression', 'inline': ''}}
    _translate(sql_select.buckets_names, agg, tokens)
    return agg


def _translate(buckets_names, agg, tokens):
    for token in tokens:
        if token.ttype == ttypes.Keyword and 'AND' == token.value.upper():
            agg['script']['inline'] = '%s%s' % (
                agg['script']['inline'], '&&')
        elif token.ttype == ttypes.Keyword and 'OR' == token.value.upper():
            agg['script']['inline'] = '%s%s' % (
                agg['script']['inline'], '||')
        elif token.is_group():
            _translate(buckets_names, agg, token.tokens)
        else:
            if token.is_field():
                buckets_name = token.as_field_name()
                bucket_path = buckets_names.get(buckets_name)
                if not bucket_path:
                    raise Exception(
                            'having clause referenced variable must exist in select clause: %s' % buckets_name)
                agg['buckets_path'][buckets_name] = bucket_path
                agg['script']['inline'] = '%s%s' % (
                    agg['script']['inline'], buckets_name)
            else:
                agg['script']['inline'] = '%s%s' % (
                    agg['script']['inline'], token.value)


def is_count_star(projection):
    return isinstance(projection, stypes.Function) \
           and 'COUNT' == projection.tokens[0].as_field_name().upper() \
           and 1 == len(projection.get_parameters()) \
           and ttypes.Wildcard == projection.get_parameters()[0].ttype
