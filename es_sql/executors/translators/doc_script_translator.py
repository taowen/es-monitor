from es_sql.sqlparse import sql as stypes
from es_sql.sqlparse import tokens as ttypes


def translate_script(tokens):
    agg = translate_as_multiple_value(tokens)
    if len(agg['fields']) == 0:
        raise Exception('doc script does not reference any field')
    elif len(agg['fields']) == 1:
        return translate_as_single_value(tokens)
    else:
        agg.pop('fields')
        return agg


def translate_as_multiple_value(tokens):
    agg = {'fields': set(), 'script': {'lang': 'expression', 'inline': ''}}
    _translate(agg, tokens, is_single_value=False)
    return agg


def translate_as_single_value(tokens):
    agg = {'fields': set(), 'script': {'lang': 'expression', 'inline': ''}}
    _translate(agg, tokens, is_single_value=True)
    agg['field'] = list(agg.pop('fields'))[0]
    return agg


def _translate(agg, tokens, is_single_value):
    for token in tokens:
        if token.ttype == ttypes.Keyword and 'AND' == token.value.upper():
            agg['script']['inline'] = '%s%s' % (
                agg['script']['inline'], '&&')
        elif token.ttype == ttypes.Keyword and 'OR' == token.value.upper():
            agg['script']['inline'] = '%s%s' % (
                agg['script']['inline'], '||')
        elif isinstance(token, stypes.Function):
            agg['script']['inline'] = '%s%s(' % (
                agg['script']['inline'], token.get_function_name())
            _translate(agg, token.get_parameters(), is_single_value)
            agg['script']['inline'] = '%s)' % (
                agg['script']['inline'])
        elif token.is_group():
            _translate(agg, token.tokens, is_single_value)
        else:
            if token.is_field():
                field_name = token.as_field_name()
                agg['fields'].add(field_name)
                if is_single_value:
                    agg['script']['inline'] = '%s%s' % (
                        agg['script']['inline'], '_value')
                else:
                    agg['script']['inline'] = '%s%s' % (
                        agg['script']['inline'], "doc['%s'].value" % field_name)
            else:
                agg['script']['inline'] = '%s%s' % (
                    agg['script']['inline'], token.value)
