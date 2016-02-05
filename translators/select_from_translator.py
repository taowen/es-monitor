from sqlparse import tokens as ttypes
import filter_translator
import having_translator
from select_inside_translator import get_object_member
import functools


def translate_select_from(sql_select):
    if sql_select.where:
        bucket_selector_agg = having_translator.translate_having(sql_select.select_from, sql_select.where.tokens[1:])
    else:
        bucket_selector_agg = None
    return bucket_selector_agg, functools.partial(select_response, sql_select)


def select_response(sql_select, response):
    rows = []
    for input in response:
        row = {}
        for projection_name, projection in sql_select.projections.iteritems():
            if projection.ttype == ttypes.Wildcard:
                row = input
            elif projection.ttype in (ttypes.String.Symbol, ttypes.Name):
                path = eval(projection.value) if projection.value.startswith('"') else projection.value
                if path in input.keys():
                    row[projection_name] = input[path]
                else:
                    row[projection_name] = get_object_member(input, path.split('.'))
            else:
                raise Exception('unexpected: %s' % repr(projection))
        row['_bucket_'] = input.get('_bucket_')
        rows.append(row)
    return rows
