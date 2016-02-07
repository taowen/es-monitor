from sqlparse import tokens as ttypes
from sqlparse import sql as stypes
import filter_translator
import having_translator
from select_inside_translator import get_object_member
import functools


def translate_select_from(sql_select):
    if sql_select.where:
        parent_pipeline_agg = having_translator.translate_having(sql_select.select_from, sql_select.where.tokens[1:])
    else:
        parent_pipeline_agg = None
    sibling_pipeline_agg = {}
    translate_projections(sql_select)
    return parent_pipeline_agg, sibling_pipeline_agg, functools.partial(select_response, sql_select)


def translate_projections(sql_select):
    for projection_name, projection in sql_select.projections.iteritems():
        if isinstance(projection, stypes.Function):
            translate_function(sql_select, projection)


def translate_function(sql_select, sql_function):
    sql_function_name = sql_function.tokens[0].get_name().upper()
    params = sql_function.get_parameters()
    if 'SUM' == sql_function_name:
        projection_name = params[0].get_name()
        bucket_keys, projection = sql_select.get_inside_projection(projection_name)
        print(bucket_keys, projection)
    else:
        raise Exception('unsupported function: %s' % sql_function_name)


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
