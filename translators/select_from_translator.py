from sqlparse import tokens as ttypes
from sqlparse import sql as stypes
import filter_translator
import having_translator
from select_inside_translator import get_object_member
import functools


def translate_select_from(sql_select):
    if sql_select.where:
        parent_pipeline_aggs = having_translator.translate_having(sql_select.select_from, sql_select.where.tokens[1:])
    else:
        parent_pipeline_aggs = None
    sibling_pipeline_aggs = translate_projections(sql_select)
    return parent_pipeline_aggs, sibling_pipeline_aggs


def translate_projections(sql_select):
    sibling_pipeline_aggs = {}
    for projection_name, projection in sql_select.projections.iteritems():
        if isinstance(projection, stypes.Identifier):
            projection = projection.tokens[0]
        if isinstance(projection, stypes.Function):
            sibling_pipeline_aggs[projection_name] = translate_function(
                sql_select, projection_name, projection)
    return sibling_pipeline_aggs


def translate_function(sql_select, projection_name, sql_function):
    sql_function_name = sql_function.tokens[0].get_name().upper()
    params = sql_function.get_parameters()
    if 'SUM' == sql_function_name:
        projection_name = params[0].get_name()
        bucket_keys_levels, projection = sql_select.get_inside_projection(projection_name)
        bucket_keys_levels[0] = bucket_keys_levels[0][1:]
        buckets_path = '>'.join(['>'.join(level) for level in bucket_keys_levels])
        if buckets_path:
            buckets_path = '%s.%s' % (
                buckets_path, '_count' if having_translator.is_count_star(projection) else projection.get_name())
        else:
            buckets_path = projection.get_name()
        return {'sum_bucket': {'buckets_path': buckets_path}}
    else:
        raise Exception('unsupported function: %s' % sql_function_name)
