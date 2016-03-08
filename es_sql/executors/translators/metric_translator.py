import json

from es_sql.sqlparse import sql as stypes
from es_sql.sqlparse import tokens as ttypes
from . import bucket_script_translator


def translate_metrics(sql_select):
    metric_request = {}
    metric_selector = {}
    for projection_name, projection in sql_select.projections.iteritems():
        if projection_name in sql_select.group_by:
            continue
        if ttypes.Wildcard == projection.ttype:
            continue
        request, selector = translate_metric(sql_select, projection, projection_name)
        if request:
            projection_mapped_to = request.pop('_projection_mapped_to_', None)
            if projection_mapped_to:
                bucket_name = projection_mapped_to[0]
                metric_request[bucket_name] = request
                sql_select.projection_mapping[projection_name] = '.'.join(projection_mapped_to)
            else:
                metric_request[projection_name] = request
        if selector:
            metric_selector[projection_name] = selector
    return metric_request, metric_selector


def translate_metric(sql_select, sql_function, projection_name):
    buckets_names = sql_select.buckets_names
    if isinstance(sql_function, stypes.Function):
        sql_function_name = sql_function.tokens[0].value.upper()
        if 'COUNT' == sql_function_name:
            return translate_count(buckets_names, sql_function, projection_name)
        elif sql_function_name in ('MAX', 'MIN', 'AVG', 'SUM'):
            return translate_min_max_avg_sum(buckets_names, sql_function, projection_name)
        elif sql_function_name in ('CSUM', 'DERIVATIVE'):
            return translate_csum_derivative(buckets_names, sql_function, projection_name)
        elif sql_function_name in ('MOVING_AVG', 'SERIAL_DIFF'):
            return translate_moving_avg_serial_diff(buckets_names, sql_function, projection_name)
        elif sql_function_name in (
        'SUM_OF_SQUARES', 'VARIANCE', 'STD_DEVIATION', 'STD_DEVIATION_UPPER_BOUND', 'STD_DEVIATION_LOWER_BOUND'):
            return translate_extended_stats(buckets_names, sql_function, projection_name)
        else:
            raise Exception('unsupported function: %s' % repr(sql_function))
    elif isinstance(sql_function, stypes.Expression):
        return translate_expression(sql_select, sql_function, projection_name)
    else:
        raise Exception('unexpected: %s' % repr(sql_function))


def translate_expression(sql_select, sql_function, projection_name):
    selector = lambda bucket: bucket[projection_name]['value']
    return {'bucket_script': bucket_script_translator.translate_script(sql_select, sql_function.tokens)}, selector

def translate_count(buckets_names, sql_function, projection_name):
    params = sql_function.get_parameters()
    if len(params) == 1 and ttypes.Wildcard == params[0].ttype:
        selector = lambda bucket: bucket['doc_count']
        return None, selector
    else:
        count_keyword = sql_function.tokens[1].token_next_by_type(0, ttypes.Keyword)
        selector = lambda bucket: bucket[projection_name]['value']
        if count_keyword:
            if 'DISTINCT' == count_keyword.value.upper():
                request = {'cardinality': {'field': params[-1].as_field_name()}}
                return request, selector
            else:
                raise Exception('unexpected: %s' % repr(count_keyword))
        else:
            request = {'value_count': {'field': params[0].as_field_name()}}
            return request, selector


def translate_min_max_avg_sum(buckets_names, sql_function, projection_name):
    params = sql_function.get_parameters()
    sql_function_name = sql_function.tokens[0].value.upper()
    if len(params) != 1:
        raise Exception('unexpected: %s' % repr(sql_function))
    selector = lambda bucket: bucket[projection_name]['value']
    field_name = params[0].as_field_name()
    buckets_path = buckets_names.get(field_name)
    if buckets_path:
        request = {'%s_bucket' % sql_function_name.lower(): {'buckets_path': buckets_path}}
    else:
        request = {sql_function_name.lower(): {'field': field_name}}
    return request, selector


def translate_csum_derivative(buckets_names, sql_function, projection_name):
    sql_function_name = sql_function.tokens[0].value.upper()
    params = sql_function.get_parameters()
    selector = lambda bucket: bucket[projection_name]['value'] if projection_name in bucket else None
    field_name = params[0].as_field_name()
    buckets_path = buckets_names.get(field_name)
    if not buckets_path:
        raise Exception('field not found: %s' % field_name)
    metric_type = {
        'CSUM': 'cumulative_sum',
        'CUMULATIVE_SUM': 'cumulative_sum',
        'DERIVATIVE': 'derivative'
    }[sql_function_name]
    request = {metric_type: {'buckets_path': buckets_path}}
    return request, selector


def translate_moving_avg_serial_diff(buckets_names, sql_function, projection_name):
    sql_function_name = sql_function.tokens[0].value.upper()
    params = sql_function.get_parameters()
    selector = lambda bucket: bucket[projection_name]['value'] if projection_name in bucket else None
    field_name = params[0].as_field_name()
    buckets_path = buckets_names.get(field_name)
    if not buckets_path:
        raise Exception('field not found: %s' % field_name)
    request = {sql_function_name.lower(): {'buckets_path': buckets_path}}
    if len(params) > 1:
        request[sql_function_name.lower()].update(json.loads(params[1].value[1:-1]))
    return request, selector

def translate_extended_stats(buckets_names, sql_function, projection_name):
    sql_function_name = sql_function.tokens[0].value.upper()
    params = sql_function.get_parameters()
    if len(params) != 1:
        raise Exception('unexpected: %s' % str(sql_function))
    if not params[0].is_field():
        raise Exception('unexpected: %s' % str(sql_function))
    field = params[0].as_field_name()
    overridden_bucket_name = '%s_extended_stats' % field
    if 'STD_DEVIATION_UPPER_BOUND' == sql_function_name:
        _projection_mapped_to_ = (overridden_bucket_name, 'std_deviation_bounds.upper')
        selector = lambda bucket: bucket[overridden_bucket_name]['std_deviation_bounds']['upper']
    elif 'STD_DEVIATION_LOWER_BOUND' == sql_function_name:
        _projection_mapped_to_ = (overridden_bucket_name, 'std_deviation_bounds.lower')
        selector = lambda bucket: bucket[overridden_bucket_name]['std_deviation_bounds']['lower']
    else:
        _projection_mapped_to_ = (overridden_bucket_name, sql_function_name.lower())
        selector = lambda bucket: bucket[overridden_bucket_name][sql_function_name.lower()]
    request = {'extended_stats': {'field': params[0].as_field_name()}, '_projection_mapped_to_': _projection_mapped_to_}
    return request, selector
