from sqlparse import tokens as ttypes
from sqlparse import sql as stypes


def translate_metrics(sql_select):
    metric_request = {}
    metric_selector = {}
    for projection_name, projection in sql_select.projections.iteritems():
        if projection_name in sql_select.group_by:
            continue
        if not isinstance(projection, stypes.Function):
            raise Exception('can only select group by fields or function in aggregation mode')
        request, selector = translate_metric(sql_select.buckets_names, projection, projection_name)
        if request:
            metric_request[projection_name] = request
        if selector:
            metric_selector[projection_name] = selector
    return metric_request, metric_selector


def translate_metric(buckets_names, sql_function, projection_name):
    if not isinstance(sql_function, stypes.Function):
        raise Exception('unexpected: %s' % repr(sql_function))
    sql_function_name = sql_function.tokens[0].value.upper()
    if 'COUNT' == sql_function_name:
        params = list(sql_function.get_parameters())
        if len(params) == 1 and ttypes.Wildcard == params[0].ttype:
            selector = lambda bucket: bucket['doc_count']
            return None, selector
        else:
            count_keyword = sql_function.tokens[1].token_next_by_type(0, ttypes.Keyword)
            selector = lambda bucket: bucket[projection_name]['value']
            if count_keyword:
                if 'DISTINCT' == count_keyword.value.upper():
                    request = {'cardinality': {'field': params[-1].value}}
                    return request, selector
                else:
                    raise Exception('unexpected: %s' % repr(count_keyword))
            else:
                request = {'value_count': {'field': params[0].value}}
                return request, selector
    elif sql_function_name in ('MAX', 'MIN', 'AVG', 'SUM'):
        if len(sql_function.get_parameters()) != 1:
            raise Exception('unexpected: %s' % repr(sql_function))
        selector = lambda bucket: bucket[projection_name]['value']
        field_name = sql_function.get_parameters()[0].as_field_name()
        buckets_path = buckets_names.get(field_name)
        if buckets_path:
            request = {'%s_bucket' % sql_function_name.lower(): {'buckets_path': buckets_path}}
        else:
            request = {sql_function_name.lower(): {'field': field_name}}
        return request, selector
    else:
        raise Exception('unsupported function: %s' % repr(sql_function))
