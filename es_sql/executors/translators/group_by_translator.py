from es_sql.sqlparse import sql as stypes
from . import case_when_translator
from . import doc_script_translator
from . import filter_translator


def translate_group_by(group_by_map):
    aggs = {}
    tail_aggs = aggs
    for group_by_name, group_by in group_by_map.iteritems():
        if isinstance(group_by, stypes.Parenthesis):
            if len(group_by.tokens > 3):
                raise Exception('unexpected: %s' % group_by)
            group_by = group_by.tokens[1]
        if group_by.is_field():
            tail_aggs = append_terms_aggs(tail_aggs, group_by, group_by_name)
        elif isinstance(group_by, stypes.Case):
            tail_aggs = append_range_aggs(tail_aggs, group_by, group_by_name)
        elif isinstance(group_by, stypes.Function):
            sql_function_name = group_by.tokens[0].value.upper()
            if sql_function_name in ('DATE_TRUNC', 'TO_CHAR'):
                tail_aggs = append_date_histogram_aggs(tail_aggs, group_by, group_by_name)
            elif 'HISTOGRAM' == sql_function_name:
                tail_aggs = append_histogram_aggs(tail_aggs, group_by, group_by_name)
            else:
                tail_aggs = append_terms_aggs_with_script(tail_aggs, group_by, group_by_name)
        elif isinstance(group_by, stypes.Expression):
            tail_aggs = append_terms_aggs_with_script(tail_aggs, group_by, group_by_name)
        elif isinstance(group_by, stypes.Where):
            tail_aggs = append_filter_aggs(tail_aggs, group_by, group_by_name)
        else:
            raise Exception('unexpected: %s' % repr(group_by))
    return aggs, tail_aggs


def append_terms_aggs(tail_aggs, group_by, group_by_name):
    new_tail_aggs = {}
    tail_aggs[group_by_name] = {
        'terms': {'field': group_by.as_field_name(), 'size': 0},
        'aggs': new_tail_aggs
    }
    return new_tail_aggs


def append_terms_aggs_with_script(tail_aggs, group_by, group_by_name):
    new_tail_aggs = {}
    script = doc_script_translator.translate_script([group_by])
    script['size'] = 0
    tail_aggs[group_by_name] = {
        'terms': script,
        'aggs': new_tail_aggs
    }
    return new_tail_aggs


def append_date_histogram_aggs(tail_aggs, group_by, group_by_name):
    new_tail_aggs = {}
    date_format = None
    if 'TO_CHAR' == group_by.tokens[0].value.upper():
        to_char_params = list(group_by.get_parameters())
        sql_function = to_char_params[0]
        date_format = to_char_params[1].value[1:-1]\
            .replace('%Y', 'yyyy')\
            .replace('%m', 'MM')\
            .replace('%d', 'dd')\
            .replace('%H', 'hh')\
            .replace('%M', 'mm')\
            .replace('%S', 'ss')
    else:
        sql_function = group_by
    if 'DATE_TRUNC' == sql_function.tokens[0].value.upper():
        parameters = list(sql_function.get_parameters())
        if len(parameters) != 2:
            raise Exception('incorrect parameters count: %s' % list(parameters))
        interval, field = parameters
        tail_aggs[group_by_name] = {
            'date_histogram': {
                'field': field.as_field_name(),
                'time_zone': '+08:00',
                'interval': eval(interval.value)
            },
            'aggs': new_tail_aggs
        }
        if date_format:
            tail_aggs[group_by_name]['date_histogram']['format'] = date_format
    else:
        raise Exception('unexpected: %s' % repr(sql_function))
    return new_tail_aggs


def append_histogram_aggs(tail_aggs, group_by, group_by_name):
    new_tail_aggs = {}
    parameters = tuple(group_by.get_parameters())
    historgram = {'field': parameters[0].as_field_name(), 'interval': eval(parameters[1].value)}
    if len(parameters) == 3:
        historgram.update(eval(eval(parameters[2].value)))
    tail_aggs[group_by_name] = {
        'histogram': historgram,
        'aggs': new_tail_aggs
    }
    return new_tail_aggs


def append_filter_aggs(tail_aggs, where, group_by_name):
    new_tail_aggs = {}
    filter = filter_translator.create_compound_filter(where.tokens[1:])
    tail_aggs[group_by_name] = {
        'filter': filter,
        'aggs': new_tail_aggs
    }
    return new_tail_aggs


def append_range_aggs(tail_aggs, case_when, group_by_name):
    new_tail_aggs = {}
    case_when_aggs = case_when_translator.translate_case_when(case_when)
    tail_aggs[group_by_name] = case_when_aggs
    tail_aggs[group_by_name]['aggs'] = new_tail_aggs
    return new_tail_aggs
