from sqlparse import sql as stypes


def is_in_mem_computation(sql_select):
    return is_eval(sql_select) or is_pivot(sql_select)


def do_in_mem_computation(sql_select, input_rows):
    if sql_select.where or sql_select.group_by or sql_select.having or sql_select.order_by or sql_select.limit:
        raise Exception('in memory computation is select only')
    if is_eval(sql_select):
        return execute_eval(sql_select, input_rows)
    elif is_pivot(sql_select):
        return execute_pivot(sql_select, input_rows)
    else:
        raise Exception('unknown in memory computation')


def is_eval(sql_select):
    return len(sql_select.projections) == 1 \
           and isinstance(sql_select.projections.values()[0], stypes.Function) \
           and 'EVAL' == sql_select.projections.values()[0].tokens[0].value.upper()


def execute_eval(sql_select, input_rows):
    eval_func = sql_select.projections.values()[0]
    source = eval(eval_func.get_parameters()[0].value)
    compiled_source = compile(source, '', 'exec')
    output_rows = []
    for row in input_rows:
        context = {'input': row, 'output': {}}
        exec (compiled_source, {}, context)
        output_rows.append(context['output'])
    return output_rows


def is_pivot(sql_select):
    return len(sql_select.projections) == 1 \
           and isinstance(sql_select.projections.values()[0], stypes.Function) \
           and 'PIVOT' == sql_select.projections.values()[0].tokens[0].value.upper()


def execute_pivot(sql_select, input_rows):
    pivot_func = sql_select.projections.values()[0]
    params = list(pivot_func.get_parameters())
    pivot_columns = [id.get_name() for id in params[:-1]]
    value_column = params[-1].get_name()
    groups = {}
    for row in input_rows:
        pivot_to = []
        for pivot_column in pivot_columns:
            pivot_to.append('%s_%s' % (pivot_column, row.pop(pivot_column, None)))
        value = row.pop(value_column, None)
        group_key = tuple(sorted(dict(row).items()))
        groups[group_key] = groups.get(group_key, row)
        groups[group_key]['_'.join(pivot_to)] = value
    return groups.values()
