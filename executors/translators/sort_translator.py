import script_translator


def translate_sort(sql_select, agg=None):
    sort = []
    for id in sql_select.order_by or []:
        asc_or_desc = 'asc'
        if 'DESC' == id.tokens[-1].value.upper():
            asc_or_desc = 'desc'
        projection = sql_select.projections.get(id.get_name())
        if not projection:
            raise Exception('can only sort on selected field: %s' % id.get_name())
        if script_translator.is_count_star(projection):
            sort.append({'_count': asc_or_desc})
        elif agg and id.get_name() == agg[0]:
            if 'terms' == agg[1]:
                sort.append({'_term': asc_or_desc})
            else:
                sort.append({'_key': asc_or_desc})
        else:
            sort.append({id.get_name(): asc_or_desc})
    return sort
