import script_translator
from sqlparse import sql as stypes


def translate_sort(sql_select, agg=None):
    sort = []
    for id in sql_select.order_by or []:
        asc_or_desc = 'asc'
        if type(id) == stypes.Identifier:
            if 'DESC' == id.tokens[-1].value.upper():
                asc_or_desc = 'desc'
            field_name = id.get_name()
            projection = sql_select.projections.get(field_name)
        else:
            field_name = str(id)
            projection = sql_select.projections.get(field_name)
        if projection and script_translator.is_count_star(projection):
            sort.append({'_count': asc_or_desc})
        elif agg and field_name == agg[0]:
            if 'terms' == agg[1]:
                sort.append({'_term': asc_or_desc})
            else:
                sort.append({'_key': asc_or_desc})
        else:
            sort.append({field_name: asc_or_desc})
    return sort
