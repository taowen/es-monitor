from es_sql.sqlparse import sql as stypes


def translate_sort(sql_select):
    sort = []
    for id in sql_select.order_by or []:
        asc_or_desc = 'asc'
        if type(id) == stypes.Identifier:
            if 'DESC' == id.tokens[-1].value.upper():
                asc_or_desc = 'desc'
            field_name = id.tokens[0].as_field_name()
            projection = sql_select.projections.get(field_name)
        else:
            field_name = id.as_field_name()
            projection = sql_select.projections.get(field_name)
        group_by = sql_select.group_by.get(field_name)
        if group_by and group_by.is_field():
            sort.append({'_term': asc_or_desc})
        else:
            buckets_path = sql_select.buckets_names.get(field_name)
            if field_name in sql_select.projection_mapping:
                sort.append({sql_select.projection_mapping[field_name]: asc_or_desc})
            elif buckets_path:
                sort.append({buckets_path: asc_or_desc})
            else:
                sort.append({field_name: asc_or_desc})
    return sort[0] if len(sort) == 1 else sort
