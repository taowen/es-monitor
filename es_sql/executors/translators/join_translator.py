from . import filter_translator


def translate_join(sql_select):
    join_table = sql_select.join_table
    if join_table in sql_select.joinable_results:
        return translate_client_side_join(join_table, sql_select)
    elif join_table in sql_select.joinable_queries:
        other_executor = sql_select.joinable_queries[join_table]
        template_filter = filter_translator.create_compound_filter(
            sql_select.join_conditions, sql_select.tables())
        term_filter = template_filter.get('term')
        if not term_filter:
            raise Exception('server side join can only on simple equal condition')
        if len(term_filter.keys()) > 1:
            raise Exception('server side join can only on simple equal condition')
        field = term_filter.keys()[0]
        field_ref = term_filter[field]
        return [{
            'filterjoin': {
                field: {
                    'indices': '%s*' % other_executor.sql_select.from_table,
                    'path': field_ref.field,
                    'query': other_executor.request['query']
                }
            }
        }]
    else:
        raise Exception('join table not found: %s' % join_table)


def translate_client_side_join(join_table, sql_select):
    template_filter = filter_translator.create_compound_filter(
        sql_select.join_conditions, sql_select.tables())
    rows = sql_select.joinable_results[join_table]
    terms_filter = optimize_as_terms(template_filter, rows)
    if terms_filter:
        return [terms_filter]
    template_filter_str = repr(template_filter)
    join_filters = []
    for row in rows:
        this_filter_as_str = template_filter_str
        for k, v in row.iteritems():
            variable_name = '${%s.%s}' % (join_table, k)
            this_filter_as_str = this_filter_as_str.replace(variable_name,
                                                            "'%s'" % v if isinstance(v, basestring) else v)
        join_filters.append(eval(this_filter_as_str))
    return join_filters


def optimize_as_terms(template_filter, rows):
    if not template_filter.get('term'):
        return None
    term_filter = template_filter['term']
    if len(term_filter) > 1:
        return None
    field = term_filter.keys()[0]
    field_ref = term_filter[field]
    terms = [row[field_ref.field] for row in rows]
    return {'terms': {field: terms}}
