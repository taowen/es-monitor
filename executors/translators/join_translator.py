import filter_translator


def translate_join(sql_select):
    join_table = sql_select.join_table
    if join_table in sql_select.joinable_results:
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
    elif join_table in sql_select.joinable_queries:
        raise Exception('not implemented')
    else:
        raise Exception('join table not found: %s' % join_table)


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
