import filter_translator


def translate_join(sql_select):
    join_table = sql_select.join_table
    if join_table in sql_select.joinable_results:
        template_filter = filter_translator.create_compound_filter(
            sql_select.join_conditions, sql_select.tables())
        template_filter_str = repr(template_filter)
        join_filters = []
        rows = sql_select.joinable_results[join_table]
        for row in rows:
            this_filter_as_str = template_filter_str
            for k, v in row.iteritems():
                variable_name = "'${%s.%s}'" % (join_table, k)
                this_filter_as_str = this_filter_as_str.replace(variable_name, "'%s'" % v if isinstance(v, basestring) else v)
            join_filters.append(eval(this_filter_as_str))
        return join_filters
    elif join_table in sql_select.joinable_queries:
        raise Exception('not implemented')
    else:
        raise Exception('join table not found: %s' % join_table)