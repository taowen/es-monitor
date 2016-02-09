def merge_aggs(sql_select, aggs, inside_aggs, parent_pipeline_aggs, sibling_pipeline_aggs):
    merge_sibling_pipeline_aggs(sql_select, aggs, sibling_pipeline_aggs)
    merge_inside_aggs(sql_select, aggs, inside_aggs)
    merge_inside_aggs(sql_select, aggs, parent_pipeline_aggs)


def merge_inside_aggs(sql_select, aggs, inside_aggs):
    # at metric level
    if inside_aggs:
        if '_global_' in aggs:
            aggs = aggs['_global_']['aggs']
        for bucket_key in sql_select.group_by.keys():
            aggs = aggs[bucket_key]['aggs']
        aggs.update(inside_aggs)


def merge_sibling_pipeline_aggs(sql_select, aggs, sibling_pipeline_aggs):
    # at buckets level
    if sibling_pipeline_aggs:
        if '_global_' in aggs:
            if aggs['_global_']['filter']:
                raise Exception('sibling pipeline does not support filter aggregation yet: %s' % aggs)
            aggs = aggs['_global_']['aggs']
        aggs.update(sibling_pipeline_aggs)
