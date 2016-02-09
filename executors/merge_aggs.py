def merge_aggs(sql_select, aggs, inside_aggs, parent_pipeline_aggs, sibling_pipeline_aggs):
    merge_inside_aggs(sql_select, aggs, inside_aggs)


def merge_inside_aggs(sql_select, aggs, inside_aggs):
    if inside_aggs:
        for bucket_key in sql_select.group_by.keys():
            if '_global_' in aggs:
                aggs = aggs['_global_']['aggs']
            aggs = aggs[bucket_key]['aggs']
        aggs.update(inside_aggs)
