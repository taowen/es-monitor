def merge_aggs(sql_select, aggs, inside_aggs=None, parent_pipeline_aggs=None, sibling_pipeline_aggs=None):
    merge_sibling_pipeline_aggs(sql_select, aggs, sibling_pipeline_aggs)
    merge_inside_aggs(sql_select, aggs, inside_aggs)
    merge_inside_aggs(sql_select, aggs, parent_pipeline_aggs)


def merge_inside_aggs(sql_select, aggs, inside_aggs):
    # at metric level
    if inside_aggs:
        for bucket_key in sql_select.group_by.keys():
            aggs = aggs[bucket_key]['aggs']
        aggs.update(inside_aggs)


def merge_sibling_pipeline_aggs(sql_select, aggs, sibling_pipeline_aggs):
    # at buckets level
    if sibling_pipeline_aggs:
        aggs.update(sibling_pipeline_aggs)
