from sqlparse import tokens as ttypes
import filter_translator
import having_translator


def translate_select_from(sql_select):
    if sql_select.where:
        bucket_selector_agg = having_translator.translate_having(sql_select.select_from, sql_select.where.tokens[1:])
    else:
        bucket_selector_agg = None
    return bucket_selector_agg, lambda response: response
