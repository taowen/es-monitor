import case_when_translator
import filter_translator
import having_translator
import metric_translator
from sqlparse import sql as stypes
from sqlparse import tokens as ttypes


def translate_select_inside(sql_select):
    translator = Translator(sql_select)
    return translator.request, translator


class Translator(object):
    def __init__(self, sql_select):
        self.sql_select = sql_select
        self.request = {}
        self.build_aggregation_request()

    def __call__(self, response):  # select_response()
        return self.select_aggregation_response(response)

