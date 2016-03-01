# -*- coding: utf-8 -*-

"""
Query elasticsearch using SQL
"""
import logging
import re

from .executors import SelectFromLeafExecutor
from .executors import SelectInsideBranchExecutor
from .executors import SelectInsideLeafExecutor
from .executors import SqlParameter
from .sqlparse.sql_select import SqlSelect

LOGGER = logging.getLogger(__name__)

def execute_sql(es_url, sql, arguments=None):
    current_sql_selects = []
    result_map = {}
    for sql_select in sql.split(';'):
        sql_select = sql_select.strip()
        if not sql_select:
            continue
        is_sql = re.match(r'^(WITH|SELECT)\s+', sql_select, re.IGNORECASE)
        if is_sql:
            current_sql_selects.append(sql_select)
        else:
            is_save = re.match(r'^SAVE\s+RESULT\s+AS\s+(.*)$', sql_select, re.IGNORECASE | re.DOTALL)
            is_remove = re.match(r'^REMOVE\s+RESULT\s+(.*)$', sql_select, re.IGNORECASE | re.DOTALL)
            if is_save:
                result_name = is_save.group(1)
                result_map[result_name] = create_executor(current_sql_selects, result_map).execute(es_url, arguments)
                current_sql_selects = []
            elif is_remove:
                if current_sql_selects:
                    result_map['result'] = create_executor(current_sql_selects, result_map).execute(es_url, arguments)
                    current_sql_selects = []
                result_map.pop(is_remove.group(1))
            else:
                exec sql_select in {'result_map': result_map}, {}
    if current_sql_selects:
        result_map['result'] = create_executor(current_sql_selects, result_map).execute(es_url, arguments)
    return result_map


def create_executor(sql_selects, joinable_results=None):
    executor_map = {}
    if not isinstance(sql_selects, list):
        sql_selects = [sql_selects]
    root_executor = None
    level = 0
    for sql_select in sql_selects:
        level += 1
        executor_name = 'level%s' % level
        if not isinstance(sql_select, SqlSelect):
            sql_select = sql_select.strip()
            if not sql_select:
                continue
            match = re.match(r'^WITH\s+(.*)\s+AS\s+\((.*)\)\s*$', sql_select, re.IGNORECASE | re.DOTALL)
            if match:
                executor_name = match.group(1)
                sql_select = match.group(2)
            sql_select = SqlSelect.parse(sql_select, joinable_results, executor_map)
        if not isinstance(sql_select.from_table, basestring):
            raise Exception('nested SELECT is not supported')
        if sql_select.from_table in executor_map:
            parent_executor = executor_map[sql_select.from_table]
            executor = SelectInsideBranchExecutor(sql_select, executor_name)
            parent_executor.add_child(executor)
        else:
            if sql_select.is_select_inside:
                executor = SelectInsideLeafExecutor(sql_select)
            else:
                executor = SelectFromLeafExecutor(sql_select)
            if root_executor:
                if executor.sql_select.join_table != root_executor[0]:
                    raise Exception('multiple root executor is not supported')
            root_executor = (executor_name, executor)
        executor_map[executor_name] = executor
    if not root_executor:
        raise Exception('sql not found in %s' % sql_selects)
    root_executor[1].build_request()
    update_placeholder(root_executor[1].request, root_executor[1].request)
    return root_executor[1]



def update_placeholder(request, obj, path=None):
    path = path or []
    if isinstance(obj, dict):
        for k, v in obj.items():
            obj[k] = update_placeholder(request, v, path + [k])
        return obj
    elif isinstance(obj, (tuple, list)):
        for i, e in enumerate(list(obj)):
            obj[i] = update_placeholder(request, e, path + [i])
        return obj
    elif isinstance(obj, SqlParameter):
        request['_parameters_'] = request.get('_parameters_', {})
        request['_parameters_'][obj.parameter_name] = {
            'path': path
        }
        if obj.field_hint:
            request['_parameters_'][obj.parameter_name]['field_hint'] = obj.field_hint
        return str(obj)
    else:
        return obj
