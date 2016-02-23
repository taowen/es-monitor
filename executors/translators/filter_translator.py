import time
import re
import datetime
import logging

from sqlparse import tokens as ttypes
from sqlparse import sql as stypes
from sqlparse import datetime_evaluator

LOGGER = logging.getLogger(__name__)


def create_compound_filter(tokens, tables=None):
    tables = tables or {}
    idx = 0
    current_filter = None
    last_filter = None
    logic_op = None
    is_not = False
    while idx < len(tokens):
        token = tokens[idx]
        idx += 1
        if token.is_whitespace():
            continue
        if isinstance(token, stypes.Comparison) or isinstance(token, stypes.Parenthesis):
            if isinstance(token, stypes.Comparison):
                new_filter = create_comparision_filter(token, tables)
            elif isinstance(token, stypes.Parenthesis):
                new_filter = create_compound_filter(token.tokens[1:-1])
            else:
                raise Exception('unexpected: %s' % token)
            try:
                if not logic_op and not current_filter:
                    if is_not:
                        is_not = False
                        new_filter = {'bool': {'must_not': [new_filter]}}
                    current_filter = new_filter
                elif 'OR' == logic_op:
                    if is_not:
                        is_not = False
                        new_filter = {'bool': {'must_not': [new_filter]}}
                    current_filter = {'bool': {'should': [current_filter, new_filter]}}
                elif 'AND' == logic_op:
                    merged_filter = try_merge_filter(new_filter, last_filter)
                    if merged_filter:
                        last_filter.clear()
                        last_filter.update(merged_filter)
                    elif is_not:
                        is_not = False
                        if isinstance(current_filter, dict) and ['bool'] == current_filter.keys():
                            current_filter['bool']['must_not'] = current_filter['bool'].get('must_not', [])
                            current_filter['bool']['must_not'].append(new_filter)
                        else:
                            current_filter = {'bool': {'filter': [current_filter], 'must_not': [new_filter]}}
                    else:
                        if isinstance(current_filter, dict) and ['bool'] == current_filter.keys():
                            current_filter['bool']['filter'] = current_filter['bool'].get('filter', [])
                            current_filter['bool']['filter'].append(new_filter)
                        else:
                            current_filter = {'bool': {'filter': [current_filter, new_filter]}}
                else:
                    raise Exception('unexpected: %s' % token)
            finally:
                last_filter = current_filter
        elif ttypes.Keyword == token.ttype:
            if 'OR' == token.value.upper():
                if logic_op == 'AND':
                    raise Exception('OR can only follow OR/NOT, otherwise use () to compound')
                logic_op = 'OR'
            elif 'AND' == token.value.upper():
                if logic_op == 'OR':
                    raise Exception('AND can only follow AND/NOT, otherwise use () to compound')
                logic_op = 'AND'
            elif 'NOT' == token.value.upper():
                is_not = True
            else:
                raise Exception('unexpected: %s' % token)
        else:
            raise Exception('unexpected: %s' % token)
    return current_filter


def try_merge_filter(new_filter, last_filter):
    if ['range'] == new_filter.keys() and ['range'] == last_filter.keys():
        for k in new_filter['range']:
            for o in new_filter['range'][k]:
                if last_filter['range'].get(k, {}).get(o):
                    return None
            for o in new_filter['range'][k]:
                last_filter['range'][k][o] = new_filter['range'][k][o]
        return dict(last_filter)
    if ['type'] == last_filter.keys() and ['ids'] == new_filter.keys():
        last_filter, new_filter = new_filter, last_filter
    if ['type'] == new_filter.keys() and ['ids'] == last_filter.keys():
        if not last_filter['ids'].get('type'):
            last_filter['ids']['type'] = new_filter['type']['value']
            return dict(last_filter)
    return None


def create_comparision_filter(comparison, tables=None):
    tables = tables or {}
    if not isinstance(comparison, stypes.Comparison):
        raise Exception('unexpected: %s' % comparison)
    operator = comparison.operator
    right_operand = comparison.right
    left_operand = comparison.left
    if operator in ('>', '>=', '<', '<='):
        right_operand_as_value = eval_value(right_operand)
        left_operand_as_value = eval_value(left_operand)
        simple_types = (ttypes.Number.Integer, ttypes.Number.Float)
        if left_operand.is_field() and right_operand_as_value is not None:
            operator_as_str = {'>': 'gt', '>=': 'gte', '<': 'lt', '<=': 'lte'}[operator]
            return {
                'range': {left_operand.as_field_name(): {operator_as_str: right_operand_as_value}}}
        elif right_operand.is_field() and left_operand_as_value is not None:
            operator_as_str = {'>': 'lte', '>=': 'lt', '<': 'gte', '<=': 'gt'}[operator]
            return {
                'range': {right_operand.as_field_name(): {operator_as_str: left_operand_as_value}}}
        else:
            raise Exception('complex range condition not supported: %s' % comparison)
    elif '=' == operator:
        cross_table_eq = eval_cross_table_eq(tables, left_operand, right_operand)
        if cross_table_eq:
            return cross_table_eq
        simple_types = (ttypes.Number.Integer, ttypes.Number.Float, ttypes.String.Single)
        right_operand_as_value = eval_value(right_operand)
        left_operand_as_value = eval_value(left_operand)
        if left_operand.is_field() and right_operand_as_value is not None:
            pass
        elif right_operand.is_field() and left_operand_as_value is not None:
            right_operand_as_value = left_operand_as_value
            left_operand, right_operand = right_operand, left_operand
        else:
            raise Exception('complex equal condition not supported: %s' % comparison)
        field = left_operand.as_field_name()
        if '_type' == field:
            return {'type': {'value': right_operand_as_value}}
        elif '_id' == field:
            return {'ids': {'value': [right_operand_as_value]}}
        else:
            return {'term': {field: right_operand_as_value}}
    elif operator.upper() in ('LIKE', 'ILIKE'):
        right_operand = eval(right_operand.value)
        return {'wildcard': {left_operand.as_field_name(): right_operand.replace('%', '*').replace('_', '?')}}
    elif operator in ('!=', '<>'):
        right_operand = eval(right_operand.value)
        return {'bool': {'must_not': {'term': {left_operand.as_field_name(): right_operand}}}}
    elif 'IN' == operator.upper():
        values = eval(right_operand.value)
        if not isinstance(values, tuple):
            values = (values,)
        values = list(values)
        if '_id' == left_operand.as_field_name():
            return {'ids': {'value': values}}
        else:
            return {'terms': {left_operand.as_field_name(): values}}
    elif re.match('IS\s+NOT', operator.upper()):
        if 'NULL' != right_operand.value.upper():
            raise Exception('unexpected: %s' % repr(right_operand))
        return {'exists': {'field': left_operand.as_field_name()}}
    elif 'IS' == operator.upper():
        if 'NULL' != right_operand.value.upper():
            raise Exception('unexpected: %s' % repr(right_operand))
        return {'bool': {'must_not': {'exists': {'field': left_operand.as_field_name()}}}}
    else:
        raise Exception('unexpected operator: %s' % operator)


def eval_cross_table_eq(tables, left, right):
    if isinstance(right, stypes.DotName) and len(right.tokens) == 3 and '.' == right.tokens[1].value:
        if isinstance(left, stypes.DotName) and len(left.tokens) == 3 and '.' == left.tokens[1].value:
            right_table = right.tokens[0].as_field_name()
            left_table = left.tokens[0].as_field_name()
            if True == tables.get(right_table):
                field_ref = FieldRef(left.tokens[0].as_field_name(), left.tokens[2].as_field_name())
                return {'term': {right.tokens[2].as_field_name(): field_ref}}
            elif True == tables.get(left_table):
                field_ref = FieldRef(right.tokens[0].as_field_name(), right.tokens[2].as_field_name())
                return {'term': {left.tokens[2].as_field_name(): field_ref}}
    return None


class FieldRef(object):
    def __init__(self, table, field):
        self.table = table
        self.field = field

    def __repr__(self):
        return '${%s.%s}' % (self.table, self.field)

    def __str__(self):
        return repr(self)

    def __unicode__(self):
        return repr(self)


def eval_value(token):
    val = str(token)
    try:
        val = eval(val, {}, datetime_evaluator.datetime_functions())
        if isinstance(val, datetime.datetime):
            return long(time.mktime(val.timetuple()) * 1000)
        return val
    except:
        return None
