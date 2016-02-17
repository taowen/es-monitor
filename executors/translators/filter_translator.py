import time
import re

from sqlparse import tokens as ttypes
from sqlparse import sql as stypes

NOW = None

def create_compound_filter(tokens):
    idx = 0
    current_filter = None
    last_filter = None
    logic_op = None
    is_not = False
    while idx < len(tokens):
        token = tokens[idx]
        idx += 1
        if token.ttype in (ttypes.Whitespace, ttypes.Comment):
            continue
        if isinstance(token, stypes.Comparison) or isinstance(token, stypes.Parenthesis):
            if isinstance(token, stypes.Comparison):
                new_filter = create_comparision_filter(token)
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
                    if try_merge_filter(new_filter, last_filter):
                        pass
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
                    return False
            for o in new_filter['range'][k]:
                last_filter['range'][k][o] = new_filter['range'][k][o]
        return True
    return False


def create_comparision_filter(comparison):
    if not isinstance(comparison, stypes.Comparison):
        raise Exception('unexpected: %s' % comparison)
    operator = comparison.operator
    if operator in ('>', '>=', '<', '<='):
        right_operand_as_value = eval_value(comparison.right)
        left_operand_as_value = eval_value(comparison.left)
        simple_types = (ttypes.Number.Integer, ttypes.Number.Float)
        if comparison.left.ttype in (ttypes.Name, ttypes.String.Symbol) and right_operand_as_value is not None:
            operator_as_str = {'>': 'gt', '>=': 'gte', '<': 'lt', '<=': 'lte'}[operator]
            return {
                'range': {eval_field_name(comparison.left): {operator_as_str: right_operand_as_value}}}
        elif comparison.right.ttype in (ttypes.Name, ttypes.String.Symbol) and left_operand_as_value is not None:
            operator_as_str = {'>': 'lte', '>=': 'lt', '<': 'gte', '<=': 'gt'}[operator]
            return {
                'range': {eval_field_name(comparison.right): {operator_as_str: left_operand_as_value}}}
        else:
            raise Exception('complex range condition not supported: %s' % comparison)
    elif '=' == operator:
        simple_types = (ttypes.Number.Integer, ttypes.Number.Float, ttypes.String.Single)
        right_operand_as_value = eval_value(comparison.right)
        left_operand_as_value = eval_value(comparison.left)
        if comparison.left.ttype in (ttypes.Name, ttypes.String.Symbol) and right_operand_as_value is not None:
            field = eval_field_name(comparison.left)
            return {'term': {field: right_operand_as_value}}
        elif comparison.right.ttype in (ttypes.Name, ttypes.String.Symbol) and left_operand_as_value is not None:
            field = eval_field_name(comparison.right)
            return {'term': {field: left_operand_as_value}}
        else:
            raise Exception('complex equal condition not supported: %s' % comparison)
    elif operator.upper() in ('LIKE', 'ILIKE'):
        right_operand = eval(comparison.right.value)
        return {'wildcard': {eval_field_name(comparison.left): right_operand.replace('%', '*').replace('_', '?')}}
    elif operator in ('!=', '<>'):
        right_operand = eval(comparison.right.value)
        return {'bool': {'must_not': {'term': {eval_field_name(comparison.left): right_operand}}}}
    elif 'IN' == operator.upper():
        values = eval(comparison.right.value)
        if not isinstance(values, tuple):
            values = (values,)
        return {'terms': {eval_field_name(comparison.left): values}}
    elif re.match('IS\s+NOT', operator.upper()):
        if 'NULL' != comparison.right.value.upper():
            raise Exception('unexpected: %s' % repr(comparison.right))
        return {'exists': {'field': eval_field_name(comparison.left)}}
    elif 'IS' == operator.upper():
        if 'NULL' != comparison.right.value.upper():
            raise Exception('unexpected: %s' % repr(comparison.right))
        return {'bool': {'must_not': {'exists': {'field': eval_field_name(comparison.left)}}}}
    else:
        raise Exception('unexpected operator: %s' % operator.value)


def eval_field_name(token):
    if ttypes.Name == token.ttype:
        return token.value
    elif ttypes.String.Symbol == token.ttype:
        return token.value[1:-1]
    else:
        raise Exception('not field: %s' % repr(token))


def eval_value(token):
    val = str(token)
    try:
        return eval(val, {}, {
            'now': func_now
        })
    except:
        return None


def func_now():
    return NOW or long(time.time() * 1000)


def eval_numeric_value(val):
    if val.startswith('('):
        val = val[1:-1]
    if val.startswith('@now'):
        val = val[4:].strip()
        if not val:
            return long(time.time() * long(1000))
        if '+' == val[0]:
            return long(time.time() * long(1000)) + eval_timedelta(val[1:])
        elif '-' == val[0]:
            return long(time.time() * long(1000)) - eval_timedelta(val[1:])
        else:
            raise Exception('unexpected: %s' % token)
    else:
        return float(val)


def eval_timedelta(str):
    if str.endswith('m'):
        return long(str[:-1]) * long(60 * 1000)
    elif str.endswith('s'):
        return long(str[:-1]) * long(1000)
    elif str.endswith('h'):
        return long(str[:-1]) * long(60 * 60 * 1000)
    elif str.endswith('d'):
        return long(str[:-1]) * long(24 * 60 * 60 * 1000)
    else:
        return long(str)
