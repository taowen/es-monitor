import time

from sqlparse import tokens as ttypes
from sqlparse import sql as stypes


def create_compound_filter(tokens):
    idx = 0
    current_filter = None
    logic_op = None
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
            if not logic_op and not current_filter:
                current_filter = new_filter
            elif 'OR' == logic_op:
                current_filter = {'bool': {'should': [current_filter, new_filter]}}
            elif 'AND' == logic_op:
                current_filter = {'bool': {'filter': [current_filter, new_filter]}}
            elif 'NOT' == logic_op:
                current_filter = {'bool': {'must_not': new_filter}}
            else:
                raise Exception('unexpected: %s' % token)
        elif ttypes.Keyword == token.ttype:
            if 'OR' == token.value.upper():
                logic_op = 'OR'
            elif 'AND' == token.value.upper():
                logic_op = 'AND'
            elif 'NOT' == token.value.upper():
                if logic_op:
                    raise Exception('unexpected: NOT')
                logic_op = 'NOT'
            else:
                raise Exception('unexpected: %s' % token)
        else:
            raise Exception('unexpected: %s' % token)
    return current_filter


def create_comparision_filter(token):
    if not isinstance(token, stypes.Comparison):
        raise Exception('unexpected: %s' % token)
    operator = token.token_next_by_type(0, ttypes.Comparison)
    if '>' == operator.value:
        return {'range': {token.left.get_name(): {'gt': eval_numeric_value(str(token.right))}}}
    elif '>=' == operator.value:
        return {'range': {token.left.get_name(): {'gte': eval_numeric_value(str(token.right))}}}
    elif '<' == operator.value:
        return {'range': {token.left.get_name(): {'lt': eval_numeric_value(str(token.right))}}}
    elif '<=' == operator.value:
        return {'range': {token.left.get_name(): {'lte': eval_numeric_value(str(token.right))}}}
    elif '=' == operator.value:
        right_operand = eval(token.right.value)
        return {'term': {token.left.get_name(): right_operand}}
    elif operator.value.upper() in ('LIKE', 'ILIKE'):
        right_operand = eval(token.right.value)
        return {'wildcard': {token.left.get_name(): right_operand.replace('%', '*').replace('_', '?')}}
    elif operator.value in ('!=', '<>'):
        right_operand = eval(token.right.value)
        return {'not': {'term': {token.left.get_name(): right_operand}}}
    elif 'IN' == operator.value.upper():
        values = eval(token.right.value)
        if not isinstance(values, tuple):
            values = (values,)
        return {'terms': {token.left.get_name(): values}}
    elif 'IS' == operator.value.upper():
        if 'NULL' != token.right.value.upper():
            raise Exception('unexpected: %s' % repr(token.right))
        return {'bool': {'must_not': {'exists': {'field': token.left.get_name()}}}}
    else:
        raise Exception('unexpected operator: %s' % operator.value)


def eval_numeric_value(token):
    token_str = str(token).strip()
    if token_str.startswith('('):
        token_str = token_str[1:-1]
    if token_str.startswith('@now'):
        token_str = token_str[4:].strip()
        if not token_str:
            return long(time.time() * long(1000))
        if '+' == token_str[0]:
            return long(time.time() * long(1000)) + eval_timedelta(token_str[1:])
        elif '-' == token_str[0]:
            return long(time.time() * long(1000)) - eval_timedelta(token_str[1:])
        else:
            raise Exception('unexpected: %s' % token)
    else:
        return float(token)


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
