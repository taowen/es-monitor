import time
import re
import datetime
import logging

from sqlparse import tokens as ttypes
from sqlparse import sql as stypes

NOW = None
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
                if not last_filter['range'].get(k, {}).get(o):
                    return False
            for o in new_filter['range'][k]:
                last_filter['range'][k][o] = new_filter['range'][k][o]
        return True
    return False


def create_comparision_filter(comparison, tables=None):
    tables = tables or {}
    if not isinstance(comparison, stypes.Comparison):
        raise Exception('unexpected: %s' % comparison)
    operator = comparison.operator
    if operator in ('>', '>=', '<', '<='):
        right_operand_as_value = eval_value(comparison.right)
        left_operand_as_value = eval_value(comparison.left)
        simple_types = (ttypes.Number.Integer, ttypes.Number.Float)
        if comparison.left.is_field() and right_operand_as_value is not None:
            operator_as_str = {'>': 'gt', '>=': 'gte', '<': 'lt', '<=': 'lte'}[operator]
            return {
                'range': {comparison.left.as_field_name(): {operator_as_str: right_operand_as_value}}}
        elif comparison.right.is_field() and left_operand_as_value is not None:
            operator_as_str = {'>': 'lte', '>=': 'lt', '<': 'gte', '<=': 'gt'}[operator]
            return {
                'range': {comparison.right.as_field_name(): {operator_as_str: left_operand_as_value}}}
        else:
            raise Exception('complex range condition not supported: %s' % comparison)
    elif '=' == operator:
        cross_table_eq = eval_cross_table_eq(tables, comparison.left, comparison.right)
        if cross_table_eq:
            return cross_table_eq
        simple_types = (ttypes.Number.Integer, ttypes.Number.Float, ttypes.String.Single)
        right_operand_as_value = eval_value(comparison.right)
        left_operand_as_value = eval_value(comparison.left)
        if comparison.left.is_field() and right_operand_as_value is not None:
            field = comparison.left.as_field_name()
            return {'term': {field: right_operand_as_value}}
        elif comparison.right.is_field() and left_operand_as_value is not None:
            field = comparison.right.as_field_name()
            return {'term': {field: left_operand_as_value}}
        else:
            raise Exception('complex equal condition not supported: %s' % comparison)
    elif operator.upper() in ('LIKE', 'ILIKE'):
        right_operand = eval(comparison.right.value)
        return {'wildcard': {comparison.left.as_field_name(): right_operand.replace('%', '*').replace('_', '?')}}
    elif operator in ('!=', '<>'):
        right_operand = eval(comparison.right.value)
        return {'bool': {'must_not': {'term': {comparison.left.as_field_name(): right_operand}}}}
    elif 'IN' == operator.upper():
        values = eval(comparison.right.value)
        if not isinstance(values, tuple):
            values = (values,)
        return {'terms': {comparison.left.as_field_name(): values}}
    elif re.match('IS\s+NOT', operator.upper()):
        if 'NULL' != comparison.right.value.upper():
            raise Exception('unexpected: %s' % repr(comparison.right))
        return {'exists': {'field': comparison.left.as_field_name()}}
    elif 'IS' == operator.upper():
        if 'NULL' != comparison.right.value.upper():
            raise Exception('unexpected: %s' % repr(comparison.right))
        return {'bool': {'must_not': {'exists': {'field': comparison.left.as_field_name()}}}}
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
        val = eval(val, {}, {'now': eval_now, 'eval_datetime': eval_datetime})
        if isinstance(val, datetime.datetime):
            return long(time.mktime(val.timetuple()) * 1000)
        return val
    except:
        return None


def eval_now():
    return NOW or datetime.datetime.now()


def eval_datetime(datetime_type, datetime_value):
    if 'INTERVAL' == datetime_type.upper():
        try:
            return eval_interval(datetime_value)
        except:
            LOGGER.debug('failed to parse: %s' % datetime_value, exc_info=1)
            raise
    elif 'TIMESTAMP' == datetime_type.upper():
        return datetime.datetime.strptime(datetime_value, '%Y-%m-%d %H:%M:%S')
    else:
        raise Exception('unsupported datetime type: %s' % datetime_type)


PATTERN_INTERVAL = re.compile(
    r'((\d+)\s+(DAYS?|HOURS?|MINUTES?|SECONDS?))?\s*'
    r'((\d+)\s+(HOURS?|MINUTES?|SECONDS?))?\s*'
    r'((\d+)\s+(MINUTES?|SECONDS?))?\s*'
    r'((\d+)\s+(SECONDS?))?', re.IGNORECASE)


def eval_interval(interval):
    interval = interval.strip()
    match = PATTERN_INTERVAL.match(interval)
    if not match or match.end() != len(interval):
        raise Exception('%s is invalid' % interval)
    timedelta = datetime.timedelta()
    last_pos = 0
    _, q1, u1, _, q2, u2, _, q3, u3, _, q4, u4 = match.groups()
    for quantity, unit in [(q1, u1), (q2, u2), (q3, u3), (q4, u4)]:
        if not quantity:
            continue
        unit = unit.upper()
        if unit in ('DAY', 'DAYS'):
            timedelta += datetime.timedelta(days=int(quantity))
        elif unit in ('HOUR', 'HOURS'):
            timedelta += datetime.timedelta(hours=int(quantity))
        elif unit in ('MINUTE', 'MINUTES'):
            timedelta += datetime.timedelta(minutes=int(quantity))
        elif unit in ('SECOND', 'SECONDS'):
            timedelta += datetime.timedelta(seconds=int(quantity))
        else:
            raise Exception('unknown unit: %s' % unit)
    return timedelta
