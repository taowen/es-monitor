import re
import datetime

NOW = None


def datetime_functions():
    functions = {'now': eval_now, 'today': eval_today, 'eval_datetime': eval_datetime, 'interval': eval_interval, 'timestamp': eval_timestamp}
    for k, v in functions.items():
        functions[k.upper()] = v
    return functions


def eval_now():
    return NOW or datetime.datetime.now()

def eval_today():
    now = eval_now()
    return datetime.datetime(now.year, now.month, now.day, tzinfo=now.tzinfo)

def eval_interval(datetime_value):
    return eval_datetime('INTERVAL', datetime_value)


def eval_timestamp(datetime_value):
    return eval_datetime('TIMESTAMP', datetime_value)


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
